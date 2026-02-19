import os
import json
import time
import random
import hashlib
import logging
from datetime import date, timedelta
from typing import Optional, Dict, Any, Set, List

import requests

# =========================
# Config
# =========================
API_URL = "https://gwapi.sad.ve.it/agenda/1.0.0/api/agenda/disponibilitaAppuntamento"
LANDING_URL = "https://dime.comune.venezia.it/servizio/richiesta-appuntamenti"

STATE_FILE = "dime_state.json"
LOG_FILE = "dime_monitor.log"

# چند روز آینده را چک کند
DAYS_AHEAD = 150  # 5 ماه

# هر اجرا کمی تصادفی تا ربات‌گونه نباشه (مثلاً 0 تا 40 ثانیه)
START_JITTER_SECONDS = 30

# Retry
MAX_RETRIES = 3
TIMEOUT_SECONDS = 30
BACKOFF_BASE_SECONDS = 6  # 6, 12, 24 (+ jitter)

# Thresholds
CONSECUTIVE_ERROR_ALERT = 3  # اگر 3 بار پشت هم خطا شد، هشدار جدی
MAX_SLOTS_IN_MESSAGE = 60

BASE_PAYLOAD = {
    "idSottocategoria": 40001,
    "idSede": 7,
    "origin": "FE",
}

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "Origin": "https://dime.comune.venezia.it",
    "Referer": "https://dime.comune.venezia.it/",
    "User-Agent": "Mozilla/5.0 (availability-checker)",
}

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


# =========================
# Logging
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("dime-monitor")


# =========================
# Helpers: state, telegram
# =========================
def load_state() -> Dict[str, Any]:
    if not os.path.exists(STATE_FILE):
        return {
            "slots": [],
            "consecutive_errors": 0,
            "last_error_type": None,
            "schema_hash": None,
            "last_status": None,
            "last_check": None,
        }
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            s = json.load(f)
        # defaults
        s.setdefault("slots", [])
        s.setdefault("consecutive_errors", 0)
        s.setdefault("last_error_type", None)
        s.setdefault("schema_hash", None)
        s.setdefault("last_status", None)
        s.setdefault("last_check", None)
        return s
    except Exception as e:
        log.error("Failed to read state file: %s", e)
        return {
            "slots": [],
            "consecutive_errors": 0,
            "last_error_type": "state_read_error",
            "schema_hash": None,
            "last_status": None,
            "last_check": None,
        }


def save_state(state: Dict[str, Any]) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def send_telegram(text: str) -> None:
    # اگر تلگرام ست نشده، فقط لاگ می‌کنیم
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram not configured. Would send:\n%s", text)
        return

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=30)
    except Exception as e:
        log.error("Failed to send telegram message: %s", e)


def now_ts() -> int:
    return int(time.time())


def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


# =========================
# Core logic
# =========================
def build_payload() -> Dict[str, Any]:
    start = date.today()
    end = start + timedelta(days=DAYS_AHEAD)
    payload = dict(BASE_PAYLOAD)
    payload["inizio"] = start.isoformat()
    payload["fine"] = end.isoformat()
    return payload


def compute_schema_hash(resp: Dict[str, Any]) -> str:
    """
    یک امضای خیلی ساده از ساختار جواب، برای تشخیص تغییر API.
    (کافی است کلیدهای اصلی و کلیدهای اولین item در dati را لحاظ کنیم)
    """
    top_keys = sorted(list(resp.keys()))
    dati = resp.get("dati", [])
    first_keys = []
    if isinstance(dati, list) and len(dati) > 0 and isinstance(dati[0], dict):
        first_keys = sorted(list(dati[0].keys()))
    sig = json.dumps({"top": top_keys, "dati0": first_keys}, ensure_ascii=False, sort_keys=True)
    return sha256_text(sig)


def extract_slots(resp: Dict[str, Any]) -> Set[str]:
    """
    خروجی: set از 'YYYY-MM-DD HH:MM'
    از resp['dati'][*]['giorno'] + resp['dati'][*]['orari'][*]['orarioInizio']
    فقط وقتی slotResidui > 0 باشد.
    """
    slots: Set[str] = set()
    dati = resp.get("dati", [])
    if not isinstance(dati, list):
        return slots

    for item in dati:
        if not isinstance(item, dict):
            continue
        day = item.get("giorno")
        orari = item.get("orari", [])
        if not day or not isinstance(orari, list):
            continue

        for o in orari:
            if not isinstance(o, dict):
                continue
            residui = o.get("slotResidui")
            if isinstance(residui, int) and residui <= 0:
                continue
            t = (o.get("orarioInizio") or "")[:5]  # HH:MM
            if t:
                slots.add(f"{day} {t}")

    return slots


def format_slots(slots: List[str]) -> str:
    lines = []
    for s in slots[:MAX_SLOTS_IN_MESSAGE]:
        lines.append(f"• {s}")
    if len(slots) > MAX_SLOTS_IN_MESSAGE:
        lines.append(f"… و {len(slots) - MAX_SLOTS_IN_MESSAGE} مورد دیگر")
    return "\n".join(lines)


def fetch_availability(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    با retry + backoff + هشدار برای 403/429.
    """
    sess = requests.Session()

    # optional: seed cookies
    try:
        sess.get(LANDING_URL, headers=HEADERS, timeout=TIMEOUT_SECONDS)
    except Exception:
        # حتی اگر لندینگ fail شد، شاید API جواب بده؛ ادامه می‌دیم.
        pass

    last_exc = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = sess.post(API_URL, headers=HEADERS, json=payload, timeout=TIMEOUT_SECONDS)

            # تشخیص بلاک/ریت‌لیمیت
            if r.status_code == 429:
                raise RuntimeError("HTTP_429_RATE_LIMIT")
            if r.status_code == 403:
                raise RuntimeError("HTTP_403_FORBIDDEN")
            if r.status_code >= 500:
                raise RuntimeError(f"HTTP_{r.status_code}_SERVER_ERROR")

            r.raise_for_status()

            data = r.json()
            if not isinstance(data, dict):
                raise RuntimeError("INVALID_JSON_SHAPE")

            return data

        except Exception as e:
            last_exc = e

            # backoff
            if attempt < MAX_RETRIES:
                sleep_s = BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
                sleep_s += random.uniform(0, 3.5)  # jitter
                log.warning("Fetch failed (attempt %s/%s): %s | sleeping %.1fs", attempt, MAX_RETRIES, e, sleep_s)
                time.sleep(sleep_s)
            else:
                log.error("Fetch failed (final): %s", e)

    # all retries failed
    if last_exc:
        raise last_exc
    return None


def main() -> None:
    # jitter at start
    if START_JITTER_SECONDS > 0:
        j = random.uniform(0, START_JITTER_SECONDS)
        log.info("Start jitter: sleeping %.1fs", j)
        time.sleep(j)

    payload = build_payload()
    state = load_state()
    state["last_check"] = now_ts()

    try:
        resp = fetch_availability(payload)

        # موفق شدیم => error counter ریست
        state["consecutive_errors"] = 0
        state["last_error_type"] = None

        # success flag
        esito = resp.get("esito", {})
        if isinstance(esito, dict) and esito.get("success") is False:
            # API گفته موفق نیست
            msg = f"⚠️ DIME API success=false\nmessage: {esito.get('message')}\nRange: {payload['inizio']} → {payload['fine']}"
            send_telegram(msg)
            state["last_status"] = "api_success_false"
            save_state(state)
            return

        # schema check
        schema_hash = compute_schema_hash(resp)
        if state.get("schema_hash") and state["schema_hash"] != schema_hash:
            send_telegram(
                "🧩 DIME API schema تغییر کرده!\n"
                "ممکنه لازم باشه اسکریپت آپدیت بشه.\n"
                f"Range: {payload['inizio']} → {payload['fine']}"
            )
        state["schema_hash"] = schema_hash

        # extract slots
        slots = extract_slots(resp)
        prev_slots = set(state.get("slots", []))

        if not slots:
            log.info("No availability 😕")
            state["last_status"] = "empty"
            save_state(state)
            return

        new_slots = slots - prev_slots
        if not new_slots:
            log.info("Availability exists but nothing new ✅")
            state["last_status"] = "no_new"
            save_state(state)
            return

        # prepare nice message
        sede = None
        cat = None
        dati = resp.get("dati", [])
        if isinstance(dati, list) and dati and isinstance(dati[0], dict):
            sede = dati[0].get("descrizioneSede")
            cat = dati[0].get("descrizioneSottocategoria")

        new_sorted = sorted(new_slots)
        msg_lines = [
            "🎉 وقت جدید در DIME پیدا شد!",
            f"📍 {sede}" if sede else f"📍 idSede={payload['idSede']}",
            f"🧾 {cat}" if cat else f"🧾 idSottocategoria={payload['idSottocategoria']}",
            f"🗓 بازه بررسی: {payload['inizio']} → {payload['fine']}",
            "",
            "⏰ زمان‌های جدید:",
            format_slots(new_sorted),
            "",
            "لینک:",
            "https://dime.comune.venezia.it/servizio/richiesta-appuntamenti",
        ]
        send_telegram("\n".join([l for l in msg_lines if l is not None]))

        # merge and save
        merged = sorted(prev_slots | slots)
        state["slots"] = merged
        state["last_status"] = "notified"
        save_state(state)
        log.info("Notified 🔔 new=%s", len(new_slots))

    except Exception as e:
        # classify error
        err_str = str(e)
        if "HTTP_403_FORBIDDEN" in err_str:
            err_type = "403_forbidden"
            human = "🚫 403 – احتمال بلاک یا محدودیت دسترسی"
        elif "HTTP_429_RATE_LIMIT" in err_str:
            err_type = "429_rate_limit"
            human = "⚠️ 429 – ریت‌لیمیت (درخواست زیاد/محدودیت موقت)"
        elif "SERVER_ERROR" in err_str:
            err_type = "5xx_server"
            human = f"⚠️ خطای سرور: {err_str}"
        elif "INVALID_JSON_SHAPE" in err_str:
            err_type = "invalid_json_shape"
            human = "🧩 پاسخ JSON غیرمنتظره (ممکنه API عوض شده باشه)"
        else:
            err_type = "network_or_unknown"
            human = f"❌ خطای شبکه/نامشخص: {err_str}"

        state["consecutive_errors"] = int(state.get("consecutive_errors", 0)) + 1
        state["last_error_type"] = err_type
        state["last_status"] = "error"
        save_state(state)

        log.error("Error: %s | consecutive=%s", human, state["consecutive_errors"])

        # پیام هشدار: همیشه برای 403/429، و برای بقیه فقط وقتی پشت سرهم زیاد شد
        if err_type in ("403_forbidden", "429_rate_limit"):
            send_telegram(
                f"{human}\n"
                f"consecutive_errors={state['consecutive_errors']}\n"
                f"Range: {build_payload()['inizio']} → {build_payload()['fine']}"
            )
        elif state["consecutive_errors"] >= CONSECUTIVE_ERROR_ALERT:
            send_telegram(
                "🔥 هشدار: چند بار پشت سر هم خطا داریم!\n"
                f"{human}\n"
                f"consecutive_errors={state['consecutive_errors']}\n"
                f"آخرین وضعیت: {state.get('last_error_type')}\n"
                f"Range: {build_payload()['inizio']} → {build_payload()['fine']}"
            )


if __name__ == "__main__":
    main()


