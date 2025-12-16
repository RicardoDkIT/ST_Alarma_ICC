import os
import sys
from datetime import datetime, timedelta

import requests
from requests.auth import HTTPBasicAuth


# ----------------------------
# Helpers
# ----------------------------
def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def safe_float(x):
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def parse_api_dt(s: str):
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def floor_to_slot(dt: datetime, slot_minutes: int) -> datetime:
    m = (dt.minute // slot_minutes) * slot_minutes
    return dt.replace(minute=m, second=0, microsecond=0)


def build_slots(now_local: datetime, slot_minutes: int, max_age_min: int):
    base_slot = floor_to_slot(now_local, slot_minutes)
    slots = []
    mins = 0
    while mins <= max_age_min:
        slots.append(base_slot - timedelta(minutes=mins))
        mins += slot_minutes
    return slots, base_slot


def send_telegram_html(token: str, chat_ids: list[str], message_html: str):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    for cid in chat_ids:
        payload = {
            "chat_id": cid,
            "text": message_html,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        r = requests.post(url, json=payload, timeout=20)
        r.raise_for_status()


# ----------------------------
# REDMET calls
# ----------------------------
def get_nearest_stations(base_ws: str, lat: str, lon: str, user: str, password: str):
    url = f"{base_ws.rstrip('/')}/getLecturas/{lat}/{lon}"
    r = requests.get(
        url,
        headers={"Accept": "application/json"},
        auth=HTTPBasicAuth(user, password),
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    return data.get("estaciones", []) or []


def get_station_records(base_ws: str, estacionid: str, fechaini: str, fechafin: str, user: str, password: str):
    url = f"{base_ws.rstrip('/')}/redmet/estaciones/lecturas"
    params = {
        "fechaini": fechaini,
        "fechafin": fechafin,
        "tipo": "fecha",
        "estacionids[]": estacionid,
    }
    r = requests.get(
        url,
        params=params,
        headers={"Accept": "application/json"},
        auth=HTTPBasicAuth(user, password),
        timeout=60,
    )
    r.raise_for_status()
    data = r.json()

    if isinstance(data, dict):
        return data.get(str(estacionid), []) or []

    if isinstance(data, list):
        return data

    return []


def pick_heatindex_record(records: list[dict], slots: list[datetime], slot_minutes: int):
    by_slot = {}

    for rec in records:
        fecha = rec.get("fecha")
        if not isinstance(fecha, str):
            continue

        dt = parse_api_dt(fecha)
        if not dt:
            continue

        rec_slot = floor_to_slot(dt, slot_minutes)
        hi_val = safe_float(rec.get("indice_calor"))
        if hi_val is None:
            continue

        t_val = safe_float(rec.get("temperatura"))  # info only
        by_slot[rec_slot] = (rec, hi_val, t_val, dt)

    for slot in slots:
        if slot in by_slot:
            rec, hi_val, t_val, dt_api = by_slot[slot]
            return rec, hi_val, t_val, dt_api, slot

    return None, None, None, None, None


# ----------------------------
# Main (single run)
# ----------------------------
def main():
    telegram_token = env("TELEGRAM_TOKEN")
    chat_id_raw = env("CHAT_ID")

    redmet_user = env("REDMET_USER")
    redmet_pass = env("REDMET_PASS")
    lat = env("LAT")
    lon = env("LON")

    if not all([telegram_token, chat_id_raw, redmet_user, redmet_pass, lat, lon]):
        print("ERROR: Missing required secrets.", file=sys.stderr)
        return 2

    chat_ids = [c.strip() for c in chat_id_raw.split(",") if c.strip()]

    base_ws = env("REDMET_BASE", "https://redmet.icc.org.gt/ws")
    heat_index_threshold = float(env("HEAT_INDEX_THRESHOLD", "10"))

    slot_minutes = int(env("SLOT_MINUTES", "15"))
    max_age_min = int(env("MAX_AGE_MIN", "45"))
    lookback_hours = int(env("LOOKBACK_HOURS", "6"))
    suppress_if_older_than_min = int(env("SUPPRESS_IF_OLDER_THAN_MIN", "90"))

    now_local = datetime.now()
    slots, _ = build_slots(now_local, slot_minutes, max_age_min)

    fechaini = (now_local - timedelta(hours=lookback_hours)).strftime("%Y-%m-%d %H:%M")
    fechafin = now_local.strftime("%Y-%m-%d %H:%M")

    estaciones = get_nearest_stations(base_ws, lat, lon, redmet_user, redmet_pass)
    if not estaciones:
        print("INFO: No stations found.")
        return 0

    chosen = None

    for sta in estaciones[:3]:
        estacionid = sta.get("estacionid")
        if not estacionid:
            continue

        records = get_station_records(base_ws, str(estacionid), fechaini, fechafin, redmet_user, redmet_pass)
        rec, hi_val, t_val, dt_api, slot_used = pick_heatindex_record(records, slots, slot_minutes)
        if rec is None:
            continue

        age_min = (now_local - dt_api).total_seconds() / 60.0
        chosen = (sta, hi_val, t_val, age_min, slot_used, rec.get("fecha"))
        break

    if not chosen:
        print("INFO: No valid heat index found.")
        return 0

    sta, hi_val, t_val, age_min, slot_used, fecha_api = chosen

    if age_min > suppress_if_older_than_min:
        print("INFO: Data too old, skipping alert.")
        return 0

    if hi_val <= heat_index_threshold:
        print("INFO: No alert condition met.")
        return 0

    temp_txt = "NA" if t_val is None else f"{t_val:.1f} °C"

    msg = (
        "🚨 <b>ALERTA DE SENSACIÓN TÉRMICA</b>\n\n"
        f"🏭 <b>ESTACIÓN UTILIZADA:</b> {sta.get('codigo')} - {sta.get('finca')}\n"
        f"📏 <b>DISTANCIA:</b> {sta.get('distancia')} km\n"
        f"🌡️ <b>TEMPERATURA:</b> {temp_txt}\n"
        f"🔥 <b>SENSACIÓN TÉRMICA:</b> {hi_val:.1f} °C (umbral &gt; {heat_index_threshold} °C)\n"
        f"🕒 <b>FECHA API CONSULTA:</b> {fecha_api}\n"
        f"⏱️ <b>RETRASO:</b> {round(age_min,1)} min\n\n"
        "📡 <b>FUENTE:</b> REDMET ICC"
    )

    send_telegram_html(telegram_token, chat_ids, msg)
    print("INFO: Alert sent.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
