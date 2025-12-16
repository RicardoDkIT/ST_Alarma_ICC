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

    # Expected format: {"1":[{...},{...}]}
    if isinstance(data, dict):
        recs = data.get(str(estacionid), [])
        return recs if isinstance(recs, list) else []
    if isinstance(data, list):
        return data
    return []


def pick_heatindex_record(records: list[dict], slots: list[datetime], slot_minutes: int):
    """
    Pick the most recent record whose 'fecha' lands on any allowed slot,
    with numeric indice_calor. Also extract temperatura if present.
    """
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

    for slot in slots:  # newest -> oldest
        if slot in by_slot:
            rec, hi_val, t_val, dt_api = by_slot[slot]
            return rec, hi_val, t_val, dt_api, slot

    return None, None, None, None, None


# ----------------------------
# Main (single run)
# ----------------------------
def main():
    # Required
    telegram_token = env("TELEGRAM_TOKEN")
    chat_ids_raw = env("TELEGRAM_CHAT_IDS")  # comma-separated: "833...,112..."
    redmet_user = env("REDMET_USER")
    redmet_pass = env("REDMET_PASS")
    lat = env("LAT")
    lon = env("LON")

    if not all([telegram_token, chat_ids_raw, redmet_user, redmet_pass, lat, lon]):
        print("ERROR: Missing required env vars: TELEGRAM_TOKEN, TELEGRAM_CHAT_IDS, REDMET_USER, REDMET_PASS, LAT, LON", file=sys.stderr)
        return 2

    chat_ids = [c.strip() for c in chat_ids_raw.split(",") if c.strip()]
    if not chat_ids:
        print("ERROR: TELEGRAM_CHAT_IDS is empty after parsing.", file=sys.stderr)
        return 2

    # Optional settings
    base_ws = env("REDMET_BASE", "https://redmet.icc.org.gt/ws")
    heat_index_threshold = float(env("HEAT_INDEX_THRESHOLD", "10.0"))

    slot_minutes = int(env("SLOT_MINUTES", "15"))
    max_age_min = int(env("MAX_AGE_MIN", "45"))
    lookback_hours = int(env("LOOKBACK_HOURS", "6"))

    # Extra safety to avoid repeated alerts on “stuck” data:
    # if record is older than this, we skip alerting entirely.
    suppress_if_older_than_min = int(env("SUPPRESS_IF_OLDER_THAN_MIN", "90"))

    now_local = datetime.now()
    slots, _ = build_slots(now_local, slot_minutes, max_age_min)

    fechaini = (now_local - timedelta(hours=lookback_hours)).strftime("%Y-%m-%d %H:%M")
    fechafin = now_local.strftime("%Y-%m-%d %H:%M")

    # 1) nearest stations
    estaciones = get_nearest_stations(base_ws, lat, lon, redmet_user, redmet_pass)
    if not estaciones:
        print("INFO: No nearest stations returned.")
        return 0

    chosen = None

    # 2) try station 1, then 2, then 3 (no averaging)
    for sta in estaciones[:3]:
        estacionid = sta.get("estacionid")
        if not estacionid:
            continue

        records = get_station_records(base_ws, str(estacionid), fechaini, fechafin, redmet_user, redmet_pass)
        if not records:
            continue

        rec, hi_val, t_val, dt_api, slot_used = pick_heatindex_record(records, slots, slot_minutes)
        if rec is None:
            continue

        age_min = (now_local - dt_api).total_seconds() / 60.0
        chosen = (sta, rec, hi_val, t_val, dt_api, slot_used, age_min)
        break

    if not chosen:
        print("INFO: No valid indice_calor found for slots in the nearest 3 stations.")
        return 0

    sta, rec, hi_val, t_val, dt_api, slot_used, age_min = chosen

    codigo = sta.get("codigo", "")
    finca = sta.get("finca", "")
    dist = sta.get("distancia", "")
    fecha_api = rec.get("fecha", "")

    print(
        f"DEBUG: station={codigo} dist_km={dist} indice_calor={hi_val} temp={t_val} "
        f"fecha_api={fecha_api} slot={slot_used.strftime('%Y-%m-%d %H:%M:%S')} age_min={round(age_min,1)}"
    )

    # Safety: skip very old readings to reduce repeats if API is delayed/stuck
    if age_min > suppress_if_older_than_min:
        print(f"INFO: Reading too old ({round(age_min,1)} min) > SUPPRESS_IF_OLDER_THAN_MIN={suppress_if_older_than_min}. Skipping alert.")
        return 0

    # Trigger ONLY on heat index
    if hi_val <= heat_index_threshold:
        print(f"INFO: No alert. indice_calor={hi_val} <= threshold={heat_index_threshold}")
        return 0

    temp_txt = "NA" if t_val is None else f"{t_val:.1f} °C"

    # Use HTML (stable bold)
    msg = (
        "🚨 <b>ALERTA DE SENSACIÓN TÉRMICA</b>\n\n"
        f"🏭 <b>ESTACIÓN UTILIZADA:</b> {codigo} - {finca}\n"
        f"📏 <b>DISTANCIA:</b> {dist} km\n"
        f"🌡️ <b>TEMPERATURA:</b> {temp_txt}\n"
        f"🔥 <b>SENSACIÓN TÉRMICA:</b> {hi_val:.1f} °C (umbral &gt; {heat_index_threshold:.1f} °C)\n"
        f"🕒 <b>FECHA API CONSULTA:</b> {fecha_api}\n"
        f"⏱️ <b>RETRASO:</b> {round(age_min,1)} min\n\n"
        "📡 <b>FUENTE:</b> REDMET ICC"
    )

    send_telegram_html(telegram_token, chat_ids, msg)
    print("INFO: Alert sent to Telegram.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
