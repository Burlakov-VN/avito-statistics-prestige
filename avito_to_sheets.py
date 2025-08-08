import os, json, datetime as dt
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# --- Конфиг из секретов GitHub ---
AVITO_CLIENT_ID = os.environ["AVITO_CLIENT_ID"]
AVITO_CLIENT_SECRET = os.environ["AVITO_CLIENT_SECRET"]
AVITO_USER_ID = os.environ["AVITO_USER_ID"]     # 363007001
SHEET_ID = os.environ["SHEET_ID"]
GOOGLE_SERVICE_JSON = os.environ["GOOGLE_SERVICE_JSON"]

def log(msg):
    print(f"[INFO] {msg}", flush=True)

def get_avito_token():
    r = requests.post(
        "https://api.avito.ru/token",
        data={
            "grant_type": "client_credentials",
            "client_id": AVITO_CLIENT_ID,
            "client_secret": AVITO_CLIENT_SECRET,
        },
        timeout=60,
    )
    r.raise_for_status()
    return r.json()["access_token"]

def get_stats(token, user_id, date_from, date_to):
    """
    Статистика по дням и объявлениям:
    POST /stats/v2/accounts/{user_id}/items
    """
    url = f"https://api.avito.ru/stats/v2/accounts/{user_id}/items"
    body = {
        "dateFrom": date_from,
        "dateTo": date_to,
        "metrics": ["views", "contacts", "uniqContacts"],
        "grouping": ["day", "item"],
        "limit": 1000,
        "offset": 0
    }
    r = requests.post(url, json=body, headers={"Authorization": f"Bearer {token}"}, timeout=120)
    log(f"POST {url} → {r.status_code}")
    r.raise_for_status()
    return r.json()

def get_item_info(token, user_id, item_id):
    """
    Информация по объявлению (в т.ч. список применённых VAS)
    GET /core/v1/accounts/{user_id}/items/{item_id}/
    """
    url = f"https://api.avito.ru/core/v1/accounts/{user_id}/items/{item_id}/"
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=60)
    if r.status_code == 404:
        return {}
    r.raise_for_status()
    return r.json()

def connect_sheet():
    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(GOOGLE_SERVICE_JSON), scope)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)
    try:
        ws = sh.worksheet("data")
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title="data", rows="1000", cols="20")
        ws.append_row(["date","item_id","title","views","contacts","uniqContacts","vas_ids","vas_finish_time","vas_next_schedule"])
    return ws

def parse_stats_to_map(stats_json):
    """
    Возвращает два словаря:
      by_day[YYYY-MM-DD] -> {metric:value}
      by_item[item_id]   -> {metric:value}
    Поддерживает 2 формата ответа:
      - старый: result.groupings = [{type,id,metrics:[{slug,value}]}]
      - новый: result.data = [{date,itemId,metrics:{...}}] или metrics как список
    """
    res = stats_json.get("result", {})
    by_day, by_item = {}, {}

    if "groupings" in res:
        for g in res.get("groupings", []):
            gtype = g.get("type")
            gid = g.get("id")
            metrics = {m["slug"]: m["value"] for m in g.get("metrics", [])}
            if gtype == "day":
                try:
                    date_str = dt.datetime.utcfromtimestamp(int(gid)).strftime("%Y-%m-%d")
                except Exception:
                    date_str = str(gid)
                by_day[date_str] = metrics
            elif gtype == "item":
                by_item[str(gid)] = metrics

    elif "data" in res:
        # Ожидаем записи вида: { "date": "YYYY-MM-DD" или ts, "itemId": 123, "metrics": {...} или [{slug,value}] }
        for row in res.get("data", []):
            date_val = row.get("date")
            try:
                # если это таймштамп
                date_str = dt.datetime.utcfromtimestamp(int(date_val)).strftime("%Y-%m-%d")
            except Exception:
                date_str = str(date_val)

            item_id = str(row.get("itemId") or row.get("item_id") or "")
            if not item_id:
                continue

            m = row.get("metrics", {})
            if isinstance(m, list):
                m = {mm.get("slug"): mm.get("value") for mm in m}
            # накапливаем: по дню и по объявлению
            # (если по одному дню несколько записей по одному item — суммируем)
            by_day.setdefault(date_str, {})
            for k, v in m.items():
                by_day[date_str][k] = by_day[date_str].get(k, 0) + (v or 0)

            by_item.setdefault(item_id, {})
            for k, v in m.items():
                by_item[item_id][k] = by_item[item_id].get(k, 0) + (v or 0)

    return by_day, by_item

def main():
    # Берём последние 3 дня
    today = dt.date.today()
    date_from = (today - dt.timedelta(days=3)).strftime("%Y-%m-%d")
    date_to   = (today - dt.timedelta(days=1)).strftime("%Y-%m-%d")
    log(f"Collecting stats for: {date_from} → {date_to}")

    token = get_avito_token()
    stats_json = get_stats(token, AVITO_USER_ID, date_from, date_to)

    by_day, by_item = parse_stats_to_map(stats_json)
    log(f"Stats records: days={len(by_day)}, items={len(by_item)}")

    if not by_day or not by_item:
        log("No stats to write (API вернул пусто по дням или объявлениям).")
        return

    ws = connect_sheet()
    rows = []

    # Собираем пары (день x объявление)
    for d in sorted(by_day.keys()):
        for item_id in by_item.keys():
            # подтянем VAS и заголовок
            info = get_item_info(token, AVITO_USER_ID, item_id) or {}
            title = info.get("title","")
            vas = info.get("vas") or []
            vas_ids = ",".join(v.get("vas_id","") for v in vas)
            finish_times = ",".join((v.get("finish_time") or "") for v in vas)
            schedules = "|".join(",".join(v.get("schedule") or []) for v in vas)

            rows.append([
                d,
                item_id,
                title,
                by_day.get(d, {}).get("views", 0),
                by_item.get(item_id, {}).get("contacts", 0),
                by_item.get(item_id, {}).get("uniqContacts", 0),
                vas_ids,
                finish_times,
                schedules
            ])

    if rows:
        log(f"Appending {len(rows)} rows to Google Sheets…")
        ws.append_rows(rows, value_input_option="USER_ENTERED")
        log("Done.")
    else:
        log("Rows list is empty after parsing — ничего не записано.")

if __name__ == "__main__":
    main()
