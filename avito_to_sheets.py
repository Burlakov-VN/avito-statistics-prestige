import os, json, datetime as dt
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ==== Конфигурация из секретов GitHub ====
AVITO_CLIENT_ID = os.environ["AVITO_CLIENT_ID"]
AVITO_CLIENT_SECRET = os.environ["AVITO_CLIENT_SECRET"]
AVITO_USER_ID = os.environ["AVITO_USER_ID"]       # у тебя это 363007001 (берётся из секрета)
SHEET_ID = os.environ["SHEET_ID"]
GOOGLE_SERVICE_JSON = os.environ["GOOGLE_SERVICE_JSON"]

# ==== Вспомогательные ====
def log(msg):
    print(f"[INFO] {msg}", flush=True)

def get_avito_token():
    """Берём access_token на 24 часа"""
    data = {
        "grant_type": "client_credentials",
        "client_id": AVITO_CLIENT_ID,
        "client_secret": AVITO_CLIENT_SECRET
    }
    r = requests.post("https://api.avito.ru/token", data=data, timeout=60)
    log(f"/token → {r.status_code}")
    if r.status_code != 200:
        raise RuntimeError(f"Auth failed: {r.status_code} {r.text[:500]}")
    token = r.json().get("access_token")
    if not token:
        raise RuntimeError(f"No access_token in response: {r.text[:500]}")
    return token

def debug_accounts(token):
    """Проверяем, какие аккаунты видит токен (и их id)"""
    url = "https://api.avito.ru/core/v1/accounts"
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=60)
    log(f"GET {url} → {r.status_code}")
    if r.status_code != 200:
        log(f"ACCOUNTS BODY: {r.text[:800]}")
        r.raise_for_status()
    try:
        data = r.json()
    except Exception:
        data = {"raw": r.text[:800]}
    log(f"ACCOUNTS: {str(data)[:800]}")
    return data

def get_stats(access_token, user_id, date_from, date_to):
    """
    Пробуем несколько вариантов эндпоинтов аналитики,
    потому что в разных аккаунтах/версиях может отличаться путь.
    """
    candidate_paths = [
        f"https://api.avito.ru/core/v3/accounts/{user_id}/analytics/items",
        f"https://api.avito.ru/core/v2/accounts/{user_id}/analytics/items",
        f"https://api.avito.ru/core/v1/accounts/{user_id}/analytics/items",
        # иногда аналитика может называться "stats"
        f"https://api.avito.ru/core/v2/accounts/{user_id}/stats/items",
        f"https://api.avito.ru/core/v1/accounts/{user_id}/stats/items",
    ]
    body = {
        "dateFrom": date_from,
        "dateTo": date_to,
        "metrics": ["views", "uniqContacts"],   # можно расширить
        "grouping": ["day", "item"],
        "limit": 1000,
        "offset": 0
    }
    headers = {"Authorization": f"Bearer {access_token}"}

    last = None
    for url in candidate_paths:
        try:
            r = requests.post(url, json=body, headers=headers, timeout=120)
        except Exception as e:
            log(f"TRY {url} → EXC {e}")
            last = (url, "EXC", str(e))
            continue

        log(f"TRY {url} → {r.status_code}")
        if r.status_code == 200:
            return r.json()
        else:
            # печатаем кусок тела, чтобы понять причину (unknown path / missing field / insufficient scope)
            log(f"BODY: {r.text[:1000]}")
            last = (url, r.status_code, r.text[:1000])

    raise RuntimeError(f"All analytics endpoints failed. Last={last}")

def get_item_info(access_token, user_id, item_id):
    url = f"https://api.avito.ru/core/v1/accounts/{user_id}/items/{item_id}"
    r = requests.get(url, headers={"Authorization": f"Bearer {access_token}"}, timeout=60)
    if r.status_code == 404:
        log(f"items/{item_id} → 404 (skip)")
        return {}
    if r.status_code != 200:
        log(f"items/{item_id} → {r.status_code} BODY: {r.text[:600]}")
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
        ws.append_row(["date","item_id","title","views","uniqContacts","vas_ids","vas_finish_time","vas_next_schedule"])
    return ws

def main():
    # --- Диапазон дат: для теста возьмём последние 3 дня, чтобы точно были данные.
    today = dt.date.today()
    date_from = (today - dt.timedelta(days=3)).strftime("%Y-%m-%d")
    date_to   = (today - dt.timedelta(days=1)).strftime("%Y-%m-%d")
    log(f"Collecting stats for: {date_from} → {date_to}")

    token = get_avito_token()

    # 1) Проверка аккаунтов, какие id видит токен
    accounts = debug_accounts(token)

    # 2) Забираем статистику (перебор эндпоинтов)
    stats = get_stats(token, AVITO_USER_ID, date_from, date_to)
    result = stats.get("result", {})
    groupings = result.get("groupings", [])
    log(f"Analytics groupings objects: {len(groupings)}")

    # 3) Разворачиваем в две карты: по дням и по объявлениям
    by_day, by_item = {}, {}
    for g in groupings:
        gtype = g.get("type")
        gid = g.get("id")
        metrics = {m["slug"]: m["value"] for m in g.get("metrics", [])}
        if gtype == "day":
            # id дня может приходить таймстампом
            try:
                date_str = dt.datetime.utcfromtimestamp(int(gid)).strftime("%Y-%m-%d")
            except Exception:
                date_str = str(gid)
            by_day[date_str] = metrics
        elif gtype == "item":
            by_item[str(gid)] = metrics

    log(f"Unique days: {len(by_day)}, unique items: {len(by_item)}")

    # 4) Подключаемся к Google Sheets и пишем строки
    ws = connect_sheet()
    rows = []
    for d, m_day in sorted(by_day.items()):
        for item_id, m_item in by_item.items():
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
                m_day.get("views",0),
                m_item.get("uniqContacts",0),
                vas_ids,
                finish_times,
                schedules
            ])

    if rows:
        log(f"Appending {len(rows)} rows to Google Sheets…")
        ws.append_rows(rows, value_input_option="USER_ENTERED")
        log("Done.")
    else:
        log("No rows formed (check that analytics returned both day and item groups).")

if __name__ == "__main__":
    main()
