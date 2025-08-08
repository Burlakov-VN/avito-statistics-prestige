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

def get_avito_token():
    """Берём access_token (живет ~24 часа)."""
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
    Правильный эндпоинт статистики:
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
    r.raise_for_status()
    return r.json()

def get_item_info(token, user_id, item_id):
    """
    Информация по объявлению (в т.ч. список применённых VAS):
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

def main():
    # Берём последние 3 дня: так больше шансов, что в Авито всё уже посчиталось
    today = dt.date.today()
    date_from = (today - dt.timedelta(days=3)).strftime("%Y-%m-%d")
    date_to   = (today - dt.timedelta(days=1)).strftime("%Y-%m-%d")

    token = get_avito_token()
    stats = get_stats(token, AVITO_USER_ID, date_from, date_to)

    # Разворачиваем группировки в “дни” и “объявления”
    by_day, by_item = {}, {}
    for g in stats.get("result", {}).get("groupings", []):
        gtype = g.get("type")
        gid = g.get("id")
