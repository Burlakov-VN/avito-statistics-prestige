import os, json, datetime as dt
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials

AVITO_CLIENT_ID = os.environ["AVITO_CLIENT_ID"]
AVITO_CLIENT_SECRET = os.environ["AVITO_CLIENT_SECRET"]
AVITO_USER_ID = os.environ["AVITO_USER_ID"]     # 363007001
SHEET_ID = os.environ["SHEET_ID"]
GOOGLE_SERVICE_JSON = os.environ["GOOGLE_SERVICE_JSON"]

def log(msg):
    print(f"[INFO] {msg}", flush=True)

def get_token():
    r = requests.post(
        "https://api.avito.ru/token",
        data={"grant_type": "client_credentials",
              "client_id": AVITO_CLIENT_ID,
              "client_secret": AVITO_CLIENT_SECRET},
        timeout=60)
    log(f"/token → {r.status_code}")
    r.raise_for_status()
    return r.json()["access_token"]

def list_items(token, user_id, per_page=100):
    """
    Получаем ВСЕ объявления аккаунта (любой статус).
    Пробуем account-эндпоинт; если структура отличается — подстраиваемся.
    """
    ids = []
    page = 1
    while True:
        url = (
            f"https://api.avito.ru/core/v1/accounts/{user_id}/items"
            f"?per_page={per_page}&page={page}"
            f"&status=active,old,removed,blocked,rejected"
        )
        r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=60)
        log(f"GET items p{page} → {r.status_code}")
        if r.status_code == 404:
            # fallback на общий список (редкие случаи)
            url = (
                f"https://api.avito.ru/core/v1/items"
                f"?per_page={per_page}&page={page}"
                f"&status=active,old,removed,blocked,rejected"
            )
            r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=60)
            log(f"GET items (fallback) p{page} → {r.status_code}")
        r.raise_for_status()

        data = (r.json() or {})
        # В разных версиях поле может называться по-разному
        resources = (
            data.get("resources")
            or data.get("items")
            or data.get("result", {}).get("items")
            or []
        )

        if not isinstance(resources, list) or not resources:
            break

        for x in resources:
            # id может быть 'id' или 'item_id'
            iid = x.get("id") or x.get("item_id")
            if iid:
                ids.append(int(iid))

        # Страницы: если записей меньше per_page — дальше пусто
        if len(resources) < per_page:
            break

        page += 1
        if page > 100:  # защита от зацикливания
            break

    ids = sorted(set(ids))
    log(f"Items found: {len(ids)}")
    return ids

def stats_v1(token, user_id, item_ids, date_from, date_to):
    """
    Дневная статистика по объявлениям:
    POST /stats/v1/accounts/{user_id}/items
    fields: uniqViews, uniqContacts
    periodGrouping: day
    Максимум 200 itemIds за один запрос.
    """
    url = f"https://api.avito.ru/stats/v1/accounts/{user_id}/items"
    body = {
        "dateFrom": date_from,
        "dateTo": date_to,
        "fields": ["uniqViews", "uniqContacts"],
        "itemIds": item_ids[:200],
        "periodGrouping": "day"
    }
    r = requests.post(url, json=body, headers={"Authorization": f"Bearer {token}"}, timeout=120)
    log(f"POST v1 stats batch({len(body['itemIds'])}) → {r.status_code}")
    if r.status_code >= 400:
        log(f"BODY: {r.text[:1000]}")
    r.raise_for_status()
    return r.json()

def get_item_info(token, user_id, item_id):
    """
    Информация по объявлению (включая массив применённых VAS):
    GET /core/v1/accounts/{user_id}/items/{item_id}/
    """
    url = f"https://api.avito.ru/core/v1/accounts/{user_id}/items/{item_id}/"
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=60)
    if r.status_code == 404:
        return {}
    if r.status_code >= 400:
        log(f"item {item_id} → {r.status_code} {r.text[:300]}")
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
        ws = sh.add_worksheet(title="data", rows="5000", cols="20")
        ws.append_row(["date","item_id","title","uniqViews","uniqContacts","vas_ids","vas_finish_time","vas_next_schedule"])
    return ws

def main():
    today = dt.date.today()
    # Возьмём последние 7 дней, чтобы точно было что-то
    date_from = (today - dt.timedelta(days=7)).strftime("%Y-%m-%d")
    date_to   = (today - dt.timedelta(days=1)).strftime("%Y-%m-%d")
