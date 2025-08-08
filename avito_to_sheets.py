import os, json, datetime as dt, math
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

def list_items(token, per_page=100):
    """Собираем ВСЕ объявления аккаунта (страницы перелистываем)."""
    ids = []
    page = 1
    while True:
        url = f"https://api.avito.ru/core/v1/items?per_page={per_page}&page={page}&status=active,old,removed,blocked,rejected"
        r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=60)
        log(f"GET items p{page} → {r.status_code}")
        r.raise_for_status()
        data = r.json()
        resources = (data or {}).get("resources") or []
        if not resources:
            break
        ids += [str(x.get("id")) for x in resou]()
