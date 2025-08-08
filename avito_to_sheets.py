# -*- coding: utf-8 -*-
"""
Avito → Google Sheets (сбор за 1 день = сегодня-3) с устойчивым листингом
- /token (client_credentials)
- Список объявлений:   GET /core/v1/items  (только этот, без 404)
- Дневная статистика:  POST /stats/v1/accounts/{user_id}/items  (periodGrouping=day)
- Инфо по объявлению:  GET /core/v1/accounts/{user_id}/items/{item_id}/  (поле vas)
ENV (GitHub Secrets):
  AVITO_CLIENT_ID, AVITO_CLIENT_SECRET, AVITO_USER_ID, SHEET_ID, GOOGLE_SERVICE_JSON
Опционально:
  ITEM_IDS_CSV="123,456,789"  — если хочешь собрать только по заданным ID
"""

import os
import json
import time
import datetime as dt
from typing import Dict, List, Tuple

import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from requests.adapters import HTTPAdapter, Retry

# ---------- утилиты ----------
def log(msg: str) -> None:
    print(f"[INFO] {msg}", flush=True)

def require_env(name: str) -> str:
    val = os.environ.get(name)
    if not val or not str(val).strip():
        raise RuntimeError(f"ENV '{name}' is missing or empty")
    return val

def make_session() -> requests.Session:
    """
    Сессия с повторами и бэкоффом на сетевые таймауты/сбои.
    """
    sess = requests.Session()
    retries = Retry(
        total=4,                # всего до 4 повторов
        backoff_factor=0.75,    # 0.75s, 1.5s, 3s, 6s
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=20)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess

# ---------- конфиг ----------
AVITO_CLIENT_ID = require_env("AVITO_CLIENT_ID")
AVITO_CLIENT_SECRET = require_env("AVITO_CLIENT_SECRET")
AVITO_USER_ID = require_env("AVITO_USER_ID")       # пример: 363007001
SHEET_ID = require_env("SHEET_ID")
GOOGLE_SERVICE_JSON = require_env("GOOGLE_SERVICE_JSON")
ITEM_IDS_CSV = os.environ.get("ITEM_IDS_CSV", "").strip()  # опционально

# Общая сессия с ретраями
SESSION = make_session()

# ---------- API ----------
def get_token() -> str:
    r = SESSION.post(
        "https://api.avito.ru/token",
        data={
            "grant_type": "client_credentials",
            "client_id": AVITO_CLIENT_ID,
            "client_secret": AVITO_CLIENT_SECRET,
        },
        timeout=30,
    )
    log(f"/token → {r.status_code}")
    if r.status_code >= 400:
        log(f"AUTH BODY: {r.text[:800]}")
    r.raise_for_status()
    token = r.json().get("access_token")
    if not token:
        raise RuntimeError("No access_token in /token response")
    return token

def list_items(token: str, per_page: int = 50, max_pages: int = 30) -> List[int]:
    """
    Стабильный листинг через /core/v1/items (без account-префикса).
    Возвращаем ВСЕ id объявлений (любой статус), постранично, с ретраями.
    """
    ids: List[int] = []
    page = 1
    headers = {"Authorization": f"Bearer {token}"}

    while True:
        url = (
            "https://api.avito.ru/core/v1/items"
            f"?per_page={per_page}&page={page}"
            "&status=active,old,removed,blocked,rejected"
        )
        try:
            r = SESSION.get(url, headers=headers, timeout=20)
        except requests.RequestException as e:
            log(f"GET items p{page} → EXC {e}")
            # дадим последний шанс и выйдем
            break

        log(f"GET items p{page} → {r.status_code}")
        if r.status_code >= 400:
            log(f"ITEMS BODY: {r.text[:800]}")
        r.raise_for_status()

        data = r.json() or {}
        resources = (
            data.get("resources")
            or data.get("items")
            or data.get("result", {}).get("items")
            or []
        )

        if not isinstance(resources, list) or not resources:
            break

        for x in resources:
            iid = x.get("id") or x.get("item_id")
            if iid:
                try:
                    ids.append(int(iid))
                except Exception:
                    continue

        # если записей меньше per_page — дальше пусто
        if len(resources) < per_page:
            break

        page += 1
        if page > max_pages:
            log(f"Reached max_pages={max_pages}, stop listing.")
            break

        # мягкий троттлинг, чтобы не ловить rate-limit
        time.sleep(0.15)

    ids = sorted(set(ids))
    log(f"Items found: {len(ids)}")
    return ids

def stats_v1(token: str, user_id: str, item_ids: List[int],
             date_from: str, date_to: str) -> Dict:
    """
    Дневная статистика:
      POST /stats/v1/accounts/{user_id}/items
      fields: ["uniqViews","uniqContacts"]
      itemIds: до 200 за запрос
      periodGrouping: "day"
    """
    url = f"https://api.avito.ru/stats/v1/accounts/{user_id}/items"
    body = {
        "dateFrom": date_from,
        "dateTo": date_to,
        "fields": ["uniqViews", "uniqContacts"],
        "itemIds": item_ids[:200],
        "periodGrouping": "day",
    }
    r = SESSION.post(url, json=body, headers={"Authorization": f"Bearer {token}"}, timeout=40)
    log(f"POST v1 stats batch({len(body['itemIds'])}) → {r.status_code}")
    if r.status_code >= 400:
        log(f"STATS BODY: {r.text[:1000]}")
    r.raise_for_status()
    return r.json()

def get_item_info(token: str, user_id: str, item_id: int) -> Dict:
    """
    Информация по объявлению (включая массив VAS):
      GET /core/v1/accounts/{user_id}/items/{item_id}/
    """
    url = f"https://api.avito.ru/core/v1/accounts/{user_id}/items/{item_id}/"
    r = SESSION.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=25)
    if r.status_code == 404:
        log(f"item {item_id} → 404 (skip)")
        return {}
    if r.status_code >= 400:
        log(f"item {item_id} → {r.status_code} {r.text[:400]}")
    r.raise_for_status()
    return r.json() or {}

# ---------- Google Sheets ----------
def connect_sheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(GOOGLE_SERVICE_JSON), scope)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)
    try:
        ws = sh.worksheet("data")
    except gspread.exceptions.WorksheetN
