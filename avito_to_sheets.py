# -*- coding: utf-8 -*-
"""
Avito → Google Sheets
Режимы:
- По умолчанию: собираем РОВНО один день = сегодня-3 (UTC) и пишем на лист 'data'
- Бэкфилл: задать ENV START_DATE и (опционально) END_DATE в формате YYYY-MM-DD — скрипт пройдет датами включительно
Фильтрация:
- Пишем ТОЛЬКО объявления, у которых в ответе /stats В ЕТОТ ДЕНЬ есть запись (даже если метрики 0) = “активно в дату”
ENV (GitHub Secrets):
  AVITO_CLIENT_ID, AVITO_CLIENT_SECRET, AVITO_USER_ID, SHEET_ID, GOOGLE_SERVICE_JSON
Опционально:
  ITEM_IDS_CSV="123,456"   # если хочешь явно задать список ID и не листать каталог
  START_DATE="2025-06-27"
  END_DATE="2025-08-05"
"""

import os
import json
import time
import datetime as dt
from typing import Dict, List, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ----------------- утилиты и ENV -----------------
def log(msg: str) -> None:
    print(f"[INFO] {msg}", flush=True)

def require_env(name: str) -> str:
    v = os.environ.get(name, "").strip()
    if not v:
        raise RuntimeError(f"ENV '{name}' is missing or empty")
    return v

AVITO_CLIENT_ID = require_env("AVITO_CLIENT_ID")
AVITO_CLIENT_SECRET = require_env("AVITO_CLIENT_SECRET")
AVITO_USER_ID = require_env("AVITO_USER_ID")
SHEET_ID = require_env("SHEET_ID")
GOOGLE_SERVICE_JSON = require_env("GOOGLE_SERVICE_JSON")

ITEM_IDS_CSV = os.environ.get("ITEM_IDS_CSV", "").strip()
START_DATE_ENV = os.environ.get("START_DATE", "").strip()
END_DATE_ENV = os.environ.get("END_DATE", "").strip()

def parse_date(s: str) -> dt.date:
    return dt.datetime.strptime(s, "%Y-%m-%d").date()

# ----------------- HTTP session с ретраями -----------------
def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=4,
        read=4,
        connect=4,
        backoff_factor=0.7,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

SESSION = make_session()

# ----------------- Avito API -----------------
def get_token() -> str:
    r = SESSION.post(
        "https://api.avito.ru/token",
        data={
            "grant_type": "client_credentials",
            "client_id": AVITO_CLIENT_ID,
            "client_secret": AVITO_CLIENT_SECRET,
        },
        timeout=60,
    )
    log(f"/token → {r.status_code}")
    if r.status_code >= 400:
        log(f"AUTH BODY: {r.text[:800]}")
    r.raise_for_status()
    token = r.json().get("access_token")
    if not token:
        raise RuntimeError("No access_token in /token response")
    return token

def list_items(token: str, user_id: str, per_page: int = 100) -> List[int]:
    ids: List[int] = []
    page = 1
    headers = {"Authorization": f"Bearer {token}"}

    while True:
        url = (
            f"https://api.avito.ru/core/v1/accounts/{user_id}/items"
            f"?per_page={per_page}&page={page}"
            f"&status=active,old,removed,blocked,rejected"
        )
        r = SESSION.get(url, headers=headers, timeout=60)
        log(f"GET items p{page} → {r.status_code}")
        if r.status_code == 404:
            url_fallback = (
                f"https://api.avito.ru/core/v1/items"
                f"?per_page={per_page}&page={page}"
                f"&status=active,old,removed,blocked,rejected"
            )
            r = SESSION.get(url_fallback, headers=headers, timeout=60)
            log(f"GET items (fallback) p{page} → {r.status_code}")
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
                    pass

        if len(resources) < per_page:
            break
        page += 1
        if page > 200:
            break
        time.sleep(0.15)

    ids = sorted(set(ids))
    log(f"Items found: {len(ids)}")
    return ids

def stats_v1(token: str, user_id: str, item_ids: List[int], date_str: str) -> Dict:
    url = f"https://api.avito.ru/stats/v1/accounts/{user_id}/items"
    body = {
        "dateFrom": date_str,
        "dateTo": date_str,
        "fields": ["uniqViews", "uniqContacts"],
        "itemIds": item_ids[:200],  # лимит 200 на запрос
        "periodGrouping": "day",
    }
    r = SESSION.post(url, json=body, headers={"Authorization": f"Bearer {token}"}, timeout=120)
    log(f"POST v1 stats batch({len(body['itemIds'])}) → {r.status_code}")
    if r.status_code >= 400:
        log(f"STATS BODY: {r.text[:1000]}")
    r.raise_for_status()
    return r.json()

def get_item_info(token: str, user_id: str, item_id: int) -> Dict:
    url = f"https://api.avito.ru/core/v1/accounts/{user_id}/items/{item_id}/"
    r = SESSION.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=60)
    if r.status_code == 404:
        log(f"item {item_id} → 404 (skip)")
        return {}
    if r.status_code >= 400:
        log(f"item {item_id} → {r.status_code} {r.text[:400]}")
    r.raise_for_status()
    return r.json() or {}

# ----------------- Google Sheets -----------------
def connect_sheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(GOOGLE_SERVICE_JSON), scope)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)
    try:
        ws = sh.worksheet("data")
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title="data", rows="100000", cols="20")
        ws.append_row([
            "date", "item_id", "title",
            "uniqViews", "uniqContacts",
            "vas_ids", "vas_finish_time", "vas_next_schedule"
        ])
    return ws

def clear_date(ws, date_str: str):
    # удалить все строки с этой датой (в колонке A = 'date')
    matches = ws.findall(date_str, in_column=1)
    if not matches:
        return
    for cell in sorted(matches, key=lambda c: c.row, reverse=True):
        if cell.row > 1:
            ws.delete_rows(cell.row)

# ----------------- обработка одного дня -----------------
def process_one_day(token: str, ws, user_id: str, the_date: dt.date, item_ids: List[int]):
    date_str = the_date.strftime("%Y-%m-%d")
    log(f"== Day {date_str} ==")

    # собираем только “активные в дату”: есть запись stats с этой датой
    all_stats_for_day: Dict[str, Dict[str, int]] = {}  # itemId -> metrics

    for i in range(0, len(item_ids), 200):
        batch = item_ids[i:i+200]
        resp = stats_v1(token, user_id, batch, date_str)
        items = (((resp or {}).get("result") or {}).get("items")) or []
        for it in items:
            iid = str(it.get("itemId") or it.get("item_id") or "")
            if not iid:
                continue
            for s in (it.get("stats") or []):
                if s.get("date") == date_str:
                    rec = all_stats_for_day.setdefault(iid, {"uniqViews": 0, "uniqContacts": 0})
                    rec["uniqViews"] += int(s.get("uniqViews", 0) or 0)
                    rec["uniqContacts"] += int(s.get("uniqContacts", 0) or 0)
        time.sleep(0.1)

    log(f"Active items on {date_str}: {len(all_stats_for_day)}")
    if not all_stats_for_day:
        log("No items considered active that day — skip writing.")
        return

    # чистим старые строки за этот день, чтобы не было дублей
    clear_date(ws, date_str)
    log(f"Cleared previous rows for {date_str}")

    # пишем строки, подтягивая заголовок и VAS (кэшируем ответы)
    rows: List[List[str]] = []
    cache_info: Dict[str, Dict] = {}

    for iid, vals in sorted(all_stats_for_day.items(), key=lambda kv: int(kv[0])):
        if iid not in cache_info:
            info = get_item_info(token, user_id, int(iid)) or {}
            vas = info.get("vas") or []
            cache_info[iid] = {
                "title": info.get("title", "") or "",
                "vas_ids": ",".join(v.get("vas_id", "") or "" for v in vas),
                "finish": ",".join((v.get("finish_time") or "") for v in vas),
                "sched": "|".join(",".join(v.get("schedule") or []) for v in vas),
            }
            time.sleep(0.05)
        ci = cache_info[iid]
        rows.append([
            date_str, iid, ci["title"],
            vals.get("uniqViews", 0), vals.get("uniqContacts", 0),
            ci["vas_ids"], ci["finish"], ci["sched"]
        ])

    if rows:
        log(f"Appending {len(rows)} rows…")
        ws.append_rows(rows, value_input_option="USER_ENTERED")
        log("Done.")
    else:
        log("No rows to append.")

# ----------------- MAIN -----------------
def main():
    log("== Avito → Google Sheets collector start ==")

    # режим: один день (сегодня-3) или бэкфилл
    if START_DATE_ENV:
        start_date = parse_date(START_DATE_ENV)
        end_date = parse_date(END_DATE_ENV) if END_DATE_ENV else (dt.date.today() - dt.timedelta(days=3))
        if end_date < start_date:
            raise RuntimeError("END_DATE is earlier than START_DATE")
        log(f"Backfill mode: {start_date} → {end_date}")
    else:
        today = dt.date.today()
        start_date = end_date = today - dt.timedelta(days=3)
        log(f"Single day mode: {start_date}")

    token = get_token()
    ws = connect_sheet()

    # список объявлений
    if ITEM_IDS_CSV:
        item_ids: List[int] = []
        for chunk in ITEM_IDS_CSV.split(","):
            chunk = chunk.strip()
            if not chunk:
                continue
            try:
                item_ids.append(int(chunk))
            except Exception:
                pass
        item_ids = sorted(set(item_ids))
        log(f"ITEM_IDS_CSV provided, using {len(item_ids)} ids")
    else:
        item_ids = list_items(token, AVITO_USER_ID)
    if not item_ids:
        log("No items found — nothing to do.")
        return

    # пройти по датам включительно
    current = start_date
    while current <= end_date:
        process_one_day(token, ws, AVITO_USER_ID, current, item_ids)
        current += dt.timedelta(days=1)

# >>>>>>>>>>> ОБЯЗАТЕЛЬНО: блок запуска <<<<<<<<<<<
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        log(f"ERROR: {e}")
        traceback.print_exc()
        raise
