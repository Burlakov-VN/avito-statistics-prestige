# -*- coding: utf-8 -*-
"""
Avito → Google Sheets (сбор за один день = сегодня-3)
- /token (client_credentials)
- Список объявлений:   GET /core/v1/accounts/{user_id}/items
- Дневная статистика:  POST /stats/v1/accounts/{user_id}/items  (periodGrouping=day)
- Инфо по объявлению:  GET /core/v1/accounts/{user_id}/items/{item_id}/  (поле vas)
ENV (GitHub Secrets):
  AVITO_CLIENT_ID, AVITO_CLIENT_SECRET, AVITO_USER_ID, SHEET_ID, GOOGLE_SERVICE_JSON
Опционально:
  ITEM_IDS_CSV="123,456,789"  # если нужно собирать только по заданным ID
"""

import os
import json
import time
import datetime as dt
from typing import Dict, List, Tuple

import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ---------- утилиты ----------
def log(msg: str) -> None:
    print(f"[INFO] {msg}", flush=True)

def require_env(name: str) -> str:
    val = os.environ.get(name)
    if not val or not str(val).strip():
        raise RuntimeError(f"ENV '{name}' is missing or empty")
    return val

# ---------- конфиг ----------
AVITO_CLIENT_ID = require_env("AVITO_CLIENT_ID")
AVITO_CLIENT_SECRET = require_env("AVITO_CLIENT_SECRET")
AVITO_USER_ID = require_env("AVITO_USER_ID")       # пример: 363007001
SHEET_ID = require_env("SHEET_ID")
GOOGLE_SERVICE_JSON = require_env("GOOGLE_SERVICE_JSON")
ITEM_IDS_CSV = os.environ.get("ITEM_IDS_CSV", "").strip()  # опционально

# ---------- API ----------
def get_token() -> str:
    r = requests.post(
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
    """Собираем ВСЕ объявления аккаунта (любой статус) постранично."""
    ids: List[int] = []
    page = 1
    headers = {"Authorization": f"Bearer {token}"}

    while True:
        url = (
            f"https://api.avito.ru/core/v1/accounts/{user_id}/items"
            f"?per_page={per_page}&page={page}"
            f"&status=active,old,removed,blocked,rejected"
        )
        r = requests.get(url, headers=headers, timeout=60)
        log(f"GET items p{page} → {r.status_code}")
        if r.status_code == 404:
            # Редкий фоллбек
            url_fallback = (
                f"https://api.avito.ru/core/v1/items"
                f"?per_page={per_page}&page={page}"
                f"&status=active,old,removed,blocked,rejected"
            )
            r = requests.get(url_fallback, headers=headers, timeout=60)
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
                    continue

        if len(resources) < per_page:
            break

        page += 1
        if page > 100:  # защита от зацикливания
            break

        time.sleep(0.2)  # мягкий троттлинг

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
    r = requests.post(url, json=body, headers={"Authorization": f"Bearer {token}"}, timeout=120)
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
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=60)
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
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title="data", rows="50000", cols="20")
        ws.append_row([
            "date", "item_id", "title",
            "uniqViews", "uniqContacts",
            "vas_ids", "vas_finish_time", "vas_next_schedule"
        ])
    return ws

# ---------- main ----------
def main():
    log("== Avito → Google Sheets collector start ==")

    # === ВАЖНО: собираем РОВНО один день = сегодня-3 ===
    today = dt.date.today()
    target_date = (today - dt.timedelta(days=3)).strftime("%Y-%m-%d")
    date_from = target_date
    date_to = target_date
    log(f"Period (single day): {date_from} → {date_to}")

    token = get_token()

    # 1) Список объявлений (или используем ручной список из ITEM_IDS_CSV)
    if ITEM_IDS_CSV:
        item_ids = []
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
        log("Нет объявлений — статистику собрать не из чего. (Можно задать ITEM_IDS_CSV)")
        return

    # 2) Забираем статистику пачками по 200 id
    all_stats: Dict[Tuple[str, str], Dict[str, int]] = {}  # (date, itemId) -> {uniqViews, uniqContacts}
    for i in range(0, len(item_ids), 200):
        batch = item_ids[i:i+200]
        resp = stats_v1(token, AVITO_USER_ID, batch, date_from, date_to)
        items = (((resp or {}).get("result") or {}).get("items")) or []
        for it in items:
            iid = str(it.get("itemId") or it.get("item_id") or "")
            if not iid:
                continue
            for s in it.get("stats", []):
                d = s.get("date")
                if not d:
                    continue
                key = (d, iid)
                rec = all_stats.setdefault(key, {"uniqViews": 0, "uniqContacts": 0})
                rec["uniqViews"] += int(s.get("uniqViews", 0) or 0)
                rec["uniqContacts"] += int(s.get("uniqContacts", 0) or 0)
        time.sleep(0.2)  # мягкий троттлинг

    log(f"Stat points collected: {len(all_stats)}")
    if not all_stats:
        log("API вернул пустую статистику за этот день.")
        return

    # 3) Пишем в Google Sheets и подтягиваем VAS/Title (кэшируем ответы)
    ws = connect_sheet()
    rows: List[List[str]] = []
    cache_info: Dict[str, Dict] = {}

    for (d, iid), vals in sorted(all_stats.items()):
        if iid not in cache_info:
            info = get_item_info(token, AVITO_USER_ID, int(iid)) or {}
            vas = info.get("vas") or []
            cache_info[iid] = {
                "title": info.get("title", "") or "",
                "vas_ids": ",".join(v.get("vas_id", "") or "" for v in vas),
                "finish": ",".join((v.get("finish_time") or "") for v in vas),
                "sched": "|".join(",".join(v.get("schedule") or []) for v in vas),
            }
            time.sleep(0.15)

        ci = cache_info[iid]
        rows.append([
            d, iid, ci["title"],
            vals.get("uniqViews", 0), vals.get("uniqContacts", 0),
            ci["vas_ids"], ci["finish"], ci["sched"]
        ])

    if rows:
        log(f"Appending {len(rows)} rows to Google Sheets…")
        ws.append_rows(rows, value_input_option="USER_ENTERED")
        log("Done.")
    else:
        log("Сформированных строк нет (rows пуст).")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        log(f"ERROR: {e}")
        traceback.print_exc()
        raise
