# -*- coding: utf-8 -*-
"""
Avito → Google Sheets (помесячно/понедельно/посуточно через profile-stats)
Режимы:
- По умолчанию: собираем ОДИН день = (сегодня - 3) и пишем на лист 'data'
- Бэкфилл: задать ENV START_DATE и (опц.) END_DATE (YYYY-MM-DD), скрипт пройдёт по датам включительно

Главное:
- Пытаемся получить метрики через "статистику профиля" с группировкой по объявлениям и дням.
- Если профильная ручка недоступна (404/400), fallback на старую /stats/v1...items (uniqViews/uniqContacts).
- title берём из листинга; VAS (vip, highlight, pushup, premium, xl) — текущее состояние (снимок "сейчас").

ENV (GitHub Secrets обязательные):
  AVITO_CLIENT_ID, AVITO_CLIENT_SECRET, AVITO_USER_ID, SHEET_ID, GOOGLE_SERVICE_JSON
Опциональные:
  ITEM_IDS_CSV="123,456"        # задать набор ID руками (ускоряет)
  START_DATE="2025-06-27"
  END_DATE="2025-08-05"
  PROFILE_STATS_URL             # на случай, если у вас другой путь, по умолчанию: https://api.avito.ru/stats/v3/accounts/{user_id}/profile
"""

import os
import json
import time
import datetime as dt
from typing import Dict, List, Any, Tuple

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
PROFILE_STATS_URL_TPL = os.environ.get("PROFILE_STATS_URL", "https://api.avito.ru/stats/v3/accounts/{user_id}/profile")

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

def list_items_with_titles(token: str, user_id: str, per_page: int = 100) -> Tuple[List[int], Dict[int, str], Dict[int, Dict[str, int]]]:
    """Возвращает список ID, мапу id->title (из листинга) и флаги VAS (id-> {'vip':0/1, 'highlight':..., ...})."""
    ids: List[int] = []
    titles: Dict[int, str] = {}
    vas_flags: Dict[int, Dict[str, int]] = {}
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
            if not iid:
                continue
            try:
                iid = int(iid)
            except Exception:
                continue
            ids.append(iid)
            # title из листинга
            t = (x.get("title") or "").strip()
            if t:
                titles[iid] = t
            # vas из листинга (если отдают)
            flags = {"vip":0,"highlight":0,"pushup":0,"premium":0,"xl":0}
            for v in (x.get("vas") or []):
                vas_id = v.get("vas_id")
                if vas_id in flags:
                    flags[vas_id] = 1
            # если vas нет в листинге — заполним позже из item_info, но базовый dict создадим
            vas_flags[iid] = flags

        if len(resources) < per_page:
            break
        page += 1
        if page > 200:
            break
        time.sleep(0.15)

    ids = sorted(set(ids))
    log(f"Items found: {len(ids)}")
    return ids, titles, vas_flags

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

# -------- профильная статистика: item+day, с нужными метриками --------
PROFILE_METRICS = [
    # основные
    "impressions", "views",
    "impressionsToViewsConversion",
    "contacts", "contactsShowPhone", "contactsMessenger",
    "contactsShowPhoneAndMessenger", "contactsSbcDiscount",
    "viewsToContactsConversion",
    "favorites",
    "averageViewCost", "averageContactCost",
    # расходы
    "allSpending", "spending", "presenceSpending",
    "promoSpending", "restSpending", "commission", "spendingBonus",
]

def fetch_profile_stats_one_day(token: str, user_id: str, day: str, limit: int = 1000) -> List[Dict[str, Any]]:
    """
    Возвращает список записей вида:
      { "itemId": 123, "date": "YYYY-MM-DD", <метрики...> }
    Пагинация по 1000, offset++.
    """
    url = PROFILE_STATS_URL_TPL.format(user_id=user_id)
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    out: List[Dict[str, Any]] = []

    offset = 0
    page = 1
    while True:
        body = {
            "dateFrom": day,
            "dateTo": day,
            "grouping": "item",          # получим по объявлениям
            "limit": limit,
            "offset": offset,
            "metrics": PROFILE_METRICS,
            # Можно добавить фильтр по статусу, если поддерживается в этой ручке:
            # "filter": {"status": ["active","old","removed","blocked","rejected"]}
        }
        r = SESSION.post(url, headers=headers, json=body, timeout=120)
        log(f"POST profile-stats p{page} → {r.status_code}")
        if r.status_code >= 400:
            # если ручка недоступна — дадим знать верхнему уровню
            log(f"PROFILE BODY: {r.text[:1000]}")
            r.raise_for_status()

        data = r.json() or {}
        items = data.get("result") or data.get("items") or data.get("resources") or []
        # Многие ручки отдают структуру: {"result":{"items":[...]}}
        if "result" in data:
            inner = data["result"]
            items = inner.get("items") or inner.get("data") or items

        if not items:
            break

        for it in items:
            # Ищем поля-ключи
            iid = it.get("itemId") or it.get("item_id") or it.get("itemID")
            d = it.get("date") or it.get("day") or it.get("period")
            if iid and d:
                try:
                    iid = int(iid)
                except Exception:
                    continue
                out.append(it)

        # пагинация
        n = len(items)
        if n < limit:
            break
        offset += limit
        page += 1

        # если лимит 1/мин, притормозим
        time.sleep(1.0)

    return out

# -------- старый stats v1 (fallback) --------
def stats_v1(token: str, user_id: str, item_ids: List[int], date_str: str) -> Dict:
    url = f"https://api.avito.ru/stats/v1/accounts/{user_id}/items"
    body = {
        "dateFrom": date_str,
        "dateTo": date_str,
        "fields": ["uniqViews", "uniqContacts"],
        "itemIds": item_ids[:200],
        "periodGrouping": "day",
    }
    r = SESSION.post(url, json=body, headers={"Authorization": f"Bearer {token}"}, timeout=120)
    log(f"POST v1 stats batch({len(body['itemIds'])}) → {r.status_code}")
    if r.status_code >= 400:
        log(f"STATS BODY: {r.text[:1000]}")
    r.raise_for_status()
    return r.json()

# ----------------- Google Sheets -----------------
DATA_HEADERS = [
    "date", "item_id", "title",
    # основные
    "impressions", "views",
    "impressionsToViewsConversion",
    "contacts", "contactsShowPhone", "contactsMessenger",
    "contactsShowPhoneAndMessenger", "contactsSbcDiscount",
    "viewsToContactsConversion",
    "favorites",
    "averageViewCost", "averageContactCost",
    # fallback метрики (uniq*)
    "uniqViews", "uniqContacts",
    # расходы
    "allSpending", "spending", "presenceSpending",
    "promoSpending", "restSpending", "commission", "spendingBonus",
    # VAS (снимок сейчас)
    "vas_ids", "vas_finish_time", "vas_next_schedule",
    "vas_flag_vip", "vas_flag_highlight", "vas_flag_pushup", "vas_flag_premium", "vas_flag_xl",
]

def connect_sheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(GOOGLE_SERVICE_JSON), scope)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)
    try:
        ws = sh.worksheet("data")
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title="data", rows="100000", cols=str(len(DATA_HEADERS) + 10))
        ws.append_row(DATA_HEADERS)
        return ws

    # гарантируем заголовки
    headers = ws.row_values(1)
    changed = False
    for h in DATA_HEADERS:
        if h not in headers:
            headers.append(h); changed = True
    if changed:
        ws.resize(rows=max(ws.row_count, 2), cols=len(headers))
        ws.update("1:1", [headers])
    return ws

def clear_date(ws, date_str: str):
    matches = ws.findall(date_str, in_column=1)
    if not matches:
        return
    for cell in sorted(matches, key=lambda c: c.row, reverse=True):
        if cell.row > 1:
            ws.delete_rows(cell.row)

# ----------------- обработка одного дня -----------------
def process_one_day(token: str, ws, user_id: str, the_date: dt.date, item_ids: List[int],
                    titles_from_list: Dict[int, str], vas_flags_from_list: Dict[int, Dict[str, int]]):
    date_str = the_date.strftime("%Y-%m-%d")
    log(f"== Day {date_str} ==")

    # 1) профильная статистика item+day
    rows_out: List[List[Any]] = []

    try:
        prof = fetch_profile_stats_one_day(token, user_id, date_str)
    except requests.HTTPError as e:
        log(f"profile-stats not available, fallback to v1: {e}")
        prof = None

    # Кэш для vas/title по item_info (если не было в листинге)
    cache_info: Dict[int, Dict[str, Any]] = {}

    # Почти всегда хотим писать ровно "активные в дату".
    active_ids_today: set = set()

    # 2) Если есть профильные данные — разложим их по строкам
    if prof:
        # карта itemId -> {метрики...} (на случай дублей)
        agg: Dict[int, Dict[str, Any]] = {}
        for it in prof:
            iid = int(it.get("itemId"))
            d = it.get("date") or it.get("day") or ""
            if d != date_str:
                continue
            active_ids_today.add(iid)
            tgt = agg.setdefault(iid, {})
            # переносим только известные поля, отсутствующие считаем 0
            for key in PROFILE_METRICS:
                val = it.get(key, 0)
                if isinstance(val, (int, float)):
                    tgt[key] = tgt.get(key, 0) + val
                else:
                    # иногда приходят строки (конверсии проценты) — оставим как есть
                    tgt[key] = val

        # 3) соберём строки
        for iid in sorted(active_ids_today):
            m = agg.get(iid, {})
            title = titles_from_list.get(iid, "")

            # если в листинге нет title — дотащим по item_info
            if not title:
                if iid not in cache_info:
                    info = get_item_info(token, user_id, iid) or {}
                    cache_info[iid] = info
                    time.sleep(0.05)
                title = (cache_info[iid].get("title") or "").strip()

            # vas flags
            flags = vas_flags_from_list.get(iid, {"vip":0,"highlight":0,"pushup":0,"premium":0,"xl":0})
            # vas ids/finish/sched (снимок сейчас)
            vas_ids, vas_finish, vas_sched = "", "", ""
            if iid not in cache_info:
                info = get_item_info(token, user_id, iid) or {}
                cache_info[iid] = info
                time.sleep(0.05)
            vas_list = cache_info[iid].get("vas") or []
            if vas_list:
                vas_ids = ",".join(v.get("vas_id","") or "" for v in vas_list)
                vas_finish = ",".join((v.get("finish_time") or "") for v in vas_list)
                vas_sched = "|".join(",".join(v.get("schedule") or []) for v in vas_list)
                # возможные доп. флаги
                for v in vas_list:
                    vid = v.get("vas_id")
                    if vid in flags:
                        flags[vid] = 1

            row = [
                date_str, str(iid), title,
                m.get("impressions", 0), m.get("views", 0),
                m.get("impressionsToViewsConversion", 0),
                m.get("contacts", 0), m.get("contactsShowPhone", 0), m.get("contactsMessenger", 0),
                m.get("contactsShowPhoneAndMessenger", 0), m.get("contactsSbcDiscount", 0),
                m.get("viewsToContactsConversion", 0),
                m.get("favorites", 0),
                m.get("averageViewCost", 0), m.get("averageContactCost", 0),
                # fallback поля — проф.ручка их обычно не отдаёт, оставим 0
                0, 0,
                # расходы
                m.get("allSpending", 0), m.get("spending", 0), m.get("presenceSpending", 0),
                m.get("promoSpending", 0), m.get("restSpending", 0), m.get("commission", 0), m.get("spendingBonus", 0),
                # vas snapshot
                vas_ids, vas_finish, vas_sched,
                flags.get("vip",0), flags.get("highlight",0), flags.get("pushup",0), flags.get("premium",0), flags.get("xl",0),
            ]
            rows_out.append(row)

        log(f"Active items on {date_str} (profile): {len(rows_out)}")

    # 4) Если профильной статистики нет — fallback: uniqViews/uniqContacts по v1
    if not prof:
        # “активные в дату” = есть запись stats на эту дату
        active_map: Dict[int, Dict[str,int]] = {}
        batch_ids = item_ids
        for i in range(0, len(batch_ids), 200):
            b = batch_ids[i:i+200]
            resp = stats_v1(token, user_id, b, date_str)
            items = (((resp or {}).get("result") or {}).get("items")) or []
            for it in items:
                iid = it.get("itemId") or it.get("item_id")
                if not iid:
                    continue
                iid = int(iid)
                for s in (it.get("stats") or []):
                    if s.get("date") == date_str:
                        rec = active_map.setdefault(iid, {"uniqViews":0,"uniqContacts":0})
                        rec["uniqViews"] += int(s.get("uniqViews",0) or 0)
                        rec["uniqContacts"] += int(s.get("uniqContacts",0) or 0)
            time.sleep(0.1)

        log(f"Active items on {date_str} (v1): {len(active_map)}")

        # соберём строки
        for iid, vals in sorted(active_map.items(), key=lambda kv: kv[0]):
            title = titles_from_list.get(iid, "")
            if not title:
                info = get_item_info(token, user_id, iid) or {}
                title = (info.get("title") or "").strip()
                time.sleep(0.05)

            # vas snapshot
            info = get_item_info(token, user_id, iid) or {}
            vas_list = info.get("vas") or []
            vas_ids = ",".join(v.get("vas_id","") or "" for v in vas_list)
            vas_finish = ",".join((v.get("finish_time") or "") for v in vas_list)
            vas_sched = "|".join(",".join(v.get("schedule") or []) for v in vas_list)
            flags = {"vip":0,"highlight":0,"pushup":0,"premium":0,"xl":0}
            for v in vas_list:
                vid = v.get("vas_id")
                if vid in flags:
                    flags[vid] = 1

            row = [
                date_str, str(iid), title,
                # проф.поля отсутствуют
                0, 0, 0,
                0, 0, 0,
                0, 0,
                0,
                0,
                0, 0,
                # fallback uniq*
                vals.get("uniqViews",0), vals.get("uniqContacts",0),
                # расходы (нет)
                0,0,0,0,0,0,0,
                # vas
                vas_ids, vas_finish, vas_sched,
                flags.get("vip",0), flags.get("highlight",0), flags.get("pushup",0), flags.get("premium",0), flags.get("xl",0),
            ]
            rows_out.append(row)

    # 5) Запись в таблицу (чистим день → добавляем)
    if not rows_out:
        log("No rows — nothing to write.")
        return

    # гарантируем порядок заголовков и их наличие
    headers = ws.row_values(1)
    header_index = {h: i for i, h in enumerate(headers)}
    changed = False
    for h in DATA_HEADERS:
        if h not in header_index:
            headers.append(h); changed = True
    if changed:
        ws.resize(rows=max(ws.row_count, 2), cols=len(headers))
        ws.update("1:1", [headers])
        header_index = {h: i for i, h in enumerate(headers)}

    clear_date(ws, date_str)
    log(f"Cleared previous rows for {date_str}")

    # если вдруг у нас появились новые колонки после ensure — расширим строки
    width = len(headers)
    normalized_rows = []
    for r in rows_out:
        if len(r) < width:
            r = r + [""]*(width - len(r))
        elif len(r) > width:
            r = r[:width]
        normalized_rows.append(r)

    log(f"Appending {len(normalized_rows)} rows…")
    ws.append_rows(normalized_rows, value_input_option="USER_ENTERED")
    log("Done.")

# ----------------- MAIN -----------------
def parse_date(s: str) -> dt.date:
    return dt.datetime.strptime(s, "%Y-%m-%d").date()

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

    # список объявлений и заголовки из листинга
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
        titles_map = {iid: "" for iid in item_ids}
        vas_flags = {iid: {"vip":0,"highlight":0,"pushup":0,"premium":0,"xl":0} for iid in item_ids}
        log(f"ITEM_IDS_CSV provided, using {len(item_ids)} ids")
    else:
        item_ids, titles_map, vas_flags = list_items_with_titles(token, AVITO_USER_ID)

    if not item_ids:
        log("No items found — nothing to do.")
        return

    # пройти по датам включительно
    current = start_date
    while current <= end_date:
        process_one_day(token, ws, AVITO_USER_ID, current, item_ids, titles_map, vas_flags)
        current += dt.timedelta(days=1)

# Обязательный запускной блок
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        log(f"ERROR: {e}")
        traceback.print_exc()
        raise
