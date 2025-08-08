import os, json, datetime as dt
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials

AVITO_CLIENT_ID = os.environ["AVITO_CLIENT_ID"]
AVITO_CLIENT_SECRET = os.environ["AVITO_CLIENT_SECRET"]
AVITO_USER_ID = os.environ["AVITO_USER_ID"]  # 363007001
SHEET_ID = os.environ["SHEET_ID"]
GOOGLE_SERVICE_JSON = os.environ["GOOGLE_SERVICE_JSON"]

def get_avito_token():
    r = requests.post("https://api.avito.ru/token", data={
        "grant_type": "client_credentials",
        "client_id": AVITO_CLIENT_ID,
        "client_secret": AVITO_CLIENT_SECRET
    }, timeout=60)
    r.raise_for_status()
    return r.json()["access_token"]

def get_stats(token, user_id, date_from, date_to):
    url = f"https://api.avito.ru/core/v2/accounts/{user_id}/analytics/items"
    body = {
        "dateFrom": date_from, "dateTo": date_to,
        "metrics": ["views", "uniqContacts"],
        "grouping": ["day", "item"],
        "limit": 1000, "offset": 0
    }
    r = requests.post(url, json=body, headers={"Authorization": f"Bearer {token}"}, timeout=120)
    r.raise_for_status()
    return r.json()

def get_item_info(token, user_id, item_id):
    url = f"https://api.avito.ru/core/v1/accounts/{user_id}/items/{item_id}"
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
        ws.append_row(["date","item_id","title","views","uniqContacts","vas_ids","vas_finish_time","vas_next_schedule"])
    return ws

def main():
    date = (dt.date.today() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
    token = get_avito_token()
    stats = get_stats(token, os.environ["AVITO_USER_ID"], date, date)
    ws = connect_sheet()

    by_day, by_item = {}, {}
    for g in stats.get("result", {}).get("groupings", []):
        t, gid = g.get("type"), str(g.get("id"))
        m = {mm["slug"]: mm["value"] for mm in g.get("metrics", [])}
        if t == "day":
            try: gid = dt.datetime.utcfromtimestamp(int(gid)).strftime("%Y-%m-%d")
            except: pass
            by_day[gid] = m
        elif t == "item":
            by_item[gid] = m

    rows=[]
    for d, m_day in by_day.items():
        for item_id, m_item in by_item.items():
            info = get_item_info(token, os.environ["AVITO_USER_ID"], item_id) or {}
            title = info.get("title","")
            vas = info.get("vas") or []
            vas_ids = ",".join(v.get("vas_id","") for v in vas)
            finish_times = ",".join((v.get("finish_time") or "") for v in vas)
            schedules = "|".join(",".join(v.get("schedule") or []) for v in vas)
            rows.append([d, item_id, title, m_day.get("views",0), m_item.get("uniqContacts",0), vas_ids, finish_times, schedules])
    if rows:
        ws.append_rows(rows, value_input_option="USER_ENTERED")

if __name__ == "__main__":
    main()
