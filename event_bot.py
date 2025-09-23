# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import json
import logging
import os
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

# ============= ログ設定 =============
logging.basicConfig(level=logging.INFO, format="%(asctime)s\t%(levelname)s\t%(message)s")

# ============= Google Sheets 設定 =============
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SERVICE_ACCOUNT_FILE = "service_account.json"  # GitHub Secrets から書き出して使う想定

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")  # これも Secrets に保存
SHEET_NAME = "sent_events"

def init_sheet():
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)
    try:
        sheet = sh.worksheet(SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        sheet = sh.add_worksheet(title=SHEET_NAME, rows=1000, cols=10)
        sheet.append_row(["送信日", "イベント名", "開催期間", "URL", "会場"])
    return sheet

# ============= 履歴判定（URL基準） =============
def already_sent(event_url, sheet):
    records = sheet.col_values(4)  # URL の列を取得
    return event_url in records

def save_event(sheet, event):
    today = datetime.now().strftime("%Y-%m-%d")
    sheet.append_row([
        today,
        event.get("name", ""),
        f"{event.get('startDate','')} ～ {event.get('endDate','')}",
        event.get("url", ""),
        event.get("location", {}).get("name", "")
    ])

# ============= イベント取得 =============
def fetch_walkerplus_events(base_url: str, max_pages: int = 2):
    events = []
    for page in range(1, max_pages + 1):
        url = base_url if page == 1 else f"{base_url}{page}.html"
        logging.info(f"取得開始: {url}")
        try:
            res = requests.get(url, timeout=10)
            res.raise_for_status()
            soup = BeautifulSoup(res.text, "html.parser")

            for tag in soup.find_all("script", {"type": "application/ld+json"}):
                try:
                    data = json.loads(tag.string)
                    if isinstance(data, list):
                        for ev in data:
                            if ev.get("@type") == "Event":
                                events.append(ev)
                    elif isinstance(data, dict) and data.get("@type") == "Event":
                        events.append(data)
                except Exception as e:
                    logging.warning(f"JSON-LD parse error: {e}")
        except Exception as e:
            logging.error(f"❌ エラー: {e}")
    logging.info(f"✅ 合計 {len(events)} 件取得 (最大{max_pages}ページ)")
    return events

# ============= LINE送信（Messaging API） =============
LINE_ACCESS_TOKEN = os.getenv("LINE_ACCESS_TOKEN")
HEADERS = {
    "Content-Type": "application/json; charset=UTF-8",
    "Authorization": f"Bearer {LINE_ACCESS_TOKEN}"
}

def send_line_broadcast(message: str):
    payload = {
        "messages": [
            {
                "type": "text",
                "text": message
            }
        ]
    }
    resp = requests.post(
        "https://api.line.me/v2/bot/message/broadcast",
        headers=HEADERS,
        data=json.dumps(payload).encode("utf-8")
    )
    logging.info(f"LINE送信結果: {resp.status_code} {resp.text}")

# ============= メイン処理 =============
def main():
    logging.info("🚀 START")
    sheet = init_sheet()

    prefectures = {
        "東京": "https://www.walkerplus.com/event_list/ar0313/",
        "神奈川": "https://www.walkerplus.com/event_list/ar0314/",
        "千葉": "https://www.walkerplus.com/event_list/ar0312/",
        "埼玉": "https://www.walkerplus.com/event_list/ar0311/"
    }

    for pref, url in prefectures.items():
        events = fetch_walkerplus_events(url, max_pages=2)
        if not events:
            continue

        messages = [f"📍 {pref} の新着イベント 🎪"]
        new_count = 0

        for ev in events:
            url_key = ev.get("url")
            if not url_key or already_sent(url_key, sheet):
                continue

            msg = (
                f"🎪 {ev.get('name')}\n"
                f"📅 {ev.get('startDate')} ～ {ev.get('endDate')}\n"
                f"📍 {ev.get('location', {}).get('name')}\n"
                f"🔗 {ev.get('url')}"
            )
            messages.append(msg)

            save_event(sheet, ev)
            new_count += 1

            if new_count >= 10:  # 送信件数制限
                break

        if new_count > 0:
            send_line_broadcast("\n\n".join(messages))

    logging.info("🏁 END")

# ============= 実行 =============
if __name__ == "__main__":
    main()
