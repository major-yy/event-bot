import requests
from bs4 import BeautifulSoup
import json
import logging
import os
from datetime import datetime, timedelta

# 保存ファイルのパス（リポジトリ内に data フォルダを作成）
BASE_DIR = os.path.dirname(__file__)
SAVE_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(SAVE_DIR, exist_ok=True)
SENT_FILE = os.path.join(SAVE_DIR, "sent_events.json")

# ============= 設定 =============
LINE_ACCESS_TOKEN = os.getenv("LINE_ACCESS_TOKEN")
if not LINE_ACCESS_TOKEN:
    raise ValueError("LINE_ACCESS_TOKEN が設定されていません")

HEADERS = {
    "Content-Type": "application/json; charset=UTF-8",
    "Authorization": f"Bearer {LINE_ACCESS_TOKEN}"
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s\t%(levelname)s\t%(message)s")

# ============= 送信済みイベント管理 =============
def load_sent_events():
    if os.path.exists(SENT_FILE):
        with open(SENT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_sent_events(sent_events):
    with open(SENT_FILE, "w", encoding="utf-8") as f:
        json.dump(sent_events, f, ensure_ascii=False, indent=2)

def clean_old_events(sent_events, keep_days=365):
    cutoff = datetime.now() - timedelta(days=keep_days)
    cleaned = {}
    for key, info in sent_events.items():
        try:
            date_str = info.get("date")
            if date_str:
                ev_date = datetime.strptime(date_str, "%Y-%m-%d")
                if ev_date >= cutoff:
                    cleaned[key] = info
        except Exception:
            continue
    return cleaned

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
                    elif isinstance(data, dict):
                        if data.get("@type") == "Event":
                            events.append(data)
                except Exception as e:
                    logging.warning(f"JSON-LD parse error: {e}")
        except Exception as e:
            logging.error(f"❌ エラー: {e}")
            continue

    logging.info(f"✅ 合計 {len(events)} 件取得 (最大{max_pages}ページ)")
    return events

# ============= LINE送信 =============
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
    print("LINE送信結果:", resp.status_code, resp.text)

# ============= メイン処理 =============
def main():
    print("🚀 START")

    prefectures = {
        "東京": "https://www.walkerplus.com/event_list/ar0313/",
        "神奈川": "https://www.walkerplus.com/event_list/ar0314/",
        "千葉": "https://www.walkerplus.com/event_list/ar0312/",
        "埼玉": "https://www.walkerplus.com/event_list/ar0311/"
    }

    sent_events = load_sent_events()
    sent_events = clean_old_events(sent_events, keep_days=365)
    new_sent = dict(sent_events)

    today = datetime.now().strftime("%Y-%m-%d")

    for pref, url in prefectures.items():
        events = fetch_walkerplus_events(url, max_pages=2)

        if not events:
            continue

        print(f"\n=== {pref} の取得イベント一覧 ===")
        for i, ev in enumerate(events, 1):
            print(f"{i}. {ev.get('name')} | {ev.get('url')}")

        messages = [f"📍 {pref} の新着イベント 🎪"]
        count = 0
        new_count = 0

        for ev in events:
            key = ev.get("url") or ev.get("name")
            if key in sent_events:
                continue

            msg = (
                f"🎪 {ev.get('name')}\n"
                f"📅 {ev.get('startDate')} ～ {ev.get('endDate')}\n"
                f"📍 {ev.get('location', {}).get('name')}\n"
                f"🏠 {ev.get('location', {}).get('address', {}).get('addressRegion', '')} "
                f"{ev.get('location', {}).get('address', {}).get('addressLocality', '')}\n"
                f"🔗 {ev.get('url')}\n"
                f"📝 {(ev.get('description') or '')[:80]}..."
            )
            messages.append(msg)

            new_sent[key] = {"date": today}
            new_count += 1
            count += 1

            if count >= 11:
                break

        print(f"🆕 新規: {new_count}件, ⏭ スキップ: {len(events)-new_count}件")

        if new_count > 0:
            send_line_broadcast("\n\n".join(messages))

    save_sent_events(new_sent)
    print("🏁 END")

if __name__ == "__main__":
    main()
