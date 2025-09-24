# -*- coding: utf-8 -*-
"""
event_bot.py
- Walker+ (JSON-LD) と TokyoArtBeat (requests + BeautifulSoup) からイベントを取得
- Google Sheets に送信済みURLを保存して重複配信を回避
- LINE公式アカウント (Messaging API broadcast) で配信

Env vars required in GitHub Actions:
  - GOOGLE_CREDENTIALS  (service account json string)
  - SPREADSHEET_ID
  - LINE_ACCESS_TOKEN
"""
import os
import json
import logging
import time
import random
import re
import urllib.parse
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials

# ============= ログ設定 =============
logging.basicConfig(level=logging.INFO, format="%(asctime)s\t%(levelname)s\t%(message)s")

# ============= Google Sheets 設定 =============
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SHEET_NAME = "sent_events"

def init_sheet():
    if not SPREADSHEET_ID:
        raise RuntimeError("SPREADSHEET_ID is not set in env")
    creds_json = os.environ["GOOGLE_CREDENTIALS"]
    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)

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
    if not event_url:
        return False
    # URL列は4列目 (1-indexed) — header exists
    try:
        records = sheet.col_values(4)
        return event_url in records
    except Exception as e:
        logging.warning("Google Sheets read failed: %s", e)
        return False

def save_event(sheet, event):
    today = datetime.now().strftime("%Y-%m-%d")
    period = ""
    # event may have start/end or startDate/endDate
    start = event.get("start") or event.get("startDate")
    end = event.get("end") or event.get("endDate")
    if start and end:
        # normalize ISO-like strings to YYYY-MM-DD if possible
        def norm(d):
            if not d:
                return ""
            if isinstance(d, str) and re.match(r"\d{4}-\d{2}-\d{2}", d):
                return d[:10]
            # try to parse yyyy年.. patterns already handled earlier; fallback raw
            return d
        period = f"{norm(start)} ～ {norm(end)}"
    elif start:
        period = start[:10] if isinstance(start, str) else str(start)

    url = event.get("official_url") or event.get("url") or event.get("detail_page") or ""
    venue = event.get("venue") or ""
    name = event.get("name") or ""

    try:
        sheet.append_row([today, name, period, url, venue])
    except Exception as e:
        logging.warning("Failed to append to sheet: %s", e)

# ============= LINE送信（Messaging API） =============
LINE_ACCESS_TOKEN = os.getenv("LINE_ACCESS_TOKEN")
HEADERS = {
    "Content-Type": "application/json; charset=UTF-8",
    "Authorization": f"Bearer {LINE_ACCESS_TOKEN}" if LINE_ACCESS_TOKEN else ""
}

def send_line_broadcast(message: str):
    if not LINE_ACCESS_TOKEN:
        logging.warning("LINE_ACCESS_TOKEN not set: skipping send")
        return
    payload = {
        "messages": [
            {
                "type": "text",
                "text": message
            }
        ]
    }
    try:
        resp = requests.post(
            "https://api.line.me/v2/bot/message/broadcast",
            headers=HEADERS,
            data=json.dumps(payload).encode("utf-8"),
            timeout=15
        )
        logging.info("LINE送信結果: %s %s", resp.status_code, resp.text)
    except Exception as e:
        logging.error("LINE送信エラー: %s", e)

# ============= Walker+ 取得 (既存) =============
def fetch_walkerplus_events(base_url: str, max_pages: int = 2):
    events = []
    for page in range(1, max_pages + 1):
        url = base_url if page == 1 else f"{base_url}{page}.html"
        logging.info("Walker+ 取得: %s", url)
        try:
            res = requests.get(url, timeout=15, headers={"User-Agent":"Mozilla/5.0"})
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
                    logging.debug("JSON-LD parse warning: %s", e)
        except Exception as e:
            logging.error("Walker+ fetch error: %s", e)
    logging.info("Walker+ 合計 %d 件取得 (最大 %d ページ)", len(events), max_pages)
    return events

# ============= Tokyo Art Beat 取得 (一覧→詳細→公式URL抽出) =============
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
EXCLUDE_DOMAINS = {
    "art.nikkei.com", "doubleclick.net", "adservice.google.com",
    "instagram.com", "twitter.com", "facebook.com", "x.com", "youtube.com",
    "lin.ee", "mailchi.mp"
}
sess = requests.Session()
sess.headers.update({"User-Agent": UA})

def uniq_preserve(seq):
    seen = set(); out=[]
    for s in seq:
        if s not in seen:
            seen.add(s); out.append(s)
    return out

def get_soup(url, timeout=20):
    r = sess.get(url, timeout=timeout)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

def extract_official_url_from_soup(soup, venue_hint=None):
    label_patterns = [
        r"展覧会URL", r"展覧会サイト", r"公式サイト", r"公式Ｈ?Ｐ", r"公式ページ",
        r"Official site", r"Official website", r"Website", r"Exhibition URL", r"URL"
    ]
    for pat in label_patterns:
        node = soup.find(string=re.compile(pat))
        if node:
            parent = node.parent
            a = parent.find("a", href=True)
            if a and a["href"].startswith("http"):
                return a["href"].strip()
            # search nearby siblings
            sib = parent.next_sibling
            tries = 0
            while sib and tries < 6:
                if getattr(sib, "find", None):
                    a2 = sib.find("a", href=True)
                    if a2 and a2["href"].startswith("http"):
                        return a2["href"].strip()
                sib = getattr(sib, "next_sibling", None)
                tries += 1

    anchors = soup.find_all("a", href=True)
    candidates = []
    for a in anchors:
        href = a["href"].strip()
        if not href.startswith("http"):
            continue
        netloc = urllib.parse.urlparse(href).netloc.lower()
        if "tokyoartbeat.com" in netloc:
            continue
        bad = any(bad in netloc for bad in EXCLUDE_DOMAINS)
        candidates.append((bad, href))
    # prefer non-bad candidates
    for bad, href in candidates:
        if not bad:
            if venue_hint and venue_hint.lower() in href.lower():
                return href
            return href
    if candidates:
        return candidates[0][1]
    return ""

def extract_venue_from_soup(soup):
    node = soup.find(string=re.compile(r"会場"))
    if node:
        parent = node.parent
        a = parent.find("a")
        if a and a.get_text(strip=True):
            return a.get_text(strip=True)
        txt = parent.get_text(" ", strip=True)
        m = re.search(r"会場[:：\s]*(.+?)(?:住所|〒|時間|$)", txt)
        if m:
            return m.group(1).strip()
    venue_tag = soup.select_one(".venue, .location, a[href*='/venue/'], a[href*='/venues/']")
    if venue_tag:
        return venue_tag.get_text(strip=True)
    return ""

def extract_date_range_from_text(text):
    if not text:
        return "", ""
    p1 = re.search(r"(\d{4})年\D*(\d{1,2})月\D*(\d{1,2})日.*?〜.*?(\d{4})年\D*(\d{1,2})月\D*(\d{1,2})日", text)
    if p1:
        y1, m1, d1, y2, m2, d2 = p1.groups()
        return f"{int(y1):04d}-{int(m1):02d}-{int(d1):02d}", f"{int(y2):04d}-{int(m2):02d}-{int(d2):02d}"
    p2 = re.search(r"(\d{4})年\D*(\d{1,2})月\D*(\d{1,2})日.*?〜.*?(\d{1,2})月\D*(\d{1,2})日", text)
    if p2:
        y1, m1, d1, m2, d2 = p2.groups()
        return f"{int(y1):04d}-{int(m1):02d}-{int(d1):02d}", f"{int(y1):04d}-{int(m2):02d}-{int(d2):02d}"
    p3 = re.search(r"(\d{4})年\D*(\d{1,2})月\D*(\d{1,2})日", text)
    if p3:
        y, m, d = p3.groups()
        s = f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
        return s, s
    # fallback: try ISO-like substrings
    iso = re.search(r"(\d{4}-\d{2}-\d{2})", text)
    if iso:
        return iso.group(1), iso.group(1)
    return "", ""

def fetch_tokyoartbeat_officials(list_url, max_items=20, politeness=(0.5,1.2)):
    logging.info("Fetching TokyoArtBeat list: %s", list_url)
    list_soup = get_soup(list_url)
    anchors = list_soup.find_all("a", href=re.compile(r"^/events/[-/]"))
    paths = [a["href"] for a in anchors if a.get("href")]
    paths = [p for p in paths if "/events/top" not in p and "/events/condId" not in p]
    paths = uniq_preserve(paths)

    results = []
    for rel in paths[:max_items]:
        detail_url = urllib.parse.urljoin("https://www.tokyoartbeat.com", rel)
        try:
            dsoup = get_soup(detail_url)
        except Exception as e:
            logging.warning("detail fetch failed %s : %s", detail_url, e)
            continue

        h1 = dsoup.find("h1")
        name = h1.get_text(strip=True) if h1 else (dsoup.title.string.strip() if dsoup.title else "")
        # schedule extraction (try explicit schedule label or page text)
        schedule_text = ""
        sched_node = dsoup.find(string=re.compile(r"スケジュール|開催期間|会期"))
        if sched_node:
            schedule_text = sched_node.parent.get_text(" ", strip=True)
        else:
            body = dsoup.get_text(" ", strip=True)
            m = re.search(r".{0,160}\d{4}年.*?〜.*?\d{1,4}日.{0,40}", body)
            schedule_text = m.group(0) if m else body[:250]
        start, end = extract_date_range_from_text(schedule_text)

        venue = extract_venue_from_soup(dsoup)
        official = extract_official_url_from_soup(dsoup, venue_hint=venue)
        # fallback: if no official, try venue link if external
        if not official:
            node = dsoup.find(string=re.compile(r"会場"))
            if node:
                p = node.parent
                a = p.find("a", href=True)
                if a and a["href"].startswith("http"):
                    official = a["href"].strip()

        results.append({
            "name": name,
            "start": start,
            "end": end,
            "venue": venue,
            "official_url": official
        })
        time.sleep(random.uniform(*politeness))
    return results

# ============= メッセージ整形 =============
def format_event_message(ev):
    # name, start, end, venue, official_url
    name = ev.get("name", "").strip()
    start = ev.get("start") or (ev.get("startDate")[:10] if ev.get("startDate") else "")
    end = ev.get("end") or (ev.get("endDate")[:10] if ev.get("endDate") else "")
    if start and end:
        date_line = f"{start} ～ {end}"
    elif start:
        date_line = start
    else:
        date_line = ""
    venue = ev.get("venue") or ""
    url = ev.get("official_url") or ev.get("url") or ""
    return f"🎪 {name}\n📅 {date_line}\n📍 {venue}\n🔗 {url}"

# ============= メイン処理 =============
def main():
    logging.info("🚀 START")
    sheet = init_sheet()

    # config: sources
    prefectures = {
        "東京": "https://www.walkerplus.com/event_list/ar0313/",
        "神奈川": "https://www.walkerplus.com/event_list/ar0314/",
        "千葉": "https://www.walkerplus.com/event_list/ar0312/",
        "埼玉": "https://www.walkerplus.com/event_list/ar0311/"
    }

    all_messages = []
    MAX_PER_SOURCE = 10
    MAX_TOTAL_SEND = 15  # overall cap to avoid huge broadcasts

    # 1) Walker+ source
    for pref, url in prefectures.items():
        events = fetch_walkerplus_events(url, max_pages=2)
        messages = [f"📍 {pref} の新着イベント 🎪"]
        new_count = 0
        for ev in events:
            # normalize: use ev.get('url') as key; startDate/endDate exist often
            url_key = ev.get("url")
            # try JSON-LD startDate/endDate keys may be 'startDate' or 'startDate' etc.
            # dedupe by official URL or detail url
            dedupe_key = url_key or ev.get("url") or ""
            if not dedupe_key:
                continue
            if already_sent(dedupe_key, sheet):
                continue
            msg = format_event_message({
                "name": ev.get("name"),
                "startDate": ev.get("startDate"),
                "endDate": ev.get("endDate"),
                "venue": (ev.get("location") or {}).get("name"),
                "official_url": ev.get("url")
            })
            messages.append(msg)
            save_event(sheet, {
                "name": ev.get("name"),
                "start": ev.get("startDate"),
                "end": ev.get("endDate"),
                "venue": (ev.get("location") or {}).get("name"),
                "official_url": ev.get("url"),
                "url": ev.get("url")
            })
            new_count += 1
            if new_count >= MAX_PER_SOURCE:
                break
        if new_count > 0:
            all_messages.append("\n\n".join(messages))

    # 2) Tokyo Art Beat source
    tab_url = "https://www.tokyoartbeat.com/events/condId/most_popular/filter/open"
    tab_events = fetch_tokyoartbeat_officials(tab_url, max_items=25)
    messages = ["📍 TokyoArtBeat の人気展覧会 🎨"]
    new_count = 0
    for ev in tab_events:
        # choose dedupe key: official_url preferred, else name+start
        dedupe_key = ev.get("official_url") or f"{ev.get('name','')}_{ev.get('start','')}"
        if not dedupe_key:
            continue
        if already_sent(dedupe_key, sheet):
            continue
        # If no official_url, still include but ensure unique save key (we save dedupe_key in URL column)
        msg = format_event_message(ev)
        messages.append(msg)
        # save; put official_url in URL col if present else use dedupe_key
        save_event(sheet, {
            "name": ev.get("name"),
            "start": ev.get("start"),
            "end": ev.get("end"),
            "venue": ev.get("venue"),
            "official_url": ev.get("official_url"),
            "url": ev.get("official_url") or dedupe_key
        })
        new_count += 1
        if new_count >= MAX_PER_SOURCE:
            break
        if len(messages) >= MAX_TOTAL_SEND:
            break
    if new_count > 0:
        all_messages.append("\n\n".join(messages))

    # 送信: まとめて1回で broadcast（長くなったら分割）
    if not all_messages:
        logging.info("新着なし。終了します。")
    else:
        # combine but ensure line length not exceed practical limits; split into chunks ~ max 4000 characters per message
        combined = "\n\n================\n\n".join(all_messages)
        CHUNK_SIZE = 3500
        chunks = [combined[i:i+CHUNK_SIZE] for i in range(0, len(combined), CHUNK_SIZE)]
        for c in chunks:
            send_line_broadcast(c)
            time.sleep(1)  # slight pause between sends

    logging.info("🏁 END")

if __name__ == "__main__":
    main()
