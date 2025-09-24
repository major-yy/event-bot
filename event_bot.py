# -*- coding: utf-8 -*-
"""
event_bot.py
Walker+ と Tokyo Art Beat を統合して取得し、
Google Sheets に送信済みを保存、LINE公式アカウントに配信するスクリプト。

Install:
  pip install requests beautifulsoup4 gspread google-auth
Env (GitHub Secrets):
  - GOOGLE_CREDENTIALS  (service account json string)
  - SPREADSHEET_ID
  - LINE_ACCESS_TOKEN
"""
import os
import re
import json
import time
import random
import logging
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
        raise RuntimeError("SPREADSHEET_ID not set")
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

# ============= 履歴判定・保存 =============
def already_sent(event_url, sheet):
    if not event_url:
        return False
    try:
        records = sheet.col_values(4)  # URL 列
        return event_url in records
    except Exception as e:
        logging.warning("Could not read sheet column: %s", e)
        return False

def save_event(sheet, event):
    """
    event: dict with keys possibly 'name','start','end','official_url','url','venue'
    Saves a row: [送信日, イベント名, 開催期間, URL, 会場]
    """
    today = datetime.now().strftime("%Y-%m-%d")
    name = event.get("name", "")
    # normalize period
    start = event.get("start") or event.get("startDate") or ""
    end = event.get("end") or event.get("endDate") or ""
    def norm(d):
        if not d: return ""
        if isinstance(d, str) and re.match(r"\d{4}-\d{2}-\d{2}", d):
            return d[:10]
        return d
    if start and end:
        period = f"{norm(start)} ～ {norm(end)}"
    elif start:
        period = norm(start)
    else:
        period = ""
    url = event.get("official_url") or event.get("url") or ""
    venue = event.get("venue") or ""
    try:
        sheet.append_row([today, name, period, url, venue])
    except Exception as e:
        logging.warning("Failed to append row: %s", e)

# ============= LINE送信 =============
LINE_ACCESS_TOKEN = os.getenv("LINE_ACCESS_TOKEN")
HEADERS = {
    "Content-Type": "application/json; charset=UTF-8",
    "Authorization": f"Bearer {LINE_ACCESS_TOKEN}" if LINE_ACCESS_TOKEN else ""
}

def send_line_broadcast(message: str):
    if not LINE_ACCESS_TOKEN:
        logging.warning("LINE_ACCESS_TOKEN not set: skipping broadcast")
        return
    payload = {"messages": [{"type": "text", "text": message}]}
    try:
        resp = requests.post("https://api.line.me/v2/bot/message/broadcast",
                             headers=HEADERS, data=json.dumps(payload).encode("utf-8"), timeout=15)
        logging.info("LINE send: %s %s", resp.status_code, resp.text)
    except Exception as e:
        logging.error("LINE send error: %s", e)

# ============= Walker+ 取得 (JSON-LD) =============
def fetch_walkerplus_events(base_url: str, max_pages: int = 2):
    events = []
    headers = {"User-Agent": "Mozilla/5.0"}
    for page in range(1, max_pages + 1):
        url = base_url if page == 1 else f"{base_url}{page}.html"
        logging.info("Walker+ fetch: %s", url)
        try:
            res = requests.get(url, timeout=15, headers=headers)
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
                except Exception:
                    continue
        except Exception as e:
            logging.warning("Walker+ page fetch failed: %s", e)
    logging.info("Walker+ total events found: %d", len(events))
    return events

# ============= TokyoArtBeat 取得 (requests + BeautifulSoup 二段階) =============
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
EXCLUDE_DOMAINS = {
    "art.nikkei.com", "doubleclick.net", "adservice.google.com",
    "instagram.com", "twitter.com", "facebook.com", "x.com", "youtube.com",
    "lin.ee", "mailchi.mp"
}
sess = requests.Session()
sess.headers.update({"User-Agent": UA})

def get_soup(url, timeout=20):
    r = sess.get(url, timeout=timeout)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

def uniq_preserve(seq):
    seen = set(); out=[]
    for s in seq:
        if s not in seen:
            seen.add(s); out.append(s)
    return out

def extract_official_url_from_soup(soup, venue_hint=None):
    # 優先: "展覧会URL" などラベルから探す
    label_patterns = [r"展覧会URL", r"展覧会サイト", r"公式サイト", r"公式Ｈ?Ｐ", r"公式ページ", r"Official site", r"Official website", r"Website"]
    for pat in label_patterns:
        node = soup.find(string=re.compile(pat))
        if node:
            parent = node.parent
            a = parent.find("a", href=True)
            if a and a["href"].startswith("http"):
                return a["href"].strip()
            # 近傍もチェック
            sib = parent.next_sibling; tries=0
            while sib and tries < 6:
                if getattr(sib, "find", None):
                    a2 = sib.find("a", href=True)
                    if a2 and a2["href"].startswith("http"):
                        return a2["href"].strip()
                sib = getattr(sib, "next_sibling", None); tries += 1
    # フォールバック: 外部リンクのうち除外ドメインを避けて最初のもの
    anchors = soup.find_all("a", href=True)
    candidates = []
    for a in anchors:
        href = a["href"].strip()
        if not href.startswith("http"): continue
        netloc = urllib.parse.urlparse(href).netloc.lower()
        if "tokyoartbeat.com" in netloc: continue
        bad = any(bad in netloc for bad in EXCLUDE_DOMAINS)
        candidates.append((bad, href))
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
        if a and a.get_text(strip=True): return a.get_text(strip=True)
        txt = parent.get_text(" ", strip=True)
        m = re.search(r"会場[:：\s]*(.+?)(?:住所|〒|時間|$)", txt)
        if m: return m.group(1).strip()
    venue_tag = soup.select_one(".venue, .location, a[href*='/venue/'], a[href*='/venues/']")
    if venue_tag: return venue_tag.get_text(strip=True)
    return ""

def extract_date_range_from_text(text):
    if not text: return "", ""
    p1 = re.search(r"(\d{4})年\D*(\d{1,2})月\D*(\d{1,2})日.*?〜.*?(\d{4})年\D*(\d{1,2})月\D*(\d{1,2})日", text)
    if p1:
        y1,m1,d1,y2,m2,d2 = p1.groups()
        return f"{int(y1):04d}-{int(m1):02d}-{int(d1):02d}", f"{int(y2):04d}-{int(m2):02d}-{int(d2):02d}"
    p2 = re.search(r"(\d{4})年\D*(\d{1,2})月\D*(\d{1,2})日.*?〜.*?(\d{1,2})月\D*(\d{1,2})日", text)
    if p2:
        y1,m1,d1,m2,d2 = p2.groups()
        return f"{int(y1):04d}-{int(m1):02d}-{int(d1):02d}", f"{int(y1):04d}-{int(m2):02d}-{int(d2):02d}"
    p3 = re.search(r"(\d{4})年\D*(\d{1,2})月\D*(\d{1,2})日", text)
    if p3:
        y,m,d = p3.groups(); s = f"{int(y):04d}-{int(m):02d}-{int(d):02d}"; return s, s
    iso = re.search(r"(\d{4}-\d{2}-\d{2})", text)
    if iso: return iso.group(1), iso.group(1)
    return "", ""

def fetch_tokyoartbeat_officials(list_url, max_items=10, politeness=(0.5,1.2)):
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
            logging.warning("Failed fetch detail %s : %s", detail_url, e)
            continue

        h1 = dsoup.find("h1")
        name = h1.get_text(strip=True) if h1 else (dsoup.title.string.strip() if dsoup.title else "")

        # schedule extraction
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
        if not official:
            # fallback: look for external link near venue
            node = dsoup.find(string=re.compile(r"会場"))
            if node:
                p = node.parent; a = p.find("a", href=True)
                if a and a["href"].startswith("http"): official = a["href"].strip()

        results.append({
            "name": name,
            "start": start,
            "end": end,
            "venue": venue,
            "official_url": official
        })
        time.sleep(random.uniform(*politeness))
    logging.info("TokyoArtBeat events fetched: %d", len(results))
    return results

# ============= 会場 -> 都県 マッピング（ベストエフォート） =============
PREF_KEYWORD_MAP = {
    "東京": ["東京都","東京","上野","六本木","銀座","新宿","浅草","渋谷","池袋","お台場","品川","丸の内","日比谷","有楽町","国立新美術館","東京都美術館","国立西洋美術館","森美術館","六本木ミュージアム","MMM","メゾン・デ・ミュゼ","スカイツリー"],
    "神奈川": ["横浜","川崎","鎌倉","藤沢","相模原","箱根","新横浜","みなとみらい","横浜美術館","横浜市","横浜港"],
    "千葉": ["幕張","千葉","成田","船橋","市川","浦安","幕張メッセ","幕張新都心","房総","海浜幕張"],
    "埼玉": ["埼玉","大宮","所沢","川越","越谷","秩父","熊谷","狭山","入間","飯能","所沢","さいたま"]
}

def map_venue_to_prefecture(venue: str, official_url: str = ""):
    if venue:
        v = venue.lower()
        for pref, keys in PREF_KEYWORD_MAP.items():
            for k in keys:
                if k.lower() in v:
                    return pref
    if official_url:
        u = official_url.lower()
        if any(x in u for x in ["yokohama","kanagawa","kawasaki","sagamihara","yokosuka"]):
            return "神奈川"
        if any(x in u for x in ["makuhari","chiba","narita","funabashi","urayasu","sakura"]):
            return "千葉"
        if any(x in u for x in ["saitama","omiya","kawagoe","tokorozawa","hanno","hanno"]):
            return "埼玉"
        if any(x in u for x in ["tokyo","roppongi","ginza","asakusa","ueno","shibuya","shinjuku","tocho"]):
            return "東京"
    # try explicit kanji
    if "神奈川" in (venue or ""): return "神奈川"
    if "千葉" in (venue or ""): return "千葉"
    if "埼玉" in (venue or ""): return "埼玉"
    if "東京" in (venue or "") or "東京都" in (venue or ""): return "東京"
    return None

# ============= メッセージ整形 =============
def format_event_message(ev):
    name = ev.get("name","").strip()
    start = ev.get("start","") or (ev.get("startDate","")[:10] if ev.get("startDate") else "")
    end = ev.get("end","") or (ev.get("endDate","")[:10] if ev.get("endDate") else "")
    date_line = f"{start} ～ {end}" if start and end else (start or "")
    venue = ev.get("venue","") or (ev.get("location") or {}).get("name","")
    url = ev.get("official_url") or ev.get("url") or ""
    return f"🎪 {name}\n📅 {date_line}\n📍 {venue}\n🔗 {url}"

# ============= メイン処理 =============
def main():
    logging.info("🚀 START")
    sheet = init_sheet()

    # Walker+ 各都県の一覧 (従来通り)
    prefectures = {
        "東京": "https://www.walkerplus.com/event_list/ar0313/",
        "神奈川": "https://www.walkerplus.com/event_list/ar0314/",
        "千葉": "https://www.walkerplus.com/event_list/ar0312/",
        "埼玉": "https://www.walkerplus.com/event_list/ar0311/"
    }

    # prepare buckets
    messages_map = {pref: [f"📍 {pref} の新着イベント 🎪"] for pref in prefectures.keys()}

    # WALKER+ : per-pref up to 10 new events
    PER_PREF_LIMIT = 10
    for pref, url in prefectures.items():
        events = fetch_walkerplus_events(url, max_pages=2)
        new_count = 0
        for ev in events:
            url_key = ev.get("url") or ""
            if not url_key: 
                continue
            if already_sent(url_key, sheet):
                continue
            msg = format_event_message({
                "name": ev.get("name"),
                "startDate": ev.get("startDate"),
                "endDate": ev.get("endDate"),
                "location": ev.get("location"),
                "official_url": ev.get("url")
            })
            messages_map[pref].append(msg)
            # save using canonical URL as key
            save_event(sheet, {"name": ev.get("name"), "startDate": ev.get("startDate"), "endDate": ev.get("endDate"), "url": url_key, "venue": (ev.get("location") or {}).get("name","")})
            new_count += 1
            if new_count >= PER_PREF_LIMIT:
                break

    # TOKYO ART BEAT : fetch up to 10 events total, map to prefectures and append
    TAB_LIST_URL = "https://www.tokyoartbeat.com/events/condId/most_popular/filter/open"
    tab_events = fetch_tokyoartbeat_officials(TAB_LIST_URL, max_items=10)
    tab_new_count = 0
    for ev in tab_events:
        # decide prefecture
        pref = map_venue_to_prefecture(ev.get("venue",""), ev.get("official_url",""))
        if not pref:
            pref = "東京"  # fallback to Tokyo
        # dedupe key: prefer official_url, else name+start
        dedupe_key = ev.get("official_url") or (ev.get("name","") + "_" + (ev.get("start","") or ""))
        if not dedupe_key:
            continue
        if already_sent(dedupe_key, sheet):
            continue
        messages_map.setdefault(pref, [f"📍 {pref} の新着イベント 🎪"])
        messages_map[pref].append(format_event_message(ev))
        save_event(sheet, {"name": ev.get("name"), "start": ev.get("start"), "end": ev.get("end"), "official_url": ev.get("official_url"), "url": dedupe_key, "venue": ev.get("venue")})
        tab_new_count += 1
        if tab_new_count >= 10:
            break

    # 送信: 各都県のバケットを順に送信
    order = ["東京","神奈川","千葉","埼玉"]
    CHUNK_SIZE = 3500
    any_sent = False
    for pref in order:
        bucket = messages_map.get(pref, [])
        if len(bucket) <= 1:
            continue
        # combine bucket into single text message (split if too large)
        combined = "\n\n".join(bucket)
        # split into reasonable chunks for LINE
        parts = [combined[i:i+CHUNK_SIZE] for i in range(0, len(combined), CHUNK_SIZE)]
        for p in parts:
            send_line_broadcast(p)
            time.sleep(1)
        any_sent = True

    if not any_sent:
        logging.info("No new events to send.")
    logging.info("🏁 END")

if __name__ == "__main__":
    main()
