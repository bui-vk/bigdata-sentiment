"""
NewsAPI Ingestion Script — DJI Sentiment Pipeline
Big Data Project (verbesserte Version)

Aenderungen gegenueber der Vorversion:
  1. SICHERHEIT: API-Keys kommen NUR noch aus einer lokalen .env-Datei
     (nie hardcoden, nie committen). Siehe .env.example unten im Tutorial.
  2. DJI-30-Liste korrigiert: Intel (INTC) und Dow Inc. (DOW) wurden im
     echten Dow Jones im Nov. 2024 durch Nvidia (NVDA) und Sherwin-Williams
     (SHW) ersetzt. Vorher waren beide alten UND ein Teil der neuen Werte
     in der Liste -> jetzt korrekt 30 Werte.
  3. DEDUPLIZIERUNG: Artikel werden pro Tag ueber einen Hash aus
     (url, falls vorhanden, sonst title+source+publishedAt) entduplikiert.
     NewsAPI liefert keine stabile article_id, daher Hash statt ID.
  4. MANIFEST: Nach jedem Tageslauf wird eine manifest_<date>.json
     geschrieben mit Artikelanzahl pro Ticker/Macro-Topic. Ticker mit
     0 Treffern werden explizit als Warnung ausgegeben (Coverage-Check,
     siehe "Tab 7"-Findings im PRD).
  5. RETRY/BACKOFF bei Rate-Limit (429) und Netzwerkfehlern.
  6. HDFS-Upload optional ueber die "hdfs" Python-Bibliothek (WebHDFS),
     statt ueber das hdfs-CLI per subprocess — funktioniert auch, wenn auf
     dem Host kein Hadoop-Client installiert ist.
  7. Pfade sind jetzt relativ/konfigurierbar statt eines hartcodierten
     lokalen macOS-Pfads.

Output JSON fields (unveraendert, Schema bleibt kompatibel mit PRD Abschnitt 6):
  date, ticker, company, article_type, publishedAt,
  source, title, description, content, url

article_type ist "company" oder "macro".
In Spark: macro-Artikel als gemeinsamen Kontext fuer alle Companies behandeln,
nicht einer einzelnen Firma zuordnen.
"""

import requests
import json
import os
import time
import hashlib
import logging
from datetime import datetime, timedelta

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv ist in ingestion/requirements.txt enthalten

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("ingestion")

# ── API Keys ──────────────────────────────────────────────────────────────────
# Erwartet eine .env Datei (siehe Tutorial) mit:
#   NEWSAPI_KEYS=key1,key2,key3
# NIE Keys hier im Code hardcoden.
_raw_keys = os.getenv("NEWSAPI_KEYS", "")
API_KEYS = [k.strip() for k in _raw_keys.split(",") if k.strip()]

if not API_KEYS:
    raise RuntimeError(
        "Kein API-Key gefunden. Lege eine .env Datei mit NEWSAPI_KEYS=dein_key an "
        "(siehe .env.example im Tutorial). Nie Keys direkt im Code eintragen."
    )

# ── DJI 30 Companies (korrigiert: NVDA statt INTC, DOW entfernt, SHW bleibt) ──
DJI_COMPANIES = [
    ("Apple OR AAPL",                   "Apple",            "AAPL"),
    ("Microsoft OR MSFT",               "Microsoft",        "MSFT"),
    ("Goldman Sachs OR GS stock",       "Goldman Sachs",    "GS"),
    ("JPMorgan OR JPM stock",           "JPMorgan",         "JPM"),
    ("Visa stock OR Visa Inc",          "Visa",             "V"),
    ("UnitedHealth OR UNH stock",       "UnitedHealth",     "UNH"),
    ("Home Depot OR HD stock",          "Home Depot",       "HD"),
    ("Salesforce OR CRM stock",         "Salesforce",       "CRM"),
    ("McDonald's OR MCD stock",         "McDonald's",       "MCD"),
    ("Caterpillar OR CAT stock",        "Caterpillar",      "CAT"),
    ("Amgen OR AMGN stock",             "Amgen",            "AMGN"),
    ("Boeing OR BA stock",              "Boeing",           "BA"),
    ("Honeywell OR HON stock",          "Honeywell",        "HON"),
    ("3M OR MMM stock",                 "3M",               "MMM"),
    ("Travelers OR TRV stock",          "Travelers",        "TRV"),
    ("Walmart OR WMT stock",            "Walmart",          "WMT"),
    ("Procter Gamble OR PG stock",      "Procter & Gamble", "PG"),
    ("Johnson Johnson OR JNJ stock",    "Johnson & Johnson","JNJ"),
    ("Chevron OR CVX stock",            "Chevron",          "CVX"),
    ("American Express OR AXP stock",   "American Express", "AXP"),
    ("Nike OR NKE stock",               "Nike",             "NKE"),
    ("Cisco OR CSCO stock",             "Cisco",            "CSCO"),
    ("Walt Disney OR DIS stock",        "Walt Disney",      "DIS"),
    ("Merck OR MRK stock",              "Merck",            "MRK"),
    ("Coca Cola OR KO stock",           "Coca-Cola",        "KO"),
    ("Verizon OR VZ stock",             "Verizon",          "VZ"),
    ("Nvidia OR NVDA stock",            "Nvidia",           "NVDA"),
    ("Sherwin Williams OR SHW stock",   "Sherwin-Williams", "SHW"),
    ("IBM stock OR International Business Machines", "IBM", "IBM"),
    ("Amazon OR AMZN stock",            "Amazon",           "AMZN"),
]
assert len(DJI_COMPANIES) == 30, f"Erwartet 30 DJI-Werte, habe {len(DJI_COMPANIES)}"

# ── Macro Queries ─────────────────────────────────────────────────────────────
MACRO_QUERIES = [
    ("Federal Reserve OR Fed interest rates OR FOMC",   "Fed / Interest Rates"),
    ("CPI inflation OR consumer price index",           "CPI / Inflation"),
    ("US recession OR GDP growth OR economic outlook",  "Recession / GDP"),
    ("oil prices OR crude oil OR energy prices",        "Oil / Energy"),
    ("US dollar DXY OR dollar index",                   "DXY / Dollar"),
    ("stock market crash OR market selloff OR S&P 500", "Market Sentiment"),
    ("US China trade war OR tariffs OR sanctions",      "Trade / Tariffs"),
    ("unemployment rate OR jobs report OR NFP",         "Jobs / Employment"),
]

# ── Config (per ENV ueberschreibbar, sinnvolle Defaults) ──────────────────────
OUTPUT_DIR = os.getenv("RAW_NEWS_DIR", os.path.join(os.path.dirname(__file__), "data", "raw_news_json"))
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "1"))   # auf >1 setzen fuer Backfill (Free Tier: max 30 Tage zurueck)
PAGE_SIZE = 100
DELAY_BETWEEN_REQUESTS = 1.2
MAX_RETRIES = 3

HDFS_UPLOAD_ENABLED = os.getenv("HDFS_UPLOAD_ENABLED", "false").lower() == "true"
HDFS_WEBHDFS_URL = os.getenv("HDFS_WEBHDFS_URL", "http://localhost:9870")
HDFS_RAW_PATH = "/data/raw/news"

# ── Key rotation ──────────────────────────────────────────────────────────────
_key_index = 0

def get_next_key():
    global _key_index
    key = API_KEYS[_key_index % len(API_KEYS)]
    _key_index += 1
    return key

# ── Noise filter ──────────────────────────────────────────────────────────────
def is_relevant(article, company_name, ticker):
    """True nur wenn Firmenname oder Ticker in Title ODER Description vorkommt."""
    targets = [company_name.lower(), ticker.lower()]
    title = (article.get("title") or "").lower()
    description = (article.get("description") or "").lower()
    return any(t in title or t in description for t in targets)

# ── Dedup-Key ──────────────────────────────────────────────────────────────────
def dedup_key(article):
    """
    NewsAPI hat keine stabile article_id -> Hash aus URL (falls vorhanden),
    sonst Hash aus title+source+publishedAt als Fallback.
    """
    url = article.get("url")
    if url:
        basis = url.strip().lower()
    else:
        basis = (
            (article.get("title") or "") +
            (article.get("source", {}).get("name") or "") +
            (article.get("publishedAt") or "")
        ).lower()
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()

# ── HTTP mit Retry/Backoff ─────────────────────────────────────────────────────
def _request_with_retry(url, params, label):
    for attempt in range(1, MAX_RETRIES + 1):
        headers = {"X-Api-Key": get_next_key()}
        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
        except requests.RequestException as e:
            log.warning(f"  [ERROR] Request failed ({label}), Versuch {attempt}/{MAX_RETRIES}: {e}")
            time.sleep(2 ** attempt)
            continue

        if response.status_code == 429:
            log.warning(f"  [RATE LIMIT] {label}, Versuch {attempt}/{MAX_RETRIES} — warte und rotiere Key")
            time.sleep(2 ** attempt)
            continue

        try:
            data = response.json()
        except ValueError:
            log.warning(f"  [ERROR] Keine gueltige JSON-Antwort fuer {label}")
            return None

        if data.get("status") != "ok":
            log.warning(f"  [API ERROR] {label}: {data.get('message', 'unknown error')}")
            return None

        return data

    log.error(f"  [FAILED] {label}: alle {MAX_RETRIES} Versuche fehlgeschlagen")
    return None

def fetch_articles(query, from_date, to_date, label):
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": query, "language": "en", "sortBy": "publishedAt",
        "from": from_date, "to": to_date, "pageSize": PAGE_SIZE, "page": 1,
    }
    data = _request_with_retry(url, params, label)
    return data.get("articles", []) if data else []

# ── Optionaler HDFS-Upload via WebHDFS ─────────────────────────────────────────
def upload_to_hdfs(local_path, date_str):
    if not HDFS_UPLOAD_ENABLED:
        return
    try:
        from hdfs import InsecureClient
        client = InsecureClient(HDFS_WEBHDFS_URL, user="root")
        hdfs_dir = f"{HDFS_RAW_PATH}/{date_str}"
        hdfs_dest = f"{hdfs_dir}/news_{date_str}.json"
        client.makedirs(hdfs_dir)
        client.upload(hdfs_dest, local_path, overwrite=True)
        log.info(f"  Nach HDFS hochgeladen: {hdfs_dest}")
    except Exception as e:
        log.warning(f"  [HDFS ERROR] Upload fehlgeschlagen: {e}")
        log.warning("  Datei liegt lokal weiter vor — manuell nachladen, sobald HDFS laeuft.")

# ── Main ──────────────────────────────────────────────────────────────────────
def run_ingestion():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for day_offset in range(LOOKBACK_DAYS):
        day = datetime.utcnow() - timedelta(days=day_offset + 1)
        date_str = day.strftime("%Y-%m-%d")
        from_date = date_str
        to_date = (day + timedelta(days=1)).strftime("%Y-%m-%d")

        log.info(f"\n=== Fetching for date: {date_str} ===")

        day_results = []
        seen_hashes = set()
        manifest = {"date": date_str, "companies": {}, "macro": {}, "zero_coverage": []}

        for query, company_name, ticker in DJI_COMPANIES:
            raw_articles = fetch_articles(query, from_date, to_date, ticker)
            filtered = [a for a in raw_articles if is_relevant(a, company_name, ticker)]

            kept = 0
            for article in filtered:
                h = dedup_key(article)
                if h in seen_hashes:
                    continue
                seen_hashes.add(h)
                kept += 1
                day_results.append({
                    "date": date_str, "ticker": ticker, "company": company_name,
                    "article_type": "company",
                    "publishedAt": article.get("publishedAt"),
                    "source": article.get("source", {}).get("name"),
                    "title": article.get("title"),
                    "description": article.get("description"),
                    "content": article.get("content"),
                    "url": article.get("url"),
                })

            manifest["companies"][ticker] = kept
            if kept == 0:
                manifest["zero_coverage"].append(ticker)

            log.info(f"  {ticker}: {len(raw_articles)} fetched -> {len(filtered)} relevant -> {kept} nach Dedup")
            time.sleep(DELAY_BETWEEN_REQUESTS)

        log.info(f"\n--- Macro signals fuer {date_str} ---")
        for query, label in MACRO_QUERIES:
            raw_articles = fetch_articles(query, from_date, to_date, label)
            kept = 0
            for article in raw_articles:
                h = dedup_key(article)
                if h in seen_hashes:
                    continue
                seen_hashes.add(h)
                kept += 1
                day_results.append({
                    "date": date_str, "ticker": "MACRO", "company": label,
                    "article_type": "macro",
                    "publishedAt": article.get("publishedAt"),
                    "source": article.get("source", {}).get("name"),
                    "title": article.get("title"),
                    "description": article.get("description"),
                    "content": article.get("content"),
                    "url": article.get("url"),
                })
            manifest["macro"][label] = kept
            log.info(f"  MACRO [{label}]: {len(raw_articles)} fetched -> {kept} nach Dedup")
            time.sleep(DELAY_BETWEEN_REQUESTS)

        # ── Speichern: Rohdaten + Manifest ────────────────────────────────────
        output_path = os.path.join(OUTPUT_DIR, f"news_{date_str}.json")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(day_results, f, ensure_ascii=False, indent=2)

        manifest_path = os.path.join(OUTPUT_DIR, f"manifest_{date_str}.json")
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)

        log.info(f"Gespeichert: {output_path} ({len(day_results)} Artikel nach Dedup)")
        if manifest["zero_coverage"]:
            log.warning(f"  [COVERAGE WARNING] 0 Artikel fuer: {', '.join(manifest['zero_coverage'])}")

        upload_to_hdfs(output_path, date_str)
        upload_to_hdfs(manifest_path, date_str)

    log.info("\nIngestion complete.")

if __name__ == "__main__":
    run_ingestion()
