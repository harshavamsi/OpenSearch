"""Fast in-process clickbench_plus generator + bulk ingester.

Skips OSB entirely. Each worker process:
  1. Generates N docs using precomputed pools (no Mimesis, no disk).
  2. Serializes a bulk body with orjson.
  3. POSTs /_bulk to the cluster over a persistent HTTPS pool.
  4. Loops until a shared atomic counter hits the target doc count.

Why this layout: OSB's Dask-based generator serializes dict chunks back to the
driver, then writes NDJSON to disk, then OSB's ingest pipeline reads it back.
On a cross-region link each round-trip is ~150 ms, so the only way to hit
hundreds of MB/s is many concurrent keep-alive clients generating + posting in
the same process.
"""
import argparse
import multiprocessing as mp
import os
import random
import signal
import ssl
import string
import sys
import time
import uuid

import orjson
import urllib3


# ---------- Precomputed pools (loaded once per worker) ----------

BROWSER_COUNTRY = [
    "US", "RU", "DE", "FR", "GB", "CN", "JP", "BR", "IN", "CA",
    "AU", "IT", "ES", "NL", "PL", "TR", "MX", "KR", "ID", "AR",
    "SE", "CH", "BE", "NO", "AT", "DK", "FI", "IE", "PT", "GR",
    "CZ", "HU", "RO", "UA", "BY", "KZ", "TH", "VN", "PH", "MY",
    "SG", "HK", "TW", "IL", "SA", "AE", "EG", "ZA", "NG", "KE",
    "CL", "CO", "PE", "VE", "NZ", "IS", "LU", "MT", "CY", "EE",
]
BROWSER_LANGUAGE = [
    "en", "ru", "de", "fr", "es", "zh", "ja", "pt", "it", "nl",
    "pl", "tr", "ko", "ar", "th", "vi", "id", "uk", "sv", "cs",
    "el", "he", "hu", "ro", "da", "fi", "no", "hr", "sk", "bg",
    "sr", "lt", "lv", "et", "sl", "ca", "eu", "gl", "is", "ga",
]
HIT_COLORS = [
    "#000000", "#FFFFFF", "#FF0000", "#00FF00", "#0000FF",
    "#FFFF00", "#FF00FF", "#00FFFF", "#808080", "#C0C0C0",
    "#800000", "#808000", "#008000", "#800080", "#008080",
    "#000080", "#A52A2A", "#FFA500", "#FFC0CB", "#FFD700",
]
UTM_SOURCES = [
    "google", "yandex", "bing", "facebook", "twitter", "instagram",
    "direct", "email", "newsletter", "affiliate", "referral",
    "vk", "telegram", "whatsapp", "reddit", "linkedin", "pinterest",
    "youtube", "tiktok", "snapchat", "duckduckgo", "yahoo", "baidu",
    "naver", "daum", "qwant", "ecosia", "startpage", "mail",
]
UTM_MEDIUMS = ["cpc", "organic", "email", "social", "direct", "referral", "banner", "display"]
PAGE_CHARSETS = ["utf-8", "windows-1251", "iso-8859-1", "windows-1252", "shift_jis", "gb2312"]
PARAM_CURRENCIES = ["USD", "EUR", "RUB", "GBP", "JPY", "CNY", "INR", "BRL", "MXN", "KRW", "TRY", "CAD", "AUD"]
OPENSTAT_SERVICES = ["direct", "mail", "social", "banner", "search", "display", "native"]
MOBILE_MODELS = [
    "", "", "", "", "", "",
    "iPhone 14", "iPhone 13", "iPhone 12", "iPhone 15",
    "Pixel 7", "Pixel 8", "Pixel 6",
    "Galaxy S23", "Galaxy S22", "Galaxy S24", "Galaxy A54",
    "OnePlus 11", "Xiaomi 13", "Redmi Note 12",
]
HTTP_ERRORS = [0, 0, 0, 0, 0, 0, 0, 0, 200, 200, 200, 301, 302, 404, 500, 403, 503]
RESOLUTION_WIDTHS = [1920, 1366, 1440, 1536, 1280, 2560, 3840, 1680, 1600, 1024]
RESOLUTION_HEIGHTS = [1080, 768, 900, 864, 720, 1440, 2160, 1050, 900, 768]

# ~5000 domains and ~500 TLDs give ~2.5M unique URL hosts.
DOMAIN_POOL = [
    "".join(random.choices(string.ascii_lowercase, k=random.randint(5, 12)))
    for _ in range(5000)
]
TLD_POOL = [
    "com", "org", "net", "io", "co", "ai", "app", "dev", "ru", "de", "fr", "jp",
    "uk", "cn", "br", "in", "au", "ca", "nl", "es", "it", "pl", "tr", "se",
    "ch", "be", "no", "at", "dk", "fi", "ie", "pt", "gr", "cz", "hu", "ro",
    "ua", "by", "kz", "th", "vn", "ph", "my", "sg", "hk", "tw", "il", "sa",
    "ae", "eg", "za", "ng", "ke", "cl", "co", "pe", "nz", "is", "lu", "mt",
]
# Pre-render 50K URL strings. Docs reference these indexes — cheap and still
# ~50K distinct URLs per worker, ~32 workers ≈ 1.6M unique URLs overall.
URL_POOL = [
    f"https://{random.choice(DOMAIN_POOL)}.{random.choice(TLD_POOL)}/"
    for _ in range(50000)
]

# Realistic-ish title words — pick a couple thousand real English-ish strings.
WORD_POOL = [
    "the", "of", "and", "to", "in", "for", "with", "on", "at", "by",
    "this", "that", "from", "as", "an", "it", "is", "be", "was", "are",
    "have", "has", "had", "been", "being", "not", "but", "or", "if",
    "about", "after", "against", "all", "also", "among", "any", "because",
    "before", "between", "both", "during", "each", "either", "every",
    "few", "first", "following", "here", "how", "into", "just", "know",
    "like", "make", "many", "may", "might", "more", "most", "much",
    "new", "now", "only", "other", "our", "out", "over", "same", "should",
    "some", "such", "than", "their", "there", "these", "they", "those",
    "time", "two", "use", "very", "want", "way", "well", "what", "when",
    "where", "which", "while", "who", "why", "will", "with", "would",
    "year", "data", "analytics", "dashboard", "query", "latency", "metric",
    "benchmark", "performance", "cluster", "index", "search", "terms",
    "aggregation", "cardinality", "nested", "multi", "streaming", "classic",
    "memory", "heap", "shard", "segment", "bucket", "document", "result",
    "error", "success", "failure", "retry", "timeout", "connection", "payload",
    "request", "response", "header", "body", "field", "value", "type", "string",
    "integer", "long", "short", "date", "boolean", "float", "double", "keyword",
]

# ---------- Cached Zipf distributions ----------

def _zipf_cdf(n, s):
    weights = [1.0 / ((i + 1) ** s) for i in range(n)]
    total = sum(weights)
    cdf = []
    acc = 0.0
    for w in weights:
        acc += w / total
        cdf.append(acc)
    return cdf


COUNTER_ZIPF_N = 1000
COUNTER_ZIPF = _zipf_cdf(COUNTER_ZIPF_N, 1.4)


def _zipf_sample_counter():
    r = random.random()
    lo, hi = 0, COUNTER_ZIPF_N - 1
    while lo < hi:
        mid = (lo + hi) // 2
        if COUNTER_ZIPF[mid] < r:
            lo = mid + 1
        else:
            hi = mid
    return lo


def _zipf_sample_user():
    # Inverse-transform approximation for large-n Zipf(s<1).
    return int(10_000_000 ** random.random()) - 1


# ---------- Per-doc generator ----------

def generate_doc(_rnd=random):
    randint = _rnd.randint
    choice = _rnd.choice
    getrandbits = _rnd.getrandbits
    rand = _rnd.random

    user_id = _zipf_sample_user()
    watch_id = getrandbits(63)
    counter_id = _zipf_sample_counter()

    url = URL_POOL[randint(0, 49999)]
    referer = URL_POOL[randint(0, 49999)]
    title = " ".join(choice(WORD_POOL) for _ in range(randint(5, 12)))
    search_phrase = " ".join(
        choice(WORD_POOL) for _ in range(randint(1, 5))
    )

    # Correlated timestamp — 2023-01-01 to 2025-01-01.
    ts_epoch = randint(1672531200, 1735689600)
    t = time.gmtime(ts_epoch)
    base_ts = f"{t.tm_year:04d}-{t.tm_mon:02d}-{t.tm_mday:02d} {t.tm_hour:02d}:{t.tm_min:02d}:{t.tm_sec:02d}"

    return {
        "URL": url,
        "Referer": referer,
        "OriginalURL": url,
        "Title": title,
        "SearchPhrase": search_phrase,
        "MobilePhoneModel": choice(MOBILE_MODELS),
        "Params": f"a={randint(0, 10000)}&b={randint(0, 10000)}",
        "UserAgentMinor": f"{randint(0, 200)}.{randint(0, 9)}",
        "SocialSourcePage": URL_POOL[randint(0, 49999)],

        "BrowserCountry": choice(BROWSER_COUNTRY),
        "BrowserLanguage": choice(BROWSER_LANGUAGE),
        "HitColor": choice(HIT_COLORS),
        "UTMSource": choice(UTM_SOURCES),
        "UTMMedium": choice(UTM_MEDIUMS),
        "UTMCampaign": f"camp_{randint(0, 500)}",
        "UTMContent": f"content_{randint(0, 2000)}",
        "UTMTerm": f"term_{randint(0, 3000)}",
        "FromTag": f"tag_{randint(0, 500)}",
        "PageCharset": choice(PAGE_CHARSETS),
        "ParamCurrency": choice(PARAM_CURRENCIES),
        "ParamOrderID": f"order_{uuid.uuid4().hex[:16]}",
        "OpenstatServiceName": choice(OPENSTAT_SERVICES),
        "OpenstatCampaignID": f"ocamp_{randint(0, 500)}",
        "OpenstatAdID": f"oad_{randint(0, 2000)}",
        "OpenstatSourceID": f"osrc_{randint(0, 200)}",

        "UserID": user_id,
        "WatchID": watch_id,
        "FUniqID": getrandbits(63),
        "RefererHash": getrandbits(63),
        "URLHash": hash(url) & ((1 << 63) - 1),
        "ParamPrice": randint(0, 1_000_000),

        "CounterID": counter_id,
        "ClientIP": getrandbits(31),
        "RemoteIP": getrandbits(31),
        "IPNetworkID": randint(0, 200_000),
        "HID": getrandbits(31),
        "RegionID": randint(0, 10_000),
        "URLRegionID": randint(0, 10_000),
        "RefererRegionID": randint(0, 10_000),
        "OpenerName": randint(0, 100),
        "ConnectTiming": randint(0, 5_000),
        "DNSTiming": randint(0, 1_000),
        "FetchTiming": randint(0, 10_000),
        "ResponseStartTiming": randint(0, 10_000),
        "ResponseEndTiming": randint(0, 15_000),
        "SendTiming": randint(0, 5_000),
        "SilverlightVersion3": randint(0, 100_000),
        "WindowName": randint(-1000, 0),
        "CodeVersion": randint(1_000_000, 2_000_000),
        "CLID": randint(0, 100_000),

        "SearchEngineID": randint(0, 31),
        "AdvEngineID": randint(0, 24),
        "OS": randint(0, 59),
        "MobilePhone": randint(0, 119),
        "UserAgent": randint(0, 1000),
        "UserAgentMajor": randint(0, 200),
        "FlashMajor": randint(0, 30),
        "FlashMinor": randint(0, 1000),
        "FlashMinor2": randint(0, 1000),
        "NetMajor": randint(0, 20),
        "NetMinor": randint(0, 100),
        "SilverlightVersion1": randint(0, 10),
        "SilverlightVersion2": randint(0, 60),
        "SilverlightVersion4": randint(0, 10_000),
        "ResolutionWidth": choice(RESOLUTION_WIDTHS),
        "ResolutionHeight": choice(RESOLUTION_HEIGHTS),
        "ResolutionDepth": choice((16, 24, 32)),
        "WindowClientWidth": randint(200, 3840),
        "WindowClientHeight": randint(200, 2160),
        "ClientTimeZone": randint(-720, 840),
        "TraficSourceID": randint(-1, 10),
        "SocialSourceNetworkID": randint(0, 20),
        "URLCategoryID": randint(0, 500),
        "RefererCategoryID": randint(0, 500),
        "Age": randint(0, 100),
        "Sex": choice((0, 1, 2)),
        "Income": randint(0, 10),
        "Interests": randint(0, 10_000),
        "Robotness": randint(0, 20),
        "CounterClass": randint(0, 5),
        "ParamCurrencyID": randint(0, 200),
        "HTTPError": choice(HTTP_ERRORS),
        "GoodEvent": getrandbits(1),
        "DontCountHits": getrandbits(1),
        "HasGCLID": getrandbits(1),
        "HistoryLength": randint(0, 100),
        "IsArtifical": 0,
        "IsDownload": getrandbits(1),
        "IsEvent": getrandbits(1),
        "IsLink": getrandbits(1),
        "IsMobile": getrandbits(1),
        "IsNotBounce": getrandbits(1),
        "IsOldCounter": getrandbits(1),
        "IsParameter": getrandbits(1),
        "IsRefresh": getrandbits(1),
        "CookieEnable": 1,
        "JavaEnable": getrandbits(1),
        "JavascriptEnable": 1,
        "WithHash": getrandbits(1),

        "EventTime": base_ts,
        "EventDate": base_ts,
        "ClientEventTime": base_ts,
        "LocalEventTime": base_ts,
    }


# ---------- Worker ----------

BULK_META = b'{"index":{}}\n'


def worker_loop(worker_id, target_docs, shared_count, shared_bytes, stop_flag,
                endpoint, index_name, auth_header, bulk_size):
    random.seed(os.getpid() * 997 + worker_id)

    urllib3.disable_warnings()
    pool = urllib3.HTTPSConnectionManager if hasattr(urllib3, "HTTPSConnectionManager") else urllib3.HTTPSConnectionPool
    # Use PoolManager for simpler API.
    pm = urllib3.PoolManager(
        maxsize=4,
        cert_reqs="CERT_NONE",
        retries=False,
        timeout=urllib3.Timeout(connect=10, read=120),
    )
    url = f"{endpoint}/{index_name}/_bulk"
    headers = {
        "authorization": auth_header,
        "content-type": "application/x-ndjson",
    }

    while not stop_flag.value:
        with shared_count.get_lock():
            if shared_count.value >= target_docs:
                break

        # Build bulk body.
        parts = []
        for _ in range(bulk_size):
            parts.append(BULK_META)
            parts.append(orjson.dumps(generate_doc()))
            parts.append(b"\n")
        body = b"".join(parts)

        attempts = 0
        while not stop_flag.value:
            try:
                r = pm.request("POST", url, body=body, headers=headers)
                if r.status == 200:
                    # Optional: check for item-level errors. Skip for speed;
                    # OpenSearch returns 200 even if some items fail.
                    body_json = orjson.loads(r.data)
                    if body_json.get("errors"):
                        # Print first error, don't retry — mapping bug would keep failing.
                        first = next(
                            (it for it in body_json["items"] if "error" in it.get("index", {})),
                            None,
                        )
                        print(f"[w{worker_id}] bulk had item errors, e.g. {first}", file=sys.stderr)
                        stop_flag.value = 1
                        return
                    break
                elif r.status in (429, 503):
                    attempts += 1
                    time.sleep(min(30, 0.5 * (2 ** attempts)))
                    continue
                else:
                    print(f"[w{worker_id}] HTTP {r.status}: {r.data[:200]}", file=sys.stderr)
                    attempts += 1
                    if attempts > 5:
                        stop_flag.value = 1
                        return
                    time.sleep(2)
                    continue
            except Exception as e:
                attempts += 1
                if attempts > 10:
                    print(f"[w{worker_id}] give up after {attempts} exceptions: {e}", file=sys.stderr)
                    stop_flag.value = 1
                    return
                time.sleep(min(30, 0.5 * (2 ** attempts)))

        with shared_count.get_lock():
            shared_count.value += bulk_size
        with shared_bytes.get_lock():
            shared_bytes.value += len(body)


# ---------- Main ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", required=True)
    ap.add_argument("--index", default="clickbench_plus")
    ap.add_argument("--user", default="Admin")
    ap.add_argument("--password", default="Admin@123")
    ap.add_argument("--workers", type=int, default=32)
    ap.add_argument("--bulk-size", type=int, default=5000)
    ap.add_argument("--target-docs", type=int, default=100_000)
    args = ap.parse_args()

    import base64
    auth_header = "Basic " + base64.b64encode(
        f"{args.user}:{args.password}".encode()
    ).decode()

    shared_count = mp.Value("q", 0)
    shared_bytes = mp.Value("q", 0)
    stop_flag = mp.Value("b", 0)

    def on_sig(signum, frame):
        stop_flag.value = 1

    signal.signal(signal.SIGINT, on_sig)
    signal.signal(signal.SIGTERM, on_sig)

    procs = []
    t0 = time.time()
    for wid in range(args.workers):
        p = mp.Process(
            target=worker_loop,
            args=(wid, args.target_docs, shared_count, shared_bytes, stop_flag,
                  args.endpoint, args.index, auth_header, args.bulk_size),
            daemon=True,
        )
        p.start()
        procs.append(p)

    # Progress loop
    last_count = 0
    last_bytes = 0
    last_t = t0
    while any(p.is_alive() for p in procs):
        time.sleep(5)
        now = time.time()
        c = shared_count.value
        b = shared_bytes.value
        dc = c - last_count
        db = b - last_bytes
        dt = now - last_t
        total_dt = now - t0
        print(
            f"[t+{total_dt:6.0f}s] docs={c:>14,} (+{dc/dt:>9,.0f}/s)  "
            f"bulk_bytes={b/1e9:7.2f} GB (+{db/dt/1e6:6.1f} MB/s)",
            flush=True,
        )
        last_count = c
        last_bytes = b
        last_t = now
        if c >= args.target_docs:
            break

    stop_flag.value = 1
    for p in procs:
        p.join(timeout=30)
    total_dt = time.time() - t0
    print(
        f"DONE. docs={shared_count.value:,}  bulk_bytes={shared_bytes.value/1e9:.2f} GB  "
        f"wall={total_dt:.0f}s  avg={shared_count.value/total_dt:,.0f} docs/s  "
        f"{shared_bytes.value/total_dt/1e6:.1f} MB/s"
    )


if __name__ == "__main__":
    main()
