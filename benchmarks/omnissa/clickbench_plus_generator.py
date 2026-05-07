"""Custom OSB synthetic-data-generator module producing clickbench-shaped docs.

Invoked by opensearch-benchmark generate-data --custom-module=<this file>.
Exports: generate_synthetic_document(providers, **custom_lists) -> dict.

Design notes
------------
- Keyword fields: Mimesis for realistic URL/title/phrase shapes; bounded enums for
  country/language/currency; Zipfian synthesis for UserID and CounterID so terms
  aggs are meaningful (a handful of hot values, long tail of cold ones).
- Short fields in clickbench are ordinal enums, not random shorts: SearchEngineID
  is one of ~20, OS one of ~60, etc. Uniform random in [-32K, 32K] would make
  every agg on these fields degenerate.
- HTTPError skewed to 0 (most requests succeed).
- SearchPhrase empty 70% of the time (matches real clickbench "no search" skew).
- Event timestamps are correlated (one base ts per doc, all 4 date fields derived).
"""

import random
import uuid


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
    "naver", "daum", "qwant", "ecosia", "startpage", "mail", "direct",
]
UTM_MEDIUMS = ["cpc", "organic", "email", "social", "direct", "referral", "banner", "display"]
PAGE_CHARSETS = ["utf-8", "windows-1251", "iso-8859-1", "windows-1252", "shift_jis", "gb2312"]
PARAM_CURRENCIES = ["USD", "EUR", "RUB", "GBP", "JPY", "CNY", "INR", "BRL", "MXN", "KRW", "TRY", "CAD", "AUD"]
OPENSTAT_SERVICES = ["direct", "mail", "social", "banner", "search", "display", "native"]

SEARCH_ENGINE_IDS = list(range(0, 32))
OS_IDS = list(range(0, 60))
ADV_ENGINE_IDS = list(range(0, 25))
MOBILE_PHONE_IDS = list(range(0, 120))

RESOLUTION_WIDTHS = [1920, 1366, 1440, 1536, 1280, 2560, 3840, 1680, 1600, 1024]
RESOLUTION_HEIGHTS = [1080, 768, 900, 864, 720, 1440, 2160, 1050, 900, 768]
MOBILE_MODELS = [
    "", "", "", "", "", "",
    "iPhone 14", "iPhone 13", "iPhone 12", "iPhone 15",
    "Pixel 7", "Pixel 8", "Pixel 6",
    "Galaxy S23", "Galaxy S22", "Galaxy S24", "Galaxy A54",
    "OnePlus 11", "Xiaomi 13", "Redmi Note 12",
]
HTTP_ERRORS = [0, 0, 0, 0, 0, 0, 0, 0, 200, 200, 200, 301, 302, 404, 500, 403, 503]


def weighted_zipf(n: int, s: float = 1.2) -> int:
    """Pick one of range(n) with Zipfian weights (lower ranks far more likely).

    Uses rejection-free inverse transform on pre-normalized weights. Cheap
    enough at n <= 1M but the 10M user ID case uses a faster formulation.
    """
    if n > 100_000:
        # Discrete approximation: sample rank via continuous Zipf inverse.
        # For s > 1, CDF inverse of Zipf(1..n, s) is approximately k = n ^ u where u ~ U(0,1).
        u = random.random()
        return int((n ** u)) - 1 if u < 1 else 0
    weights = [1.0 / ((i + 1) ** s) for i in range(n)]
    total = sum(weights)
    r = random.random() * total
    acc = 0.0
    for i, w in enumerate(weights):
        acc += w
        if r <= acc:
            return i
    return n - 1


def generate_synthetic_document(providers, **custom_lists):
    g = providers["generic"]

    user_id = weighted_zipf(10_000_000, s=0.8)
    watch_id = random.getrandbits(63)
    counter_id = weighted_zipf(1000, s=1.4)

    url = g.internet.url()
    referer = g.internet.url()
    title = g.text.title()

    if random.random() < 0.7:
        search_phrase = ""
    else:
        search_phrase = " ".join(g.text.words(quantity=random.randint(1, 5)))

    dt = g.datetime.datetime(start=2023, end=2025)
    base_ts = dt.strftime("%Y-%m-%d %H:%M:%S")

    return {
        # Keywords (high cardinality)
        "URL": url,
        "Referer": referer,
        "OriginalURL": url,
        "Title": title,
        "SearchPhrase": search_phrase,
        "MobilePhoneModel": random.choice(MOBILE_MODELS),
        "Params": "a={}&b={}".format(random.randint(0, 10000), random.randint(0, 10000)),
        "UserAgentMinor": "{}.{}".format(random.randint(0, 200), random.randint(0, 9)),
        "SocialSourcePage": g.internet.url(),

        # Keywords (bounded enums)
        "BrowserCountry": random.choice(BROWSER_COUNTRY),
        "BrowserLanguage": random.choice(BROWSER_LANGUAGE),
        "HitColor": random.choice(HIT_COLORS),
        "UTMSource": random.choice(UTM_SOURCES),
        "UTMMedium": random.choice(UTM_MEDIUMS),
        "UTMCampaign": "camp_{}".format(random.randint(0, 500)),
        "UTMContent": "content_{}".format(random.randint(0, 2000)),
        "UTMTerm": "term_{}".format(random.randint(0, 3000)),
        "FromTag": "tag_{}".format(random.randint(0, 500)),
        "PageCharset": random.choice(PAGE_CHARSETS),
        "ParamCurrency": random.choice(PARAM_CURRENCIES),
        "ParamOrderID": "order_{}".format(uuid.uuid4().hex[:16]),
        "OpenstatServiceName": random.choice(OPENSTAT_SERVICES),
        "OpenstatCampaignID": "ocamp_{}".format(random.randint(0, 500)),
        "OpenstatAdID": "oad_{}".format(random.randint(0, 2000)),
        "OpenstatSourceID": "osrc_{}".format(random.randint(0, 200)),

        # Longs
        "UserID": user_id,
        "WatchID": watch_id,
        "FUniqID": random.getrandbits(63),
        "RefererHash": random.getrandbits(63),
        "URLHash": hash(url) & ((1 << 63) - 1),
        "ParamPrice": random.randint(0, 1_000_000),

        # Integers
        "CounterID": counter_id,
        "ClientIP": random.getrandbits(31),
        "RemoteIP": random.getrandbits(31),
        "IPNetworkID": random.randint(0, 200_000),
        "HID": random.getrandbits(31),
        "RegionID": random.randint(0, 10_000),
        "URLRegionID": random.randint(0, 10_000),
        "RefererRegionID": random.randint(0, 10_000),
        "OpenerName": random.randint(0, 100),
        "ConnectTiming": random.randint(0, 5_000),
        "DNSTiming": random.randint(0, 1_000),
        "FetchTiming": random.randint(0, 10_000),
        "ResponseStartTiming": random.randint(0, 10_000),
        "ResponseEndTiming": random.randint(0, 15_000),
        "SendTiming": random.randint(0, 5_000),
        "SilverlightVersion3": random.randint(0, 100_000),
        "WindowName": random.randint(-1000, 0),
        "CodeVersion": random.randint(1_000_000, 2_000_000),
        "CLID": random.randint(0, 100_000),

        # Shorts (bounded enums — NOT uniform over full short range)
        "SearchEngineID": random.choice(SEARCH_ENGINE_IDS),
        "AdvEngineID": random.choice(ADV_ENGINE_IDS),
        "OS": random.choice(OS_IDS),
        "MobilePhone": random.choice(MOBILE_PHONE_IDS),
        "UserAgent": random.randint(0, 1000),
        "UserAgentMajor": random.randint(0, 200),
        "FlashMajor": random.randint(0, 30),
        "FlashMinor": random.randint(0, 1000),
        "FlashMinor2": random.randint(0, 1000),
        "NetMajor": random.randint(0, 20),
        "NetMinor": random.randint(0, 100),
        "SilverlightVersion1": random.randint(0, 10),
        "SilverlightVersion2": random.randint(0, 60),
        "SilverlightVersion4": random.randint(0, 10_000),
        "ResolutionWidth": random.choice(RESOLUTION_WIDTHS),
        "ResolutionHeight": random.choice(RESOLUTION_HEIGHTS),
        "ResolutionDepth": random.choice([16, 24, 32]),
        "WindowClientWidth": random.randint(200, 3840),
        "WindowClientHeight": random.randint(200, 2160),
        "ClientTimeZone": random.randint(-720, 840),
        "TraficSourceID": random.randint(-1, 10),
        "SocialSourceNetworkID": random.randint(0, 20),
        "URLCategoryID": random.randint(0, 500),
        "RefererCategoryID": random.randint(0, 500),
        "Age": random.randint(0, 100),
        "Sex": random.choice([0, 1, 2]),
        "Income": random.randint(0, 10),
        "Interests": random.randint(0, 10_000),
        "Robotness": random.randint(0, 20),
        "CounterClass": random.randint(0, 5),
        "ParamCurrencyID": random.randint(0, 200),
        "HTTPError": random.choice(HTTP_ERRORS),
        "GoodEvent": random.choice([0, 1]),
        "DontCountHits": random.choice([0, 1]),
        "HasGCLID": random.choice([0, 1]),
        "HistoryLength": random.randint(0, 100),
        "IsArtifical": 0,
        "IsDownload": random.choice([0, 1]),
        "IsEvent": random.choice([0, 1]),
        "IsLink": random.choice([0, 1]),
        "IsMobile": random.choice([0, 1]),
        "IsNotBounce": random.choice([0, 1]),
        "IsOldCounter": random.choice([0, 1]),
        "IsParameter": random.choice([0, 1]),
        "IsRefresh": random.choice([0, 1]),
        "CookieEnable": 1,
        "JavaEnable": random.choice([0, 1]),
        "JavascriptEnable": 1,
        "WithHash": random.choice([0, 1]),

        # Dates (correlated)
        "EventTime": base_ts,
        "EventDate": base_ts,
        "ClientEventTime": base_ts,
        "LocalEventTime": base_ts,
    }
