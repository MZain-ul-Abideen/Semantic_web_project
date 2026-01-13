import json
import time
import requests
from urllib.parse import quote

INPUT_FILE = "data/raw/all_characters.txt"
OUTPUT_FILE = "data/raw/json_pages.json"

API_URL = "https://tolkiengateway.net/w/api.php"

# Optional: limit for testing
TEST_LIMIT = None  # set to a number for testing

def fetch_page_json(title):
    """Fetch wikitext, templates, links for a page via MediaWiki API."""
    params = {
        "action": "parse",
        "page": title,
        "format": "json",
        "prop": "wikitext|templates|links"
    }
    try:
        r = requests.get(API_URL, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        if "error" in data:
            print(f"⚠ API error for {title}: {data['error']}")
            return None
        return {
            "title": title,
            "json": data
        }
    except Exception as e:
        print(f"✗ Failed to fetch {title}: {e}")
        return None

if __name__ == "__main__":
    with open(INPUT_FILE, encoding="utf-8") as f:
        titles = [line.strip() for line in f if line.strip()]

    if TEST_LIMIT:
        titles = titles[:TEST_LIMIT]
        print(f"TEST MODE: scraping only {len(titles)} pages")

    pages = []
    total = len(titles)

    for i, title in enumerate(titles, 1):
        print(f"[{i}/{total}] Fetching: {title}")
        page_data = fetch_page_json(title)
        if page_data:
            pages.append(page_data)
        time.sleep(1)  # be nice to the server

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(pages, f, indent=2, ensure_ascii=False)

    print(f"\nSaved {len(pages)} pages to {OUTPUT_FILE}")
