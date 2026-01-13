import cloudscraper
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time

BASE_URL = "https://tolkiengateway.net"
CATEGORY_URL = "https://tolkiengateway.net/wiki/Category:Characters"
OUTPUT_FILE = "data/raw/characters_categories.txt"

scraper = cloudscraper.create_scraper()

def get_subcategories(category_url):
    """Return a list of subcategory URLs from a category page."""
    subcats = []
    r = scraper.get(category_url, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    for a in soup.select("div#mw-subcategories a"):
        href = a.get("href")
        if href and href.startswith("/wiki/Category:"):
            subcats.append(urljoin(BASE_URL, href))
    return subcats

def get_character_links_from_category(category_url):
    """Get all character names from a single category page (handles pagination)."""
    names = set()
    next_url = category_url
    while next_url:
        r = scraper.get(next_url, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        for a in soup.select("div.mw-category a"):
            title = a.get_text(strip=True)
            if title:
                names.add(title)
        next_link = soup.find("a", string="next page")
        next_url = urljoin(BASE_URL, next_link["href"]) if next_link else None
        time.sleep(1)
    return names

def scrape_all_characters(category_url):
    all_names = set()

    # First, get all subcategories
    subcats = get_subcategories(category_url)
    print(f"Found {len(subcats)} subcategories")

    # Go through each subcategory and collect character names
    for sub in subcats:
        print(f"Scraping subcategory: {sub}")
        names = get_character_links_from_category(sub)
        print(f"Found {len(names)} characters in this subcategory")
        all_names.update(names)
    
    return sorted(all_names)

if __name__ == "__main__":
    characters = scrape_all_characters(CATEGORY_URL)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for c in characters:
            f.write(c + "\n")
    print(f"Saved {len(characters)} characters to {OUTPUT_FILE}")
