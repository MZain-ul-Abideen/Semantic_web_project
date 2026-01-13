import cloudscraper
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time

BASE_URL = "https://tolkiengateway.net"
INPUT_FILE = "data/raw/characters_categories.txt"  # the subcategories you scraped
OUTPUT_FILE = "data/raw/all_characters.txt"

scraper = cloudscraper.create_scraper()

def get_character_links_from_category(category_url):
    """Get all character names from a single category page (handles pagination)."""
    names = set()
    next_url = category_url
    while next_url:
        print(f"Scraping category page: {next_url}")
        try:
            r = scraper.get(next_url, timeout=20)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "lxml")
            
            # Character links are under div.mw-category a
            for a in soup.select("div.mw-category a"):
                title = a.get_text(strip=True)
                if title:
                    names.add(title)
            
            # Check for pagination
            next_link = soup.find("a", string="next page")
            next_url = urljoin(BASE_URL, next_link["href"]) if next_link else None
            time.sleep(1)
        except Exception as e:
            print(f"Error scraping {next_url}: {e}")
            next_url = None
    return names

if __name__ == "__main__":
    # Load subcategory names
    with open(INPUT_FILE, encoding="utf-8") as f:
        subcategories = [line.strip() for line in f if line.strip()]
    
    all_characters = set()
    
    for i, sub in enumerate(subcategories, 1):
        # Build the URL for the subcategory page
        sub_url = urljoin(BASE_URL, "/wiki/Category:" + sub.replace(" ", "_"))
        print(f"[{i}/{len(subcategories)}] Scraping subcategory: {sub}")
        characters = get_character_links_from_category(sub_url)
        print(f"Found {len(characters)} characters in this subcategory")
        all_characters.update(characters)
    
    # Save all unique character names
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for c in sorted(all_characters):
            f.write(c + "\n")
    
    print(f"\nSaved {len(all_characters)} characters to {OUTPUT_FILE}")
