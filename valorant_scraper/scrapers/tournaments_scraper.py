import requests
from bs4 import BeautifulSoup
import time
import re
import pandas as pd
import os
import hashlib
from utils.rate_limiter import rate_limit
from utils.csv_handler import save_to_csv, load_from_csv
from config.settings import VLR_BASE_URL

@rate_limit
def scrape_tournaments_data(start_page=1, tier=60, delay=1.0):
    """
    Scrape VLR.gg tournament data dynamically until no more events are found.
    Skip tournaments that already exist in the temp file.
    """
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

    raw_path = "/opt/airflow/valorant_scraper/data/raw/tournaments.csv"
    temp_path = "/opt/airflow/valorant_scraper/data/temp/tournaments.csv"

    # Load existing IDs if available
    existing_ids = set()
    if os.path.exists(temp_path):
        try:
            existing_df = load_from_csv(temp_path)
            existing_ids = set(existing_df['tournament_id'].astype(str).tolist())
            print(f"Loaded {len(existing_ids)} existing tournament IDs from temp file.")
        except Exception as e:
            print(f"Could not load temp file: {e}")

    all_tournaments = []
    page = start_page

    while True:
        print(f"Scraping page {page}...")
        url = f"{VLR_BASE_URL}/events/?tier={tier}&page={page}"

        try:
            time.sleep(delay)
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")

            events_container = soup.select_one(
                "#wrapper > div.col-container > div > div.events-container > div:nth-child(2)"
            )
            if not events_container:
                print(f"No events container found on page {page}. Stopping.")
                break

            event_cards = events_container.find_all("a", class_="wf-card")
            if not event_cards:
                print(f"No tournaments found on page {page}. Scraping complete.")
                break

            new_tournaments = []

            for card in event_cards:
                try:
                    tournament = {"prize_pool": 0}

                    title_elem = card.find("div", class_="event-item-title")
                    tournament["name"] = title_elem.get_text(strip=True) if title_elem else None

                    prize_elem = card.find("div", class_="event-item-desc-item mod-prize")
                    if prize_elem:
                        prize_text = prize_elem.find(text=True, recursive=False)
                        if prize_text:
                            match = re.search(r"\$([\d,]+)", prize_text.strip())
                            tournament["prize_pool"] = int(match.group(1).replace(",", "")) if match else 0

                    location_elem = card.find("i", class_="flag")
                    if location_elem:
                        classes = location_elem.get("class", [])
                        tournament["region"] = classes[-1] if classes else None

                    href = card.get("href", "")
                    tournament["url"] = f"{VLR_BASE_URL}{href}" if href else None

                    # Extract ID from URL (e.g., /event/2283/... -> 2283)
                    tournament["tournament_id"] = None
                    if href:
                        match = re.search(r"/event/(\d+)/", href)
                        if match:
                            tournament["tournament_id"] = match.group(1)

                    # Skip if no valid ID found
                    if not tournament["tournament_id"]:
                        print(f"Could not extract ID from URL: {href}")
                        continue

                    # Skip already seen IDs
                    if tournament["tournament_id"] in existing_ids:
                        print(f"Skipping already scraped tournament: {tournament['name']}")
                        continue

                    new_tournaments.append(tournament)
                    existing_ids.add(tournament["tournament_id"])

                except Exception as e:
                    print(f"Error parsing tournament card: {e}")
                    continue

            save_to_csv(new_tournaments, raw_path)
            save_to_csv(pd.DataFrame([t["tournament_id"] for t in new_tournaments], columns=["tournament_id"]), temp_path)

            all_tournaments.extend(new_tournaments)
            print(f"Found {len(new_tournaments)} new tournaments on page {page}")
            page += 1

        except requests.RequestException as e:
            print(f"Error fetching page {page}: {e}")
            break
        except Exception as e:
            print(f"Error parsing page {page}: {e}")
            break