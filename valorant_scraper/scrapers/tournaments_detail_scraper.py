import requests
from bs4 import BeautifulSoup
import time
import re
import pandas as pd
import os
from collections import defaultdict
from utils.rate_limiter import rate_limit
from utils.csv_handler import save_to_csv, load_from_csv

@rate_limit
def scrape_tournaments_detail_data(delay=1.0):
    """
    Scrape tournament details (placements, prizes, circuit points, teams) from VLR.gg.
    Reads tournament URLs from tournaments.csv and saves detailed results.
    Skips tournaments that have already been scraped.
    """
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    tournaments_path = "/opt/airflow/valorant_scraper/data/raw/tournaments.csv"
    output_path = "/opt/airflow/valorant_scraper/data/raw/tournaments_detail.csv"
    temp_path = "/opt/airflow/valorant_scraper/data/temp/tournaments_detail.csv"
    
    # Load existing scraped tournament IDs
    scraped_ids = set()
    if os.path.exists(temp_path):
        try:
            temp_df = load_from_csv(temp_path)
            scraped_ids = set(temp_df['tournament_id'].astype(str).tolist())
            print(f"Loaded {len(scraped_ids)} already scraped tournament IDs.")
        except Exception as e:
            print(f"Could not load temp file: {e}")
    
    # Load tournaments to scrape
    if not os.path.exists(tournaments_path):
        print(f"Error: {tournaments_path} not found.")
        return
    
    tournaments_df = load_from_csv(tournaments_path)
    
    # Determine the correct ID column name
    id_column = 'tournament_id' if 'tournament_id' in tournaments_df.columns else 'id'
    tournaments_df[id_column] = tournaments_df[id_column].astype(str)
    
    # Filter out already scraped tournaments
    tournaments_to_scrape = tournaments_df[~tournaments_df[id_column].isin(scraped_ids)]
    print(f"Found {len(tournaments_to_scrape)} new tournaments to scrape (skipping {len(scraped_ids)} already scraped).")
    
    if tournaments_to_scrape.empty:
        print("No new tournaments to scrape.")
        return
    
    all_details = []
    
    for idx, row in tournaments_to_scrape.iterrows():
        tournament_id = str(row[id_column])
        tournament_url = row['url']
        tournament_name = row.get('name', 'Unknown')
        
        print(f"Scraping tournament {idx + 1}/{len(tournaments_df)}: {tournament_name} (ID: {tournament_id})")
        
        try:
            time.sleep(delay)
            response = requests.get(tournament_url, headers=headers, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            
            # Find all stage links (Playoffs, Group Stage, etc.)
            stage_links = soup.find("div", class_="wf-subnav mod-dark")
            stages = []
            
            if stage_links:
                stage_items = stage_links.find_all("a", class_="wf-subnav-item")
                for stage_item in stage_items:
                    stage_url = stage_item.get("href", "")
                    stage_title_elem = stage_item.find("div", class_="wf-subnav-item-title")
                    stage_title = stage_title_elem.get_text(strip=True) if stage_title_elem else None
                    if stage_url and stage_title:
                        stages.append({
                            "url": f"https://www.vlr.gg{stage_url}" if not stage_url.startswith("http") else stage_url,
                            "name": stage_title
                        })
            
            # If no stages found, use current page
            if not stages:
                stages = [{"url": tournament_url, "name": "Main Event"}]
            
            # Scrape each stage
            for stage in stages:
                try:
                    if stage["url"] != tournament_url:
                        time.sleep(delay)
                        stage_response = requests.get(stage["url"], headers=headers, timeout=10)
                        stage_response.raise_for_status()
                        stage_soup = BeautifulSoup(stage_response.content, "html.parser")
                    else:
                        stage_soup = soup
                    
                    standings_table = stage_soup.find("table", class_="wf-table mod-simple")
                    if not standings_table or not standings_table.find("tbody"):
                        print(f"No standings table found for {stage['name']}")
                        continue
                    
                    rows = standings_table.find("tbody").find_all("tr")
                    
                    # Calculate normalization factor for circuit points
                    max_circuit_points = max(
                        (int(match.group(1)) for row in rows 
                         if len(row.find_all("td")) > 3 
                         and (match := re.search(r'(\d+)\s*points?', row.find_all("td")[3].get_text(strip=True)))),
                        default=0
                    )
                    normalization_factor = 11.0 / max_circuit_points if max_circuit_points > 0 else 1.0
                    
                    # Track placement ranges to assign individual placements
                    placement_counters = defaultdict(int)
                    
                    for row in rows:
                        try:
                            cells = row.find_all("td")
                            if len(cells) < 3:
                                continue
                            
                            # Extract placement range and convert to individual placement
                            place_text = re.sub(r'(st|nd|rd|th)', '', cells[0].get_text(strip=True))
                            if '–' in place_text or '-' in place_text:
                                start_place = int(re.search(r'(\d+)', place_text).group(1))
                                placement = start_place + placement_counters[place_text]
                                placement_counters[place_text] += 1
                            else:
                                placement = int(place_text)
                            
                            # Extract prize
                            prize_text = cells[1].get_text(strip=True)
                            prize = None
                            if prize_text and prize_text != "-":
                                if match := re.search(r'\$?([\d,]+)', prize_text):
                                    num_str = int(match.group(1).replace(",", ""))
                                    prize = int(float(num_str))
                            
                            # Extract team ID
                            team_link = cells[2].find("a", class_="standing-item-team")
                            team_id = None
                            team_name = None
                            if team_link and (href := team_link.get("href", "")):
                                if match := re.search(r'/team/(\d+)/', href):
                                    team_id = match.group(1)

                            team_name_elem = team_link.find("div", class_="standing-item-team-name")
                            if team_name_elem:
                                # The team name text is the first direct text node (before the country <div>)
                                team_name_text = team_name_elem.find(string=True, recursive=False)
                                if team_name_text:
                                    team_name = team_name_text.strip()
                            
                            # Extract circuit points
                            circuit_points = None
                            circuit_points_normalized = None
                            if len(cells) > 3:
                                points_text = cells[3].get_text(strip=True)
                                if match := re.search(r'(\d+)\s*points?', points_text):
                                    circuit_points = int(match.group(1))
                                    circuit_points_normalized = round(circuit_points * normalization_factor, 2)
                            
                            # Extract note (Champions qualification, etc.)
                            note = None
                            if len(cells) > 4 and (note_link := cells[4].find("a")):
                                note = note_link.get_text(strip=True)
                            
                            all_details.append({
                                "tournament_id": tournament_id,
                                "stage": stage["name"],
                                "placement": placement,
                                "prize": prize,
                                "team_id": team_id,
                                "team_name": team_name,
                                "circuit_points": circuit_points,
                                "circuit_points_normalized": circuit_points_normalized,
                                "note": note
                            })
                            
                        except Exception as e:
                            print(f"Error parsing row in {stage['name']}: {e}")
                            continue
                
                except Exception as e:
                    print(f"Error scraping stage {stage['name']}: {e}")
                    continue
            
            print(f"Scraped {len([d for d in all_details if d['tournament_id'] == tournament_id])} placements")
            
        except requests.RequestException as e:
            print(f"Error fetching tournament {tournament_name}: {e}")
            continue
        except Exception as e:
            print(f"Error parsing tournament {tournament_name}: {e}")
            continue
    
    # Save results
    if all_details:
        details_df = pd.DataFrame(all_details)
        save_to_csv(details_df, output_path)
        
        # Save tournament IDs to temp file for tracking
        new_scraped_ids = details_df[['tournament_id', 'team_id']].drop_duplicates().reset_index(drop=True)
        save_to_csv(new_scraped_ids, temp_path)
        
        print(f"\nSaved {len(all_details)} placement details to {output_path}")
    else:
        print("\nNo tournament details scraped.")