import requests
from bs4 import BeautifulSoup
import time
import re
import pandas as pd
import os
from utils.rate_limiter import rate_limit
from utils.csv_handler import save_to_csv, load_from_csv

@rate_limit
def scrape_tournaments_match_data(delay=1.0):
    """
    Scrape tournament matches from VLR.gg.
    Reads tournament URLs from tournaments.csv and saves match details.
    Skips tournaments that have already been scraped.
    """
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    tournaments_path = "/opt/airflow/valorant_scraper/data/raw/tournaments.csv"
    output_path = "/opt/airflow/valorant_scraper/data/raw/tournaments_match.csv"
    temp_path = "/opt/airflow/valorant_scraper/data/temp/tournaments_match.csv"
    
    scraped_ids = set()
    if os.path.exists(temp_path):
        try:
            temp_df = load_from_csv(temp_path)
            scraped_ids = set(temp_df['tournament_id'].astype(str).tolist())
            print(f"Loaded {len(scraped_ids)} already scraped tournament IDs.")
        except Exception as e:
            print(f"Could not load temp file: {e}")
    
    if not os.path.exists(tournaments_path):
        print(f"Error: {tournaments_path} not found.")
        return
    
    tournaments_df = load_from_csv(tournaments_path)
    
    id_column = 'tournament_id' if 'tournament_id' in tournaments_df.columns else 'id'
    tournaments_df[id_column] = tournaments_df[id_column].astype(str)
    
    tournaments_to_scrape = tournaments_df[~tournaments_df[id_column].isin(scraped_ids)]
    print(f"Found {len(tournaments_to_scrape)} new tournaments to scrape (skipping {len(scraped_ids)} already scraped).")
    
    if tournaments_to_scrape.empty:
        print("No new tournaments to scrape.")
        return
    
    all_matches = []
    
    for idx, row in tournaments_to_scrape.iterrows():
        tournament_id = str(row[id_column])
        tournament_url = row['url']
        tournament_name = row.get('name', 'Unknown')
        
        if match := re.search(r'/event/(\d+)/', tournament_url):
            event_id = match.group(1)
            matches_url = f"https://www.vlr.gg/event/matches/{event_id}"
        else:
            print(f"Could not extract event ID from URL: {tournament_url}")
            continue
        
        print(f"Scraping tournament {idx + 1}/{len(tournaments_df)}: {tournament_name} (ID: {tournament_id})")
        
        try:
            time.sleep(delay)
            response = requests.get(matches_url, headers=headers, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")

            match_cards = soup.select("a.match-item")

            if not match_cards:
                print(f"No matches found for {tournament_name}")
                continue

            for match_card in match_cards:
                try:
                    match_href = match_card.get("href", "")
                    match_url = f"https://www.vlr.gg{match_href}" if match_href else None
                    match_id = None
                    if match_href:
                        if match := re.search(r'/(\d+)/', match_href):
                            match_id = match.group(1)
                    
                    if not match_id:
                        continue
                    
                    vs_section = match_card.find("div", class_="match-item-vs")
                    if not vs_section:
                        continue
                    
                    teams = vs_section.find_all("div", class_="match-item-vs-team")
                    if len(teams) < 2:
                        continue
                    
                    team1_name_div = teams[0].find("div", class_="text-of")
                    team1_name = team1_name_div.get_text(strip=True) if team1_name_div else None
                    team1_score_div = teams[0].find("div", class_="match-item-vs-team-score")
                    team1_score = None
                    if team1_score_div:
                        score_text = team1_score_div.get_text(strip=True)
                        if score_text.isdigit():
                            team1_score = int(score_text)
                    
                    team2_name_div = teams[1].find("div", class_="text-of")
                    team2_name = team2_name_div.get_text(strip=True) if team2_name_div else None
                    team2_score_div = teams[1].find("div", class_="match-item-vs-team-score")
                    team2_score = None
                    if team2_score_div:
                        score_text = team2_score_div.get_text(strip=True)
                        if score_text.isdigit():
                            team2_score = int(score_text)
                    
                    # Normalize scores: convert multi-map series to binary win/loss
                    if team1_score is not None and team2_score is not None and team1_score + team2_score > 5:
                        if team1_score > team2_score:
                            team1_score, team2_score = 1, 0
                        else:
                            team1_score, team2_score = 0, 1
                    
                    if "mod-winner" in teams[0].get("class", []):
                        winner_name = team1_name
                        loser_name = team2_name
                    elif "mod-winner" in teams[1].get("class", []):
                        winner_name = team2_name
                        loser_name = team1_name
                    else:
                        winner_name = None
                        loser_name = None
                    
                    event_div = match_card.find("div", class_="match-item-event")
                    stage = None
                    sub_stage = None
                    
                    if event_div:
                        series_div = event_div.find("div", class_="match-item-event-series")
                        if series_div:
                            sub_stage = series_div.get_text(strip=True)
                        
                        stage_text = event_div.get_text(strip=True)
                        if sub_stage:
                            stage = stage_text.replace(sub_stage, "").strip()
                        else:
                            stage = stage_text
                    
                    all_matches.append({
                        "tournament_id": tournament_id,
                        "match_id": match_id,
                        "team1_name": team1_name,
                        "team2_name": team2_name,
                        "score1": team1_score,
                        "score2": team2_score,
                        "winner_name": winner_name,
                        "loser_name": loser_name,
                        "sub_stage": sub_stage,
                        "stage": stage,
                        "match_url": match_url
                    })
                    
                except Exception as e:
                    print(f"Error parsing match: {e}")
                    continue
            
            print(f"Scraped {len([m for m in all_matches if m['tournament_id'] == tournament_id])} matches")
            
        except requests.RequestException as e:
            print(f"Error fetching matches for {tournament_name}: {e}")
            continue
        except Exception as e:
            print(f"Error parsing matches for {tournament_name}: {e}")
            continue
    
    if all_matches:
        matches_df = pd.DataFrame(all_matches)
        
        save_to_csv(matches_df, output_path)
        
        new_scraped_ids = matches_df[['tournament_id', 'match_id']].drop_duplicates().reset_index(drop=True)
        save_to_csv(new_scraped_ids, temp_path)
        
        print(f"\nSaved {len(all_matches)} matches to {output_path}")
    else:
        print("\nNo matches scraped.")
# tournament_id, match_id, team_id, game_id, map, player_id, agent, rating, acs, kill, death, assist, kd, kast, adr, hs, fk, fd, fkfd, round