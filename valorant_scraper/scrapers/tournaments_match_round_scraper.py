import requests
from bs4 import BeautifulSoup
import time
import re
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from utils.rate_limiter import rate_limit
from utils.csv_handler import save_to_csv, load_from_csv
import logging
import gc

logger = logging.getLogger(__name__)

def get_session_with_retries():
    """Create a requests session with retry logic"""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

@rate_limit
def scrape_tournaments_match_round_data(delay=0.3, max_workers=10, batch_size=50):
    """
    Scrape round-by-round results from VLR.gg match pages.
    Memory-optimized with streaming processing for 24k+ games.
    Only processes matches not in temp checkpoint file.
    
    Args:
        delay: Delay between requests (0.3s = ~200 req/min)
        max_workers: Number of concurrent threads (10 for speed)
        batch_size: Save every N games (50 for memory management)
    """
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    matches_path = "/opt/airflow/valorant_scraper/data/raw/tournaments_match.csv"
    output_path = "/opt/airflow/valorant_scraper/data/raw/tournaments_match_round.csv"
    temp_path = "/opt/airflow/valorant_scraper/data/temp/tournaments_match_round.csv"
    
    os.makedirs(os.path.dirname(temp_path), exist_ok=True)
    
    # Load processed matches (not games)
    processed_matches = set()
    if os.path.exists(temp_path):
        try:
            temp_df = load_from_csv(temp_path)
            processed_matches = set(temp_df['match_id'].astype(str).unique())
            logger.info(f"Loaded {len(processed_matches)} already processed matches.")
            del temp_df
            gc.collect()
        except Exception as e:
            logger.warning(f"Could not load temp file: {e}")
    
    if not os.path.exists(matches_path):
        logger.error(f"Error: {matches_path} not found.")
        return
    
    # Load matches and filter out processed ones
    matches_df = load_from_csv(matches_path)
    matches_df['match_id'] = matches_df['match_id'].astype(str)
    
    # Filter to only unprocessed matches
    unprocessed_matches = matches_df[~matches_df['match_id'].isin(processed_matches)]
    
    total_matches = len(matches_df)
    remaining_matches = len(unprocessed_matches)
    
    logger.info(f"Total matches: {total_matches}")
    logger.info(f"Already processed: {len(processed_matches)}")
    logger.info(f"Remaining to process: {remaining_matches}")
    
    if remaining_matches == 0:
        logger.info("No new matches to process!")
        return
    
    def scrape_game(game_data):
        """Helper function to scrape a single game"""
        tournament_id, match_id, game_id, map_name, game_url, team1_name, team2_name = game_data
        session = get_session_with_retries()
        
        try:
            time.sleep(delay)
            game_response = session.get(game_url, headers=headers, timeout=20)
            game_response.raise_for_status()
            game_soup = BeautifulSoup(game_response.content, "html.parser")
            
            rounds_section = game_soup.find("div", class_="vlr-rounds")
            if not rounds_section:
                session.close()
                return [], None
            
            team_divs = rounds_section.find_all("div", class_="team")
            if len(team_divs) >= 2:
                round_team1 = team_divs[0].get_text(strip=True)
                round_team2 = team_divs[1].get_text(strip=True)
            else:
                round_team1 = team1_name
                round_team2 = team2_name
            
            round_cols = rounds_section.find_all("div", class_="vlr-rounds-row-col")
            game_rounds = []
            
            for round_col in round_cols:
                if "mod-spacing" in round_col.get("class", []):
                    continue
                
                round_num_div = round_col.find("div", class_="rnd-num")
                if not round_num_div:
                    continue
                
                round_number = round_num_div.get_text(strip=True)
                round_score = round_col.get("title", "")
                
                if not round_score:
                    continue
                
                score_parts = round_score.split("-")
                if len(score_parts) != 2:
                    continue
                
                try:
                    team1_score = int(score_parts[0])
                    team2_score = int(score_parts[1])
                except ValueError:
                    continue
                
                round_squares = round_col.find_all("div", class_="rnd-sq")
                winner_team = None
                win_type = None
                side = None
                
                for sq in round_squares:
                    if "mod-win" in sq.get("class", []):
                        if round_squares.index(sq) == 0:
                            winner_team = round_team1
                        else:
                            winner_team = round_team2
                        
                        if "mod-t" in sq.get("class", []):
                            side = "attack"
                        elif "mod-ct" in sq.get("class", []):
                            side = "defense"
                        
                        img = sq.find("img")
                        if img:
                            img_src = img.get("src", "")
                            if "elim" in img_src:
                                win_type = "elimination"
                            elif "boom" in img_src:
                                win_type = "spike_detonated"
                            elif "defuse" in img_src:
                                win_type = "spike_defused"
                            elif "time" in img_src:
                                win_type = "time_expired"
                
                game_rounds.append({
                    "tournament_id": tournament_id,
                    "match_id": match_id,
                    "game_id": game_id,
                    "map": map_name,
                    "round_number": round_number,
                    "team1_name": round_team1,
                    "team2_name": round_team2,
                    "team1_score": team1_score,
                    "team2_score": team2_score,
                    "winner_team": winner_team,
                    "side": side,
                    "win_type": win_type
                })
            
            session.close()
            return game_rounds, (tournament_id, match_id, game_id)
            
        except Exception as e:
            session.close()
            return [], None
    
    # STREAMING APPROACH: Process unprocessed matches in chunks
    logger.info("Starting streaming scrape process...")
    total_games_scraped = 0
    games_buffer = []
    session = get_session_with_retries()
    
    # Process matches in chunks to avoid memory issues
    chunk_size = 500  # Process 500 matches at a time
    num_chunks = (remaining_matches + chunk_size - 1) // chunk_size
    
    for chunk_idx in range(num_chunks):
        chunk_start = chunk_idx * chunk_size
        chunk_end = min(chunk_start + chunk_size, remaining_matches)
        matches_chunk = unprocessed_matches.iloc[chunk_start:chunk_end]
        
        logger.info(f"\n[Chunk {chunk_idx+1}/{num_chunks}] Processing matches {chunk_start+1}-{chunk_end} of {remaining_matches}")
        
        # Collect games from this chunk only
        for idx, row in matches_chunk.iterrows():
            tournament_id = str(row['tournament_id'])
            match_id = str(row['match_id'])
            match_url = row['match_url']
            team1_name = row['team1_name']
            team2_name = row['team2_name']
            
            try:
                time.sleep(delay)
                response = session.get(match_url, headers=headers, timeout=20)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, "html.parser")
                
                game_nav_items = soup.find_all("div", class_="vm-stats-gamesnav-item")
                map_index = 1
                
                for nav_item in game_nav_items:
                    game_id = nav_item.get("data-game-id", "")
                    disabled = nav_item.get("data-disabled", "0")
                    
                    if game_id == "all" or disabled == "1":
                        continue
                    
                    map_name = None
                    map_div = nav_item.find("div", style=re.compile("margin-bottom"))
                    if map_div:
                        map_text = map_div.get_text(strip=True)
                        map_name = re.sub(r'^\d+', '', map_text)
                    
                    game_url = f"{match_url}?map={map_index}"
                    games_buffer.append((tournament_id, match_id, game_id, map_name, game_url, team1_name, team2_name))
                    map_index += 1
                    
                    # Process batch when buffer is full
                    if len(games_buffer) >= batch_size:
                        total_games_scraped += process_batch(
                            games_buffer, 
                            scrape_game, 
                            output_path, 
                            temp_path, 
                            max_workers, 
                            total_games_scraped
                        )
                        games_buffer = []
                        gc.collect()  # Force garbage collection
                
            except Exception as e:
                logger.error(f"Error processing match {match_id}: {e}")
                continue
        
        logger.info(f"[Chunk {chunk_idx+1}/{num_chunks}] Completed. Total scraped so far: {total_games_scraped}")
    
    # Process remaining games in buffer
    if games_buffer:
        total_games_scraped += process_batch(
            games_buffer, 
            scrape_game, 
            output_path, 
            temp_path, 
            max_workers, 
            total_games_scraped
        )
    
    session.close()
    logger.info(f"\n{'='*60}")
    logger.info(f"✓ Scraping completed! Total games processed: {total_games_scraped}")
    logger.info(f"{'='*60}")

def process_batch(games_batch, scrape_func, output_path, temp_path, max_workers, total_so_far):
    """Process a batch of games and save results"""
    logger.info(f"Processing batch of {len(games_batch)} games...")
    start_time = time.time()
    
    batch_rounds = []
    batch_game_ids = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(scrape_func, game_data): game_data for game_data in games_batch}
        
        for future in as_completed(futures):
            try:
                rounds, game_id_tuple = future.result()
                if rounds and game_id_tuple:
                    batch_rounds.extend(rounds)
                    batch_game_ids.append(game_id_tuple)
            except Exception as e:
                logger.error(f"Future error: {e}")
    
    elapsed = time.time() - start_time
    
    if batch_rounds:
        rounds_df = pd.DataFrame(batch_rounds)
        
        save_to_csv(rounds_df, output_path)
        
        del rounds_df
        
        if batch_game_ids:
            new_temp_df = pd.DataFrame(batch_game_ids, columns=['tournament_id', 'match_id', 'game_id'])
            
            save_to_csv(new_temp_df, temp_path)
            
            del new_temp_df
        
        logger.info(f"✓ Saved {len(batch_rounds)} rounds from {len(batch_game_ids)} games")
        logger.info(f"Time: {elapsed:.1f}s | Total progress: {total_so_far + len(batch_game_ids)}")
        
        gc.collect()
        return len(batch_game_ids)
    
    return 0