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
def scrape_tournaments_match_overview_data(delay=0.4, max_workers=8, batch_size=50):
    """
    Scrape match overview player statistics from VLR.gg.
    Memory-optimized with streaming processing for 24k+ games.
    Only processes matches not in temp checkpoint file.
    
    Args:
        delay: Delay between requests (0.4s for stability)
        max_workers: Number of concurrent threads (8 for balance)
        batch_size: Save every N games (50 for memory management)
    """
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    matches_path = "/opt/airflow/valorant_scraper/data/raw/tournaments_match.csv"
    output_path = "/opt/airflow/valorant_scraper/data/raw/tournaments_match_overview.csv"
    temp_path = "/opt/airflow/valorant_scraper/data/temp/tournaments_match_overview.csv"
    
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
        tournament_id, match_id, game_id, map_name, game_url = game_data
        session = get_session_with_retries()
        
        try:
            time.sleep(delay)
            game_response = session.get(game_url, headers=headers, timeout=20)
            game_response.raise_for_status()
            game_soup = BeautifulSoup(game_response.content, "html.parser")
            
            stat_tables = game_soup.find_all("table", class_="wf-table-inset mod-overview")
            game_stats = []
            
            for table in stat_tables:
                tbody = table.find("tbody")
                if not tbody:
                    continue
                
                rows = tbody.find_all("tr")
                
                for row in rows:
                    try:
                        player_cell = row.find("td", class_="mod-player")
                        if not player_cell:
                            continue
                        
                        player_link = player_cell.find("a")
                        if not player_link:
                            continue
                        
                        player_href = player_link.get("href", "")
                        player_id = None
                        if player_href:
                            if m := re.search(r'/player/(\d+)/', player_href):
                                player_id = m.group(1)
                        
                        team_tag_div = player_cell.find("div", class_="ge-text-light")
                        team_name = team_tag_div.get_text(strip=True) if team_tag_div else None
                        
                        agent_cell = row.find("td", class_="mod-agents")
                        agent = None
                        if agent_cell:
                            agent_img = agent_cell.find("img")
                            if agent_img:
                                agent = agent_img.get("alt", "").capitalize()
                        
                        stat_cells = row.find_all("td", class_="mod-stat")
                        
                        def extract_stat(cell, default=None):
                            if not cell:
                                return default
                            both_span = cell.find("span", class_=re.compile("mod-both|side mod-both"))
                            if both_span:
                                text = both_span.get_text(strip=True)
                                text = text.replace('%', '').replace('+', '').replace('−', '-').replace('–', '-')
                                try:
                                    return float(text) if '.' in text else int(text)
                                except:
                                    return default
                            return default
                        
                        game_stats.append({
                            "tournament_id": tournament_id,
                            "match_id": match_id,
                            "team_name": team_name,
                            "game_id": game_id,
                            "map": map_name,
                            "player_id": player_id,
                            "agent": agent,
                            "rating": extract_stat(stat_cells[0]) if len(stat_cells) > 0 else None,
                            "acs": extract_stat(stat_cells[1]) if len(stat_cells) > 1 else None,
                            "kill": extract_stat(stat_cells[2]) if len(stat_cells) > 2 else None,
                            "death": extract_stat(stat_cells[3]) if len(stat_cells) > 3 else None,
                            "assist": extract_stat(stat_cells[4]) if len(stat_cells) > 4 else None,
                            "kd": extract_stat(stat_cells[5]) if len(stat_cells) > 5 else None,
                            "kast": extract_stat(stat_cells[6]) if len(stat_cells) > 6 else None,
                            "adr": extract_stat(stat_cells[7]) if len(stat_cells) > 7 else None,
                            "hs": extract_stat(stat_cells[8]) if len(stat_cells) > 8 else None,
                            "fk": extract_stat(stat_cells[9]) if len(stat_cells) > 9 else None,
                            "fd": extract_stat(stat_cells[10]) if len(stat_cells) > 10 else None,
                            "fkfd": extract_stat(stat_cells[11]) if len(stat_cells) > 11 else None
                        })
                        
                    except Exception as e:
                        logger.warning(f"Error parsing player row: {e}")
                        continue
            
            session.close()
            return game_stats, (tournament_id, match_id, game_id)
            
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
                    games_buffer.append((tournament_id, match_id, game_id, map_name, game_url))
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
    
    batch_stats = []
    batch_game_ids = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(scrape_func, game_data): game_data for game_data in games_batch}
        
        for future in as_completed(futures):
            try:
                stats, game_id_tuple = future.result()
                if stats and game_id_tuple:
                    batch_stats.extend(stats)
                    batch_game_ids.append(game_id_tuple)
            except Exception as e:
                logger.error(f"Future error: {e}")
    
    elapsed = time.time() - start_time
    
    if batch_stats:
        stats_df = pd.DataFrame(batch_stats)
        
        save_to_csv(stats_df, output_path)
        
        del stats_df
        
        if batch_game_ids:
            new_temp_df = pd.DataFrame(batch_game_ids, columns=['tournament_id', 'match_id', 'game_id'])
            
            save_to_csv(new_temp_df, temp_path)
            
            del new_temp_df
        
        logger.info(f"✓ Saved {len(batch_stats)} stats from {len(batch_game_ids)} games")
        logger.info(f"Time: {elapsed:.1f}s | Total progress: {total_so_far + len(batch_game_ids)}")
        
        gc.collect()
        return len(batch_game_ids)
    
    return 0