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
logger.setLevel(logging.DEBUG)
logging.getLogger().setLevel(logging.DEBUG)

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
def scrape_tournaments_game_overview_data(delay=1.0, max_workers=5, batch_size=50):
    """
    Scrape match overview player statistics from VLR.gg.
    Memory-optimized with streaming processing for 24k+ games.
    Only processes games not in temp checkpoint file.
    
    Args:
        delay: Delay between requests (0.3s for stability)
        max_workers: Number of concurrent threads (10 for balance)
        batch_size: Save every N games (50 for memory management)
    """
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    games_path = "/opt/airflow/valorant_scraper/data/raw/tournaments_game.csv"
    output_path = "/opt/airflow/valorant_scraper/data/raw/tournaments_game_overview.csv"
    temp_path = "/opt/airflow/valorant_scraper/data/temp/tournaments_game_overview.csv"
    
    os.makedirs(os.path.dirname(temp_path), exist_ok=True)
    
    # Load processed games
    processed_games = set()
    if os.path.exists(temp_path):
        try:
            temp_df = load_from_csv(temp_path)
            # Create composite key of tournament_id-match_id-game_id
            processed_games = set(
                temp_df['tournament_id'].astype(str) + '-' + 
                temp_df['match_id'].astype(str) + '-' + 
                temp_df['game_id'].astype(str)
            )
            logger.info(f"Loaded {len(processed_games)} already processed games.")
            del temp_df
            gc.collect()
        except Exception as e:
            logger.warning(f"Could not load temp file: {e}")
    
    if not os.path.exists(games_path):
        logger.error(f"Error: {games_path} not found.")
        return
    
    # Load games data
    games_df = load_from_csv(games_path)
    games_df['tournament_id'] = games_df['tournament_id'].astype(str)
    games_df['match_id'] = games_df['match_id'].astype(str)
    games_df['game_id'] = games_df['game_id'].astype(str)
    
    # Create composite key for filtering
    games_df['composite_key'] = (
        games_df['tournament_id'] + '-' + 
        games_df['match_id'] + '-' + 
        games_df['game_id']
    )
    
    # Filter to only unprocessed games
    unprocessed_games = games_df[~games_df['composite_key'].isin(processed_games)]
    
    total_games = len(games_df)
    remaining_games = len(unprocessed_games)
    
    logger.info(f"Total games: {total_games}")
    logger.info(f"Already processed: {len(processed_games)}")
    logger.info(f"Remaining to process: {remaining_games}")
    
    if remaining_games == 0:
        logger.info("No new games to process!")
        return
    
    def scrape_game(game_data):
        """Helper function to scrape a single game's player stats"""
        tournament_id, match_id, game_id, game_url, team1_id, team2_id = game_data
        session = get_session_with_retries()
        
        try:
            time.sleep(delay)
            logger.debug(f"Scraping game {game_id} from URL: {game_url}")
            game_response = session.get(game_url, headers=headers, timeout=20)
            game_response.raise_for_status()
            game_soup = BeautifulSoup(game_response.content, "html.parser")
            
            # Find the correct game section for THIS specific game_id
            all_game_sections = game_soup.find_all("div", class_="vm-stats-game")
            
            target_game_section = None
            for game_section in all_game_sections:
                # Check if this section's data-game-id matches
                if game_section.get("data-game-id") == game_id:
                    target_game_section = game_section
                    break
            
            # If not found by data attribute, and there's only one non-"all" section, use it
            if not target_game_section:
                for game_section in all_game_sections:
                    if game_section.get("data-game-id") != "all":
                        target_game_section = game_section
                        break
            
            if not target_game_section:
                logger.warning(f"No game section found for game {game_id}")
                session.close()
                return [], (tournament_id, match_id, game_id)
            
            # Find stat tables within THIS specific game section
            stat_tables = target_game_section.find_all("table", class_="wf-table-inset mod-overview")
            
            if len(stat_tables) < 2:
                logger.warning(f"Expected 2 stat tables for game {game_id}, found {len(stat_tables)}")
                session.close()
                return [], (tournament_id, match_id, game_id)
            
            game_stats = []
            
            # Process both teams' tables
            # First table = team1 (team1_id), Second table = team2 (team2_id)
            team_ids = [team1_id, team2_id]
            
            for table_idx, table in enumerate(stat_tables[:2]):  # Only process first 2 tables
                team_id = team_ids[table_idx]
                
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
                        
                        # Extract agent
                        agent_cell = row.find("td", class_="mod-agents")
                        agent = None
                        if agent_cell:
                            agent_img = agent_cell.find("img")
                            if agent_img:
                                agent = agent_img.get("alt", "").capitalize()
                        
                        # Extract stats
                        stat_cells = row.find_all("td", class_="mod-stat")
                        
                        def extract_stat(cell, default=None):
                            if not cell:
                                return default
                            both_span = cell.find("span", class_=re.compile(r"mod-both|side mod-both"))
                            if both_span:
                                text = both_span.get_text(strip=True)
                                # Handle non-breaking spaces and special characters
                                text = text.replace('\xa0', '').replace('&nbsp;', '').strip()
                                # Skip if empty after cleaning
                                if not text:
                                    return default
                                # Remove percentage, plus, minus signs
                                text = text.replace('%', '').replace('+', '').replace('−', '-').replace('–', '-')
                                try:
                                    return float(text) if '.' in text else int(text)
                                except:
                                    return default
                            return default
                        
                        game_stats.append({
                            "tournament_id": tournament_id,
                            "match_id": match_id,
                            "game_id": game_id,
                            "team_id": team_id,
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
                        logger.warning(f"Error parsing player row in game {game_id}: {e}")
                        continue
            
            session.close()
            return game_stats, (tournament_id, match_id, game_id)
            
        except Exception as e:
            logger.warning(f"Error scraping game {game_id}: {e}")
            session.close()
            return [], None
    
    # STREAMING APPROACH: Process unprocessed games in chunks
    logger.info("Starting streaming scrape process...")
    total_games_scraped = 0
    games_buffer = []
    
    # Process games in chunks to avoid memory issues
    chunk_size = 500  # Process 500 games at a time
    num_chunks = (remaining_games + chunk_size - 1) // chunk_size
    
    for chunk_idx in range(num_chunks):
        chunk_start = chunk_idx * chunk_size
        chunk_end = min(chunk_start + chunk_size, remaining_games)
        games_chunk = unprocessed_games.iloc[chunk_start:chunk_end]
        
        logger.info(f"\n[Chunk {chunk_idx+1}/{num_chunks}] Processing games {chunk_start+1}-{chunk_end} of {remaining_games}")
        
        # Collect games from this chunk
        for idx, row in games_chunk.iterrows():
            tournament_id = str(row['tournament_id'])
            match_id = str(row['match_id'])
            game_id = str(row['game_id'])
            game_url = row['game_url']
            team1_id = str(row['team1_id'])
            team2_id = str(row['team2_id'])
            
            # Add &tab=overview if not already present
            if '?' in game_url:
                if 'tab=' not in game_url:
                    game_url = f"{game_url}&tab=overview"
            else:
                game_url = f"{game_url}?tab=overview"
            
            games_buffer.append((tournament_id, match_id, game_id, game_url, team1_id, team2_id))
            
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
        
        # Process remaining games in buffer after chunk completes
        if games_buffer:
            total_games_scraped += process_batch(
                games_buffer, 
                scrape_game, 
                output_path, 
                temp_path, 
                max_workers, 
                total_games_scraped
            )
            games_buffer = []
            gc.collect()
        
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
        
        # Ensure column order matches requirements
        column_order = [
            "tournament_id", "match_id", "game_id", "team_id", "player_id", 
            "agent", "rating", "acs", "kill", "death", "assist", "kd", 
            "kast", "adr", "hs", "fk", "fd", "fkfd"
        ]
        stats_df = stats_df[column_order]
        
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