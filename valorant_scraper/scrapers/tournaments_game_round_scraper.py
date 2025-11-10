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
def scrape_tournaments_game_round_data(delay=1.0, max_workers=5, batch_size=50):
    """
    Scrape round-by-round results from VLR.gg match pages.
    Memory-optimized with streaming processing for 24k+ games.
    Only processes games not in temp checkpoint file.
    
    Args:
        delay: Delay between requests (0.3s = ~200 req/min)
        max_workers: Number of concurrent threads (10 for speed)
        batch_size: Save every N games (50 for memory management)
    """
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    games_path = "/opt/airflow/valorant_scraper/data/raw/tournaments_game.csv"
    output_path = "/opt/airflow/valorant_scraper/data/raw/tournaments_game_round.csv"
    temp_path = "/opt/airflow/valorant_scraper/data/temp/tournaments_game_round.csv"
    
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
        """Helper function to scrape a single game's round data"""
        tournament_id, match_id, game_id, game_url, team1_id, team2_id = game_data
        session = get_session_with_retries()
        
        try:
            time.sleep(delay)
            logger.debug(f"Scraping rounds for game {game_id} from URL: {game_url}")
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
            
            # Find rounds section within THIS specific game section
            rounds_section = target_game_section.find("div", class_="vlr-rounds")
            if not rounds_section:
                logger.warning(f"No rounds section found for game {game_id}")
                session.close()
                return [], (tournament_id, match_id, game_id)
            
            # Extract team names from the rounds section to identify which is team1 and team2
            team_divs = rounds_section.find_all("div", class_="team")
            round_team1_name = None
            round_team2_name = None
            
            if len(team_divs) >= 2:
                round_team1_name = team_divs[0].get_text(strip=True)
                round_team2_name = team_divs[1].get_text(strip=True)
            
            # Find all round columns
            round_cols = rounds_section.find_all("div", class_="vlr-rounds-row-col")
            game_rounds = []
            
            for round_col in round_cols:
                # Skip spacing columns
                if "mod-spacing" in round_col.get("class", []):
                    continue
                
                # Get round number
                round_num_div = round_col.find("div", class_="rnd-num")
                if not round_num_div:
                    continue
                
                round_number = round_num_div.get_text(strip=True)
                
                # Get scores from title attribute (e.g., "3-2")
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
                
                # Find round result squares
                round_squares = round_col.find_all("div", class_="rnd-sq")
                winner_team_id = None
                win_type = None
                side = None
                
                # Process each square to find the winner
                for sq_idx, sq in enumerate(round_squares):
                    if "mod-win" in sq.get("class", []):
                        # Determine which team won based on position
                        # First square (index 0) = team1, Second square (index 1) = team2
                        if sq_idx == 0:
                            winner_team_id = team1_id
                        else:
                            winner_team_id = team2_id
                        
                        # Determine side (attack/defense)
                        if "mod-t" in sq.get("class", []):
                            side = "attack"
                        elif "mod-ct" in sq.get("class", []):
                            side = "defense"
                        
                        # Determine win type from icon
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
                        
                        break  # Found winner, no need to check other squares
                
                game_rounds.append({
                    "tournament_id": tournament_id,
                    "match_id": match_id,
                    "game_id": game_id,
                    "round_number": round_number,
                    "team1_id": team1_id,
                    "team2_id": team2_id,
                    "team1_score": team1_score,
                    "team2_score": team2_score,
                    "winner_team": winner_team_id,
                    "side": side,
                    "win_type": win_type
                })
            
            session.close()
            return game_rounds, (tournament_id, match_id, game_id)
            
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
            
            # Ensure URL doesn't have tab parameter (we just need the base game URL)
            # Remove any existing tab parameter
            if '&tab=' in game_url:
                game_url = game_url.split('&tab=')[0]
            
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
        
        # Ensure column order matches requirements
        column_order = [
            "tournament_id", "match_id", "game_id", "round_number", 
            "team1_id", "team2_id", "team1_score", "team2_score", 
            "winner_team", "side", "win_type"
        ]
        rounds_df = rounds_df[column_order]
        
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