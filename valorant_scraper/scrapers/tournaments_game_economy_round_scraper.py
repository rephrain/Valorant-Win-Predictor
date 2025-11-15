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
def scrape_tournaments_game_economy_round_data(delay=1.0, max_workers=5, batch_size=50):
    """
    Scrape head-to-head matchup statistics from VLR.gg.
    Memory-optimized with streaming processing for 24k+ games.
    Only processes games not in temp checkpoint file.
    
    Args:
        delay: Delay between requests (1.0s minimum to avoid rate limits)
        max_workers: Number of concurrent threads (5 for safe scraping)
        batch_size: Save every N games (50 for memory management)
    """
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    games_path = "/opt/airflow/valorant_scraper/data/raw/tournaments_game.csv"
    output_path = "/opt/airflow/valorant_scraper/data/raw/tournaments_game_economy_round.csv"
    temp_path = "/opt/airflow/valorant_scraper/data/temp/tournaments_game_economy_round.csv"
    
    os.makedirs(os.path.dirname(temp_path), exist_ok=True)
    
    # Load processed games from checkpoint
    processed_games = set()
    if os.path.exists(temp_path):
        try:
            temp_df = load_from_csv(temp_path)
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
        """Scrape round-by-round economy data from a single game."""
        tournament_id, match_id, game_id, game_url, team1_id, team2_id = game_data
        session = get_session_with_retries()
        
        def parse_bank_value(bank_str):
            """Convert bank string (e.g., '2.4k') to integer (e.g., 2400)."""
            bank_str = bank_str.strip().lower()
            if 'k' in bank_str:
                # Remove 'k' and convert to float, then multiply by 1000
                try:
                    value = float(bank_str.replace('k', ''))
                    return int(value * 1000)
                except ValueError:
                    logger.warning(f"Could not parse bank value: {bank_str}")
                    return 0
            else:
                # No 'k', just return the integer value
                try:
                    return int(bank_str)
                except ValueError:
                    logger.warning(f"Could not parse bank value: {bank_str}")
                    return 0
        
        try:
            time.sleep(delay)
            game_response = session.get(game_url, headers=headers, timeout=20)
            game_response.raise_for_status()
            game_soup = BeautifulSoup(game_response.content, "html.parser")
            
            # Find the correct game section
            all_game_sections = game_soup.find_all("div", class_="vm-stats-game")
            target_game_section = None
            
            for game_section in all_game_sections:
                if game_section.get("data-game-id") == game_id:
                    target_game_section = game_section
                    break
            
            if not target_game_section:
                for game_section in all_game_sections:
                    if game_section.get("data-game-id") != "all":
                        target_game_section = game_section
                        break
            
            if not target_game_section:
                logger.info(f"Game {game_id}: No game section found, searching entire page")
                target_game_section = game_soup
            
            # Find round economy table with round-num divs
            round_econ_table = None
            all_tables = target_game_section.find_all("table", class_="mod-econ")
            
            logger.info(f"Game {game_id}: Found {len(all_tables)} tables with mod-econ class")
            
            # Look for the table with round-num divs (round-by-round data, not summary)
            for idx, table in enumerate(all_tables):
                round_nums = table.find_all("div", class_="round-num")
                logger.info(f"  Table {idx}: {len(round_nums)} round-num divs found")
                if round_nums:
                    round_econ_table = table
                    logger.info(f"    ✓ Selected table {idx} for round economy data ({len(round_nums)} rounds)")
                    break
            
            if not round_econ_table:
                logger.info(f"Game {game_id}: No round economy table found")
                session.close()
                return [], (tournament_id, match_id, game_id)
            
            # Get all rows
            tbody = round_econ_table.find("tbody")
            if tbody:
                rows = tbody.find_all("tr")
            else:
                rows = round_econ_table.find_all("tr")
            
            if not rows:
                logger.info(f"Game {game_id}: No rows found in round economy table")
                session.close()
                return [], (tournament_id, match_id, game_id)
            
            logger.info(f"Game {game_id}: Found {len(rows)} rows in round economy table")
            
            game_rounds = []
            
            def get_econ_type(rnd_sq_div, is_pistol_round):
                """Determine economy type from rnd-sq div."""
                if is_pistol_round:
                    return "pistol"
                
                text = rnd_sq_div.get_text(strip=True)
                if text == "$$$":
                    return "full_buy"
                elif text == "$$":
                    return "semi_buy"
                elif text == "$":
                    return "semi_eco"
                else:
                    return "eco"
            
            # Track which team is in which row position
            # In overtime, rows alternate: team1, team2, team1, team2, etc.
            team_row_mapping = {}  # Maps row_idx to team_id
            
            # Process each row
            for row_idx, row in enumerate(rows):
                cells = row.find_all("td")
                logger.info(f"  Row {row_idx}: {len(cells)} cells")
                
                # Determine which team this row belongs to
                # For standard games (rows 0-1): row 0 = team1, row 1 = team2
                # For overtime (rows 2+): pattern continues alternating
                if row_idx % 2 == 0:
                    current_team_id = team1_id
                    other_team_id = team2_id
                    team_position = 1  # This team's data is in position 1 of rnd-sq
                else:
                    current_team_id = team2_id
                    other_team_id = team1_id
                    team_position = 2  # This team's data is in position 2 of rnd-sq
                
                team_row_mapping[row_idx] = current_team_id
                
                # Skip first cell (team names/labels)
                for cell_idx, cell in enumerate(cells[1:], start=1):
                    # Get round number
                    round_num_div = cell.find("div", class_="round-num")
                    if not round_num_div:
                        continue
                    
                    round_num = int(round_num_div.get_text(strip=True))
                    
                    # Determine if this is a pistol round (round 1 or 13)
                    is_pistol_round = (round_num == 1 or round_num == 13)
                    
                    # Get ALL bank divs in this cell (should be 2: one before rnd-sq, one after)
                    banks = cell.find_all("div", class_="bank")
                    if len(banks) < 2:
                        logger.warning(f"    Round {round_num}: Only {len(banks)} banks found, expected 2")
                        continue
                    
                    bank1_str = banks[0].get_text(strip=True)
                    bank2_str = banks[-1].get_text(strip=True)
                    
                    # Convert bank values from 'k' format to integers
                    bank1 = parse_bank_value(bank1_str)
                    bank2 = parse_bank_value(bank2_str)
                    
                    # Get rnd-sq divs (2 per cell: team1 and team2)
                    rnd_sqs = cell.find_all("div", class_="rnd-sq")
                    if len(rnd_sqs) < 2:
                        logger.warning(f"    Round {round_num}: Only {len(rnd_sqs)} rnd-sq divs found")
                        continue
                    
                    team1_div = rnd_sqs[0]
                    team2_div = rnd_sqs[1]
                    
                    # Get loadout values from title attribute
                    loadout1 = team1_div.get("title", "0")
                    loadout2 = team2_div.get("title", "0")
                    
                    # Get economy types
                    econ1 = get_econ_type(team1_div, is_pistol_round)
                    econ2 = get_econ_type(team2_div, is_pistol_round)
                    
                    # Determine winner (check for mod-win class)
                    winner_team = None
                    team1_classes = team1_div.get("class", [])
                    team2_classes = team2_div.get("class", [])
                    
                    if "mod-win" in team1_classes:
                        winner_team = team1_id
                    elif "mod-win" in team2_classes:
                        winner_team = team2_id
                    
                    # Check if this round already exists in game_rounds
                    # This handles the case where multiple rows contain the same round data
                    existing_round = next((r for r in game_rounds if r["round"] == round_num), None)
                    
                    if not existing_round:
                        game_rounds.append({
                            "tournament_id": tournament_id,
                            "match_id": match_id,
                            "game_id": game_id,
                            "team1_id": team1_id,
                            "team2_id": team2_id,
                            "round": round_num,
                            "bank1": bank1,
                            "bank2": bank2,
                            "loadout1": loadout1,
                            "loadout2": loadout2,
                            "econ1": econ1,
                            "econ2": econ2,
                            "winner_team": winner_team
                        })
            
            logger.info(f"Game {game_id}: Extracted {len(game_rounds)} round records")
            session.close()
            return game_rounds, (tournament_id, match_id, game_id)
            
        except Exception as e:
            logger.warning(f"Error scraping game rounds {game_id}: {e}")
            session.close()
            return [], None
    
    # Streaming approach: Process unprocessed games in chunks
    logger.info("Starting streaming scrape process...")
    total_games_scraped = 0
    games_buffer = []
    
    # Process games in chunks to avoid memory issues
    chunk_size = 500
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
            
            # Add &tab=economy if not already present
            if '?' in game_url:
                if 'tab=' not in game_url:
                    game_url = f"{game_url}&tab=economy"
                else:
                    game_url = re.sub(r'tab=[^&]*', 'tab=economy', game_url)
            else:
                game_url = f"{game_url}?tab=economy"
            
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
                gc.collect()
        
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
                if game_id_tuple:
                    batch_game_ids.append(game_id_tuple)
                    if stats:
                        batch_stats.extend(stats)
            except Exception as e:
                logger.error(f"Future error: {e}")
    
    elapsed = time.time() - start_time
    games_processed = len(batch_game_ids)
    
    logger.info(f"Batch complete: {games_processed} games processed, {len(batch_stats)} matchup records extracted")
    
    # Save stats if any exist
    if batch_stats:
        stats_df = pd.DataFrame(batch_stats)
        
        # Ensure column order matches requirements
        column_order = [
            "tournament_id", "match_id", "game_id", "team1_id", "team2_id", "round", 
            "bank1", "bank2", "loadout1", "loadout2", "econ1", "econ2", "winner_team"
        ]
        stats_df = stats_df[column_order]
        
        logger.info(f"Saving {len(batch_stats)} matchup records to {output_path}")
        save_to_csv(stats_df, output_path)
        logger.info(f"✓ Successfully saved matchup records to CSV")
        
        del stats_df
    
        # Always save checkpoint for processed games (even if no stats extracted)
        if batch_game_ids:
            new_temp_df = pd.DataFrame(batch_game_ids, columns=['tournament_id', 'match_id', 'game_id'])
            
            logger.info(f"Saving checkpoint for {len(batch_game_ids)} games to {temp_path}")
            save_to_csv(new_temp_df, temp_path)
            logger.info(f"✓ Successfully saved checkpoint")
            
            del new_temp_df
            
            logger.info(f"✓ Processed {len(batch_game_ids)} games with {len(batch_stats)} economy round matchups")
            logger.info(f"Time: {elapsed:.1f}s | Total progress: {total_so_far + len(batch_game_ids)}")
            
            gc.collect()
            return len(batch_game_ids)
    else:
        logger.warning(f"No games to save in checkpoint")
    
    gc.collect()
    return games_processed