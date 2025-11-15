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
def scrape_tournaments_game_economy_team_data(delay=1.0, max_workers=5, batch_size=50):
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
    output_path = "/opt/airflow/valorant_scraper/data/raw/tournaments_game_economy_team.csv"
    temp_path = "/opt/airflow/valorant_scraper/data/temp/tournaments_game_economy_team.csv"
    
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
        """Scrape economy data from a single game."""
        tournament_id, match_id, game_id, game_url, team1_id, team2_id = game_data
        session = get_session_with_retries()
        
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
                session.close()
                return [], (tournament_id, match_id, game_id)
            
            # Find economy table (mod-econ)
            econ_table = None
            all_tables = target_game_section.find_all("table")
            
            logger.info(f"Game {game_id}: Checking {len(all_tables)} tables")
            
            for idx, table in enumerate(all_tables):
                table_classes = table.get('class', [])
                logger.info(f"  Table {idx}: {table_classes}")
                
                if 'mod-econ' in table_classes:
                    econ_table = table
                    logger.info(f"    ✓ Selected this table for Economy data")
                    break
            
            if not econ_table:
                logger.info(f"Game {game_id}: No economy table found")
                session.close()
                return [], (tournament_id, match_id, game_id)
            
            # Try to find tbody, if not found use table directly
            tbody = econ_table.find("tbody")
            if tbody:
                rows = tbody.find_all("tr")
            else:
                rows = econ_table.find_all("tr")
            
            # Need at least 3 rows: header + 2 teams
            if len(rows) < 3:
                logger.info(f"Game {game_id}: Not enough rows ({len(rows)})")
                session.close()
                return [], (tournament_id, match_id, game_id)
            
            logger.info(f"Game {game_id}: Found {len(rows)} rows in economy table")
            
            # Extract data from team rows (skip header row)
            team1_row = rows[1]
            team2_row = rows[2]
            
            def parse_stat_cell(cell):
                """Parse a stats cell to extract total and wins."""
                text = cell.get_text(strip=True)
                # Format: "total (wins)" or just "total" for pistol
                if '(' in text:
                    parts = text.split('(')
                    total = int(parts[0].strip())
                    wins = int(parts[1].rstrip(')').strip())
                    loses = total - wins
                    return wins, loses
                else:
                    # Pistol rounds - just return the number
                    return int(text), None
            
            # Parse Team1 data
            team1_cells = team1_row.find_all("td")[1:]  # Skip first cell (team name)
            pistol1 = parse_stat_cell(team1_cells[0])[0]
            eco1_win, eco1_lose = parse_stat_cell(team1_cells[1])
            semi_eco1_win, semi_eco1_lose = parse_stat_cell(team1_cells[2])
            semi_buy1_win, semi_buy1_lose = parse_stat_cell(team1_cells[3])
            full_buy1_win, full_buy1_lose = parse_stat_cell(team1_cells[4])
            
            # Parse Team2 data
            team2_cells = team2_row.find_all("td")[1:]  # Skip first cell (team name)
            pistol2 = parse_stat_cell(team2_cells[0])[0]
            eco2_win, eco2_lose = parse_stat_cell(team2_cells[1])
            semi_eco2_win, semi_eco2_lose = parse_stat_cell(team2_cells[2])
            semi_buy2_win, semi_buy2_lose = parse_stat_cell(team2_cells[3])
            full_buy2_win, full_buy2_lose = parse_stat_cell(team2_cells[4])
            
            game_stat = {
                "tournament_id": tournament_id,
                "match_id": match_id,
                "game_id": game_id,
                "team1_id": team1_id,
                "team2_id": team2_id,
                "pistol1": pistol1,
                "pistol2": pistol2,
                "eco1_win": eco1_win,
                "eco1_lose": eco1_lose,
                "eco2_win": eco2_win,
                "eco2_lose": eco2_lose,
                "semi_eco1_win": semi_eco1_win,
                "semi_eco1_lose": semi_eco1_lose,
                "semi_eco2_win": semi_eco2_win,
                "semi_eco2_lose": semi_eco2_lose,
                "semi_buy1_win": semi_buy1_win,
                "semi_buy1_lose": semi_buy1_lose,
                "semi_buy2_win": semi_buy2_win,
                "semi_buy2_lose": semi_buy2_lose,
                "full_buy1_win": full_buy1_win,
                "full_buy1_lose": full_buy1_lose,
                "full_buy2_win": full_buy2_win,
                "full_buy2_lose": full_buy2_lose
            }
            
            logger.info(f"Game {game_id}: Extracted economy data")
            session.close()
            return [game_stat], (tournament_id, match_id, game_id)
            
        except Exception as e:
            logger.warning(f"Error scraping game {game_id}: {e}")
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
            "tournament_id", "match_id", "game_id", "team1_id", "team2_id", "pistol1", "pistol2", "eco1_win", "eco1_lose", "eco2_win", "eco2_lose", 
            "semi_eco1_win", "semi_eco1_lose", "semi_eco2_win", "semi_eco2_lose", "semi_buy1_win", "semi_buy1_lose", "semi_buy2_win", "semi_buy2_lose", 
            "full_buy1_win", "full_buy1_lose", "full_buy2_win", "full_buy2_lose"
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
            
            logger.info(f"✓ Processed {len(batch_game_ids)} games with {len(batch_stats)} H2H matchups")
            logger.info(f"Time: {elapsed:.1f}s | Total progress: {total_so_far + len(batch_game_ids)}")
            
            gc.collect()
            return len(batch_game_ids)
    else:
        logger.warning(f"No games to save in checkpoint")
    
    gc.collect()
    return games_processed