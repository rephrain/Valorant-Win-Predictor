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
import json

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
def scrape_tournaments_game_performance_data(delay=1.0, max_workers=5, batch_size=50):
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
    output_path = "/opt/airflow/valorant_scraper/data/raw/tournaments_game_performance.csv"
    temp_path = "/opt/airflow/valorant_scraper/data/temp/tournaments_game_performance.csv"
    
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
        """Scrape advanced stats (multikills, clutches, etc.) from a single game."""
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
            
            # Find advanced stats table with mod-adv-stats class
            adv_stats_table = None
            all_tables = target_game_section.find_all("table")
            
            logger.info(f"Game {game_id}: Checking {len(all_tables)} tables for advanced stats")
            
            for idx, table in enumerate(all_tables):
                table_classes = table.get('class', [])
                if 'mod-adv-stats' in table_classes:
                    adv_stats_table = table
                    logger.info(f"  ✓ Found advanced stats table at index {idx}")
                    break
            
            if not adv_stats_table:
                logger.info(f"Game {game_id}: No advanced stats table found")
                session.close()
                return [], (tournament_id, match_id, game_id)
            
            # Get tbody or rows directly from table
            tbody = adv_stats_table.find("tbody")
            if tbody:
                rows = tbody.find_all("tr")
            else:
                rows = adv_stats_table.find_all("tr")
            
            # Skip first row (header row)
            data_rows = rows[1:] if len(rows) > 1 else []
            
            if len(data_rows) < 10:
                logger.info(f"Game {game_id}: Not enough data rows ({len(data_rows)}), expected 10")
                session.close()
                return [], (tournament_id, match_id, game_id)
            
            logger.info(f"Game {game_id}: Found {len(data_rows)} player rows in advanced stats table")
            
            game_stats = []
            
            # Process rows: rows 0-4 are team1, rows 5-9 are team2
            for row_idx, row in enumerate(data_rows[:10]):  # Only process first 10 rows
                cells = row.find_all("td")
                
                if len(cells) < 14:  # Need all columns: player, agent, 2k-5k, 1v1-1v5, econ, pl, de
                    logger.warning(f"Game {game_id}: Row {row_idx} has insufficient cells ({len(cells)})")
                    continue
                
                # Determine team_id based on row position
                team_id = team1_id if row_idx < 5 else team2_id
                
                # Extract player name from first cell
                player_cell = cells[0]
                team_div = player_cell.find("div", class_="team")
                if not team_div:
                    continue
                
                player_div = team_div.find("div", recursive=False)
                if not player_div:
                    continue
                
                full_text = player_div.get_text(separator='\n', strip=True)
                lines = [line.strip() for line in full_text.split('\n') if line.strip()]
                if not lines:
                    continue
                
                player_name = lines[0]
                
                # Extract agent from second cell
                agent_cell = cells[1]
                agent_img = agent_cell.find("img")
                agent = ""
                if agent_img and agent_img.get("src"):
                    # Extract agent name from image path like "/img/vlr/game/agents/raze.png"
                    src = agent_img.get("src")
                    agent = src.split('/')[-1].replace('.png', '') if '/' in src else ""
                
                # Helper function to extract stat value from a cell
                def extract_stat(cell):
                    stat_div = cell.find("div", class_="stats-sq")
                    if stat_div:
                        # Check if it's an empty cell (mod-egg)
                        if 'mod-egg' in stat_div.get('class', []):
                            return ""
                        
                        # Get only the direct text content, not nested popable content
                        # First try to get the direct text node
                        direct_text = None
                        for content in stat_div.contents:
                            if isinstance(content, str):
                                text = content.strip()
                                if text:
                                    direct_text = text
                                    break
                        
                        if direct_text:
                            try:
                                return int(direct_text)
                            except ValueError:
                                return ""
                        
                        # Fallback: get first text node before any div
                        text_parts = []
                        for elem in stat_div.children:
                            if isinstance(elem, str):
                                text_parts.append(elem.strip())
                            elif elem.name == 'div':
                                break  # Stop before popable content
                        
                        if text_parts:
                            text = ''.join(text_parts).strip()
                            if text:
                                try:
                                    return int(text)
                                except ValueError:
                                    return ""
                        
                        return ""
                    return ""
                
                # Helper function to extract detailed descriptions from popable content
                def extract_desc(cell):
                    """Extract detailed round information from wf-popable divs"""
                    stat_div = cell.find("div", class_="stats-sq")
                    if not stat_div:
                        return []
                    
                    # Find the popable contents div
                    popable_div = stat_div.find("div", class_="wf-popable-contents")
                    if not popable_div:
                        return []
                    
                    rounds = []
                    # Find all round sections
                    round_divs = popable_div.find_all("div", style=lambda s: s and "margin-top: 10px" in s)
                    
                    for round_div in round_divs:
                        # Get round number
                        round_span = round_div.find("span")
                        if not round_span:
                            continue
                        
                        try:
                            round_num = int(round_span.get_text(strip=True))
                        except ValueError:
                            continue
                        
                        # Get victim names
                        victims = []
                        victim_divs = round_div.find_all("div", style=lambda s: s and "display: flex" in s)
                        
                        for victim_div in victim_divs:
                            # Extract just the text (player name), not the agent image
                            victim_text = victim_div.get_text(strip=True)
                            if victim_text:
                                victims.append(victim_text)
                        
                        if victims:
                            rounds.append({round_num: victims})
                    
                    return rounds
                
                # Extract stats from columns 2-13
                two_k = extract_stat(cells[2])
                three_k = extract_stat(cells[3])
                four_k = extract_stat(cells[4])
                five_k = extract_stat(cells[5])
                
                one_v1 = extract_stat(cells[6])
                one_v2 = extract_stat(cells[7])
                one_v3 = extract_stat(cells[8])
                one_v4 = extract_stat(cells[9])
                one_v5 = extract_stat(cells[10])
                
                econ = extract_stat(cells[11])
                pl = extract_stat(cells[12])
                de = extract_stat(cells[13])
                
                # Build desc JSON structure
                desc_dict = {}
                
                # Extract descriptions for multikills (2k-5k)
                multikill_cols = [
                    (cells[2], '2k'),
                    (cells[3], '3k'),
                    (cells[4], '4k'),
                    (cells[5], '5k')
                ]
                
                for cell, key in multikill_cols:
                    rounds_data = extract_desc(cell)
                    if rounds_data:
                        desc_dict[key] = {}
                        for round_info in rounds_data:
                            for round_num, victims in round_info.items():
                                desc_dict[key][round_num] = victims
                
                # Extract descriptions for clutches (1v1-1v5)
                clutch_cols = [
                    (cells[6], '1v1'),
                    (cells[7], '1v2'),
                    (cells[8], '1v3'),
                    (cells[9], '1v4'),
                    (cells[10], '1v5')
                ]
                
                for cell, key in clutch_cols:
                    rounds_data = extract_desc(cell)
                    if rounds_data:
                        desc_dict[key] = {}
                        for round_info in rounds_data:
                            for round_num, victims in round_info.items():
                                desc_dict[key][round_num] = victims
                
                # Convert desc_dict to JSON string
                desc_json = json.dumps(desc_dict) if desc_dict else ""
                
                game_stats.append({
                    "tournament_id": tournament_id,
                    "match_id": match_id,
                    "game_id": game_id,
                    "team_id": team_id,
                    "player_name": player_name,
                    "agent": agent,
                    "2k": two_k,
                    "3k": three_k,
                    "4k": four_k,
                    "5k": five_k,
                    "1v1": one_v1,
                    "1v2": one_v2,
                    "1v3": one_v3,
                    "1v4": one_v4,
                    "1v5": one_v5,
                    "econ": econ,
                    "pl": pl,
                    "de": de,
                    "desc": desc_json
                })
            
            logger.info(f"Game {game_id}: Extracted {len(game_stats)} player advanced stats records")
            session.close()
            return game_stats, (tournament_id, match_id, game_id)
            
        except Exception as e:
            logger.warning(f"Error scraping advanced stats for game {game_id}: {e}")
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
            
            # Add &tab=performance if not already present
            if '?' in game_url:
                if 'tab=' not in game_url:
                    game_url = f"{game_url}&tab=performance"
                else:
                    game_url = re.sub(r'tab=[^&]*', 'tab=performance', game_url)
            else:
                game_url = f"{game_url}?tab=performance"
            
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
            "tournament_id", "match_id", "game_id", "team_id",
            "player_name","agent","2k","3k","4k","5k",
            "1v1","1v2","1v3","1v4","1v5","econ","pl","de","desc"
        ]
        stats_df = stats_df[column_order]
        
        logger.info(f"Saving {len(batch_stats)} matchup records to {output_path}")
        save_to_csv(stats_df, output_path)
        logger.info(f"✓ Successfully saved matchup records to CSV")
        
        del stats_df
    
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