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
def scrape_tournaments_game_data(delay=0.4, max_workers=8, batch_size=50):
    """
    Scrape game-level data from VLR.gg.
    Memory-optimized with streaming processing for 24k+ games.
    Only processes matches not in temp checkpoint file.
    
    Args:
        delay: Delay between requests (0.4s for stability)
        max_workers: Number of concurrent threads (8 for balance)
        batch_size: Save every N games (50 for memory management)
    """
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    matches_path = "/opt/airflow/valorant_scraper/data/raw/tournaments_match.csv"
    output_path = "/opt/airflow/valorant_scraper/data/raw/tournaments_game.csv"
    temp_path = "/opt/airflow/valorant_scraper/data/temp/tournaments_game.csv"
    
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
        tournament_id, match_id, game_id, map_name, game_url, team1_id, team2_id = game_data
        session = get_session_with_retries()
        
        try:
            time.sleep(delay)
            logger.debug(f"Scraping game {game_id} from URL: {game_url}")
            game_response = session.get(game_url, headers=headers, timeout=20)
            game_response.raise_for_status()
            game_soup = BeautifulSoup(game_response.content, "html.parser")
            
            # Find ALL game headers (there might be multiple if viewing a specific game)
            # We need to find the one matching our game_id
            all_game_headers = game_soup.find_all("div", class_="vm-stats-game")
            
            target_game_header = None
            for game_section in all_game_headers:
                # Check if this section's data-game-id matches
                if game_section.get("data-game-id") == game_id:
                    target_game_header = game_section.find("div", class_="vm-stats-game-header")
                    break
            
            # If not found by data attribute, try to find by the game parameter in URL
            if not target_game_header:
                # When viewing ?game=X, there's usually only one game header shown
                game_header = game_soup.find("div", class_="vm-stats-game-header")
                if game_header:
                    target_game_header = game_header
            
            if not target_game_header:
                logger.debug(f"No game header found for game {game_id}")
                session.close()
                return [], (tournament_id, match_id, game_id)
            
            # Extract team data from THIS specific game
            teams = target_game_header.find_all("div", class_="team")
            
            if len(teams) < 2:
                logger.debug(f"Less than 2 teams found for game {game_id}")
                session.close()
                return [], (tournament_id, match_id, game_id)
            
            # Team 1 (left side)
            team1_div = teams[0]
            score1_div = team1_div.find("div", class_="score")
            score1_text = score1_div.get_text(strip=True) if score1_div else "0"
            score1 = int(re.sub(r'\D', '', score1_text)) if score1_text else 0
            is_team1_winner = "mod-win" in score1_div.get("class", []) if score1_div else False
            
            # Extract Team 1 CT and T scores - handle different orderings
            score1_ct = None
            score1_t = None
            team1_spans = team1_div.find_all("span", class_=re.compile(r"mod-(ct|t)"))
            for span in team1_spans:
                classes = span.get("class", [])
                text = span.get_text(strip=True)
                if text and text.isdigit():
                    if "mod-ct" in classes:
                        score1_ct = int(text)
                    elif "mod-t" in classes:
                        score1_t = int(text)
            
            # Team 2 (right side)
            team2_div = teams[1]
            score2_div = team2_div.find("div", class_="score")
            score2_text = score2_div.get_text(strip=True) if score2_div else "0"
            score2 = int(re.sub(r'\D', '', score2_text)) if score2_text else 0
            is_team2_winner = "mod-win" in score2_div.get("class", []) if score2_div else False
            
            # Extract Team 2 CT and T scores - handle different orderings
            score2_ct = None
            score2_t = None
            team2_spans = team2_div.find_all("span", class_=re.compile(r"mod-(ct|t)"))
            for span in team2_spans:
                classes = span.get("class", [])
                text = span.get_text(strip=True)
                if text and text.isdigit():
                    if "mod-ct" in classes:
                        score2_ct = int(text)
                    elif "mod-t" in classes:
                        score2_t = int(text)
            
            # Determine winner and loser
            if is_team1_winner:
                winner_id = team1_id
                loser_id = team2_id
            elif is_team2_winner:
                winner_id = team2_id
                loser_id = team1_id
            else:
                # No winner marked, determine by score
                if score1 > score2:
                    winner_id = team1_id
                    loser_id = team2_id
                elif score2 > score1:
                    winner_id = team2_id
                    loser_id = team1_id
                else:
                    winner_id = None
                    loser_id = None
            
            # Extract map info and duration FROM THIS SPECIFIC GAME HEADER
            map_div = target_game_header.find("div", class_="map")
            duration = None
            pick = None
            
            if map_div:
                # Get duration for THIS game
                duration_div = map_div.find("div", class_="map-duration")
                if duration_div:
                    duration = duration_div.get_text(strip=True)
                    # Handle "-" as None
                    if duration == "-":
                        duration = None
                
                # Get pick info for THIS game
                pick_span = map_div.find("span", class_="picked")
                if pick_span:
                    # The pick span has classes like "picked mod-1" or "picked mod-2"
                    pick_classes = pick_span.get("class", [])
                    for cls in pick_classes:
                        if cls.startswith("mod-"):
                            pick_num = cls.replace("mod-", "")
                            if pick_num == "1":
                                pick = team1_id
                            elif pick_num == "2":
                                pick = team2_id
                            break
            
            game_stats = [{
                "tournament_id": tournament_id,
                "match_id": match_id,
                "game_id": game_id,
                "map": map_name,
                "pick": pick,
                "team1_id": team1_id,
                "team2_id": team2_id,
                "score1_ct": score1_ct,
                "score1_t": score1_t,
                "score2_ct": score2_ct,
                "score2_t": score2_t,
                "score1": score1,
                "score2": score2,
                "winner_id": winner_id,
                "loser_id": loser_id,
                "duration": duration,
                "game_url": game_url
            }]
            
            session.close()
            return game_stats, (tournament_id, match_id, game_id)
            
        except Exception as e:
            logger.warning(f"Error scraping game {game_id}: {e}")
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
        
        chunk_games_found = 0
        
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
                
                # Extract team IDs from match header
                team1_id = None
                team2_id = None
                
                match_header_vs = soup.find("div", class_="match-header-vs")
                if match_header_vs:
                    team_links = match_header_vs.find_all("a", class_="match-header-link")
                    
                    if len(team_links) >= 2:
                        # Team 1 (mod-1)
                        team1_link = team_links[0]
                        team1_href = team1_link.get("href", "")
                        if team1_href:
                            if m := re.search(r'/team/(\d+)/', team1_href):
                                team1_id = m.group(1)
                        
                        # Team 2 (mod-2)
                        team2_link = team_links[1]
                        team2_href = team2_link.get("href", "")
                        if team2_href:
                            if m := re.search(r'/team/(\d+)/', team2_href):
                                team2_id = m.group(1)
                
                if not team1_id or not team2_id:
                    logger.warning(f"Could not extract team IDs for match {match_id}")
                    continue
                
                # Find game navigation items
                game_nav_items = soup.find_all("div", class_="vm-stats-gamesnav-item")
                
                # Handle case when there are no game nav items (single game match)
                if not game_nav_items:
                    logger.debug(f"No game nav items found for match {match_id} - checking for single game")
                    
                    # Look for vm-stats-game div with data-game-id
                    game_sections = soup.find_all("div", class_="vm-stats-game")
                    for game_section in game_sections:
                        game_id = game_section.get("data-game-id", "")
                        
                        # Skip "all" game section
                        if game_id == "all" or not game_id:
                            continue
                        
                        # Try to extract map from the game header
                        map_name = None
                        game_header = game_section.find("div", class_="vm-stats-game-header")
                        if game_header:
                            map_div = game_header.find("div", class_="map")
                            if map_div:
                                map_span = map_div.find("span")
                                if map_span:
                                    map_name = map_span.get_text(strip=True)
                        
                        # Add game to buffer
                        game_url = match_url  # No ?game= parameter needed
                        games_buffer.append((tournament_id, match_id, game_id, map_name, game_url, team1_id, team2_id))
                        chunk_games_found += 1
                        logger.debug(f"Match {match_id}: found single game {game_id}")
                        break  # Only process first valid game
                else:
                    # Normal case: process game nav items
                    games_in_match = 0
                    for nav_item in game_nav_items:
                        game_id = nav_item.get("data-game-id", "")
                        disabled = nav_item.get("data-disabled", "0")
                        
                        # Skip "all" and disabled games
                        if game_id == "all" or disabled == "1" or not game_id:
                            continue
                        
                        map_name = None
                        map_div = nav_item.find("div", style=re.compile("margin-bottom"))
                        if map_div:
                            map_text = map_div.get_text(strip=True)
                            # Remove leading number and whitespace
                            map_name = re.sub(r'^\d+\s*', '', map_text).strip()
                        
                        game_url = f"{match_url}/?game={game_id}"
                        games_buffer.append((tournament_id, match_id, game_id, map_name, game_url, team1_id, team2_id))
                        games_in_match += 1
                        chunk_games_found += 1
                    
                    if games_in_match > 0:
                        logger.debug(f"Match {match_id}: found {games_in_match} games")
                    else:
                        logger.warning(f"Match {match_id}: no valid games found")
                
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
        
        logger.info(f"[Chunk {chunk_idx+1}/{num_chunks}] Found {chunk_games_found} games in this chunk")
        
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