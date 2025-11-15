# Valorant Match Prediction Data Scraper

## Overview
Apache Airflow pipeline to scrape and process Valorant match data for prediction modeling.

## Data Sources
- VLR.gg: Match stats, player performance, side splits
- Liquipedia: Rosters, events, vetoes, head-to-head
- Riot Games: Patch notes and balance changes

## Features Captured

### Team-Level
- Recent form (exponential decay win rate)
- Map performance by side (Attack/Defense)
- Pistol round stats and conversion
- Opening duel efficiency
- Trade efficiency
- Clutch performance
- Strength-of-schedule adjusted ratings
- Roster stability

### Player-Level
- ACS, ADR, K:D by role
- Entry success/survival
- Headshot% and accuracy
- Multikill rates
- KAST
- Agent pool breadth
- Form volatility

### Match-Level
- Series type (BO1/BO3/BO5)
- Map pool and veto order
- Patch version
- Event tier and stage
- Travel and rest days
- Head-to-head history

## Setup

1. Install requirements:
   ```
   pip install -r requirements.txt
   ```

2. Initialize Airflow:
   ```
   airflow db init
   ```

3. Copy files to Airflow home directory:
   ```
   cp -r dags/ scrapers/ processors/ config/ utils/ $AIRFLOW_HOME/
   ```

4. Start Airflow:
   ```
   airflow webserver -p 8080
   airflow scheduler
   ```

5. Enable the DAG in Airflow UI

## Output
Final CSV: `data/final/features_for_modeling.csv`

## Notes
- Respects rate limits (2s delay between requests)
- Runs daily at 2 AM
- Retries failed tasks up to 2 times
- Stores raw data for reproducibility