import os
import time
import requests
import json
from datetime import datetime
from google.cloud import bigquery

# ---------------- CONFIGURATION ---------------- #

PROJECT_ID = "pubg-analytics-2025"
DATASET_ID = "pubg_raw"
PLATFORM = "steam"  # Options: steam, kakao, xbox, psn

# List of Pro Players to track
PLAYER_NAMES = ["TGLTN", "Kickstart", "xmpl", "M1ME"]

# API Key Validation
API_KEY = os.environ.get("PUBG_API_KEY")
if not API_KEY:
    raise ValueError("❌ PUBG_API_KEY environment variable is missing!")

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/vnd.api+json"
}

# Initialize BigQuery Client
bq_client = bigquery.Client(project=PROJECT_ID)

# ---------------- HELPER FUNCTIONS ---------------- #

def load_json_to_bq(table_name, rows):
    """
    Loads data into BigQuery.
    CRITICAL: Enforces 'raw' column as JSON type to handle nested data.
    """
    if not rows:
        return

    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    # Define Schema to force "raw" as JSON type
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=[
            bigquery.SchemaField("ingested_at", "TIMESTAMP"),
            bigquery.SchemaField("match_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("player_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("season_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("raw", "JSON") 
        ]
    )

    try:
        job = bq_client.load_table_from_json(rows, table_id, job_config=job_config)
        job.result()  # Wait for job to complete
        print(f"✅ Loaded {len(rows)} row(s) into {table_name}")
    except Exception as e:
        print(f"❌ Failed to load {table_name}: {e}")

def safe_get(url):
    """
    Robust API fetcher with rate limit handling (429).
    """
    try:
        res = requests.get(url, headers=HEADERS, timeout=15)
        if res.status_code == 200:
            return res.json()
        elif res.status_code == 429:
            print("⏳ Rate limit hit. Sleeping 5s...")
            time.sleep(5)
            return safe_get(url)  # Retry
        elif res.status_code == 404:
            return None  # Normal for some missing data
        else:
            print(f"⚠️ API Error {res.status_code}: {url}")
            return None
    except Exception as e:
        print(f"❌ Connection error: {e}")
        return None

# ---------------- CORE LOGIC ---------------- #

def get_latest_season_id():
    """
    Fetches all seasons and returns the ID of the current active season.
    Filters out 'offseason' and sorts to find the latest 'pc-2018' entry.
    """
    url = f"https://api.pubg.com/shards/{PLATFORM}/seasons"
    data = safe_get(url)
    
    if not data or "data" not in data:
        return None

    seasons = data["data"]
    
    # Filter 1: Remove "isOffseason": true
    valid_seasons = [s for s in seasons if not s["attributes"].get("isOffseason", False)]
    
    # Filter 2: Keep only standard PC seasons (avoids beta/early access weirdness)
    # Most modern seasons look like "division.bro.official.pc-2018-28"
    pc_seasons = [s for s in valid_seasons if "pc-2018" in s["id"]]

    if not pc_seasons:
        # Fallback if specific naming convention fails, just take the last valid one
        return valid_seasons[-1]["id"] if valid_seasons else None

    # Return the very last one in the list (Latest Season)
    return pc_seasons[-1]["id"]

# ---------------- MAIN PIPELINE ---------------- #

def ingest_raw():
    ingested_at = datetime.utcnow().isoformat()
    print(f"🚀 Starting Ingestion: {ingested_at}")

    # 1. Identify the Correct Season
    target_season_id = get_latest_season_id()
    print(f"📅 Target Season ID: {target_season_id}")

    for player_name in PLAYER_NAMES:
        print(f"\n🔍 Processing Player: {player_name}")
        
        # 2. Get Player Account ID
        url_player = f"https://api.pubg.com/shards/{PLATFORM}/players?filter[playerNames]={player_name}"
        player_data = safe_get(url_player)

        if not player_data:
            print(f"❌ Player {player_name} not found.")
            continue

        player_obj = player_data["data"][0]
        player_id = player_obj["id"]

        # Load Player Identity
        load_json_to_bq("players", [{
            "ingested_at": ingested_at,
            "player_name": player_name,
            "raw": player_obj
        }])

        # 3. Get Season Stats (Only if we found a valid season)
        if target_season_id:
            url_stats = f"https://api.pubg.com/shards/{PLATFORM}/players/{player_id}/seasons/{target_season_id}"
            stats_data = safe_get(url_stats)
            
            if stats_data:
                load_json_to_bq("player_season_stats", [{
                    "ingested_at": ingested_at,
                    "player_name": player_name,
                    "season_id": target_season_id,
                    "raw": stats_data
                }])
            else:
                print(f"⚠️ No stats available for {player_name} in season {target_season_id}")
            
            time.sleep(0.5)  # Be polite to API

        # 4. Get Latest Match
        matches = player_obj.get("relationships", {}).get("matches", {}).get("data", [])
        
        if matches:
            latest_match_id = matches[0]["id"]
            print(f"🎮 Fetching Match: {latest_match_id}")
            
            url_match = f"https://api.pubg.com/shards/{PLATFORM}/matches/{latest_match_id}"
            match_data = safe_get(url_match)
            
            if match_data:
                load_json_to_bq("matches", [{
                    "ingested_at": ingested_at,
                    "match_id": latest_match_id,
                    "raw": match_data
                }])

                # 5. Get Telemetry (The big data!)
                # Telemetry URL is hidden in the "included" array under type "asset"
                telemetry_url = None
                for item in match_data.get("included", []):
                    if item.get("type") == "asset":
                        telemetry_url = item.get("attributes", {}).get("URL")
                        break
                
                if telemetry_url:
                    print("📡 Downloading Telemetry...")
                    # Telemetry is hosted on CDN, standard request is fine
                    telemetry_res = requests.get(telemetry_url, headers={"Accept-Encoding": "gzip"})
                    if telemetry_res.status_code == 200:
                        telemetry_json = telemetry_res.json()
                        
                        load_json_to_bq("telemetry", [{
                            "ingested_at": ingested_at,
                            "match_id": latest_match_id,
                            "raw": telemetry_json
                        }])
                    else:
                        print("❌ Failed to download telemetry file.")

        time.sleep(1)  # Pause between players

    print("\n✅ Ingestion Complete!")

if __name__ == "__main__":
    ingest_raw()