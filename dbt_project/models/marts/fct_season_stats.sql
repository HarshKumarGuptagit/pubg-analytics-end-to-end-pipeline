WITH staging AS (
    SELECT * FROM {{ ref('stg_season_stats') }}
    -- Get the most recent record for each player/season combo (deduplication)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY player_name, season_id ORDER BY ingested_at DESC) = 1
)

SELECT 
    player_name,
    season_id,
    
    -- Core Counts
    rounds_played,
    wins,
    top_10s,
    kills,
    
    -- Calculated KPIs
    
    -- 1. K/D Ratio: Kills / Deaths
    -- In PUBG, Deaths = Rounds Played - Wins (You don't die if you win)
    ROUND(
        kills / NULLIF((rounds_played - wins), 0), 
        2
    ) as kd_ratio,
    
    -- 2. Average Damage per Round (ADR)
    ROUND(damage_dealt / NULLIF(rounds_played, 0), 0) as avg_damage,
    
    -- 3. Win Rate %
    ROUND((wins / NULLIF(rounds_played, 0)) * 100, 1) as win_rate_pct,
    
    -- 4. Top 10 Rate %
    ROUND((top_10s / NULLIF(rounds_played, 0)) * 100, 1) as top10_rate_pct

FROM staging