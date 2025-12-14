WITH json_data AS (
    SELECT 
        match_id,
        player_name AS reference_player_name,
        season_id,
        JSON_EXTRACT_SCALAR(raw, '$.data.attributes.mapName') as map_name,
        JSON_EXTRACT_SCALAR(raw, '$.data.attributes.gameMode') as game_mode,
        TIMESTAMP(JSON_EXTRACT_SCALAR(raw, '$.data.attributes.createdAt')) as match_start_time,
        SAFE_CAST(JSON_EXTRACT_SCALAR(raw, '$.data.attributes.duration') AS INT64) as match_duration_sec,
        entry
    FROM {{ source('pubg_raw', 'matches') }},
    UNNEST(JSON_EXTRACT_ARRAY(raw, '$.included')) as entry
)

SELECT
    match_id,
    season_id,
    reference_player_name,
    map_name,
    game_mode,
    match_start_time,
    match_duration_sec,
    
    -- Player Identifiers
    JSON_EXTRACT_SCALAR(entry, '$.attributes.stats.playerId') as account_id,
    JSON_EXTRACT_SCALAR(entry, '$.attributes.stats.name') as player_name,
    
    -- Core Performance Metrics
    SAFE_CAST(JSON_EXTRACT_SCALAR(entry, '$.attributes.stats.winPlace') AS INT64) as win_place,
    SAFE_CAST(JSON_EXTRACT_SCALAR(entry, '$.attributes.stats.kills') AS INT64) as kills,
    SAFE_CAST(JSON_EXTRACT_SCALAR(entry, '$.attributes.stats.damageDealt') AS FLOAT64) as damage_dealt,
    SAFE_CAST(JSON_EXTRACT_SCALAR(entry, '$.attributes.stats.headshotKills') AS INT64) as headshot_kills,
    SAFE_CAST(JSON_EXTRACT_SCALAR(entry, '$.attributes.stats.assists') AS INT64) as assists,
    SAFE_CAST(JSON_EXTRACT_SCALAR(entry, '$.attributes.stats.DBNOs') AS INT64) as dbnos, -- Knocked downs
    
    -- Survival & Travel
    SAFE_CAST(JSON_EXTRACT_SCALAR(entry, '$.attributes.stats.timeSurvived') AS FLOAT64) as time_survived_sec,
    SAFE_CAST(JSON_EXTRACT_SCALAR(entry, '$.attributes.stats.walkDistance') AS FLOAT64) + 
    SAFE_CAST(JSON_EXTRACT_SCALAR(entry, '$.attributes.stats.rideDistance') AS FLOAT64) + 
    SAFE_CAST(JSON_EXTRACT_SCALAR(entry, '$.attributes.stats.swimDistance') AS FLOAT64) as total_distance_meters,
    
    -- Economy/Looting
    SAFE_CAST(JSON_EXTRACT_SCALAR(entry, '$.attributes.stats.weaponsAcquired') AS INT64) as weapons_looted,
    SAFE_CAST(JSON_EXTRACT_SCALAR(entry, '$.attributes.stats.heals') AS INT64) + 
    SAFE_CAST(JSON_EXTRACT_SCALAR(entry, '$.attributes.stats.boosts') AS INT64) as total_healing_items_used

FROM json_data
WHERE JSON_EXTRACT_SCALAR(entry, '$.type') = 'participant'