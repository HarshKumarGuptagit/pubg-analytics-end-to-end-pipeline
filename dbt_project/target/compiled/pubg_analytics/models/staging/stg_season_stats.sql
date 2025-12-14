WITH raw_stats AS (
    SELECT 
        player_name,
        season_id,
        raw as json_blob,
        ingested_at
    FROM `pubg-analytics-2025`.`pubg_raw`.`player_season_stats`
)

SELECT 
    player_name,
    season_id,
    
    -- Extract SQUAD-FPP Stats (The Competitive Standard)
    -- Path: raw -> data -> attributes -> gameModeStats -> squad-fpp
    SAFE_CAST(JSON_EXTRACT_SCALAR(json_blob, '$.data.attributes.gameModeStats.squad-fpp.roundsPlayed') AS INT64) as rounds_played,
    SAFE_CAST(JSON_EXTRACT_SCALAR(json_blob, '$.data.attributes.gameModeStats.squad-fpp.wins') AS INT64) as wins,
    SAFE_CAST(JSON_EXTRACT_SCALAR(json_blob, '$.data.attributes.gameModeStats.squad-fpp.top10s') AS INT64) as top_10s,
    SAFE_CAST(JSON_EXTRACT_SCALAR(json_blob, '$.data.attributes.gameModeStats.squad-fpp.kills') AS INT64) as kills,
    SAFE_CAST(JSON_EXTRACT_SCALAR(json_blob, '$.data.attributes.gameModeStats.squad-fpp.damageDealt') AS FLOAT64) as damage_dealt,
    SAFE_CAST(JSON_EXTRACT_SCALAR(json_blob, '$.data.attributes.gameModeStats.squad-fpp.assists') AS INT64) as assists,
    
    -- Keep the timestamp to know how fresh the data is
    ingested_at

FROM raw_stats
-- Only keep rows where we successfully extracted rounds (removes empty seasons)
WHERE JSON_EXTRACT_SCALAR(json_blob, '$.data.attributes.gameModeStats.squad-fpp.roundsPlayed') IS NOT NULL