

  create or replace view `pubg-analytics-2025`.`pubg_marts`.`stg_telemetry`
  OPTIONS()
  as WITH raw_telemetry AS (
    SELECT 
        match_id,
        season_id,
        raw as json_blob
    FROM `pubg-analytics-2025`.`pubg_raw`.`telemetry`
)

SELECT 
    t.match_id,
    t.season_id,
    
    -- Extract Event Type (Clean "LogPlayerKillV2" to just "LogPlayerKill")
    JSON_EXTRACT_SCALAR(x, '$._T') as event_type,
    
    -- Timestamp of the event
    TIMESTAMP(JSON_EXTRACT_SCALAR(x, '$._D')) as event_timestamp,

    -- KILLER / ATTACKER INFO
    -- We use COALESCE because "Damage" events use 'attacker' and "Kill" events use 'killer'
    COALESCE(
        JSON_EXTRACT_SCALAR(x, '$.killer.name'), 
        JSON_EXTRACT_SCALAR(x, '$.attacker.name'), 
        'Environment' 
    ) as player_name,

    -- VICTIM INFO
    JSON_EXTRACT_SCALAR(x, '$.victim.name') as victim_name,

    -- COMBAT STATS
    SAFE_CAST(JSON_EXTRACT_SCALAR(x, '$.damage') AS FLOAT64) as damage_dealt,
    JSON_EXTRACT_SCALAR(x, '$.damageTypeCategory') as weapon_category,
    JSON_EXTRACT_SCALAR(x, '$.damageCauserName') as weapon_name

FROM raw_telemetry t,
UNNEST(JSON_EXTRACT_ARRAY(t.json_blob)) as x
WHERE 
    -- Filter for only the events we need for the dashboard
    JSON_EXTRACT_SCALAR(x, '$._T') LIKE 'LogPlayerKill%' 
    OR 
    JSON_EXTRACT_SCALAR(x, '$._T') = 'LogPlayerTakeDamage';

