
  
    

    create or replace table `pubg-analytics-2025`.`pubg_marts`.`fct_match_stats`
      
    
    

    OPTIONS()
    as (
      WITH telemetry AS (
    SELECT * FROM `pubg-analytics-2025`.`pubg_marts`.`stg_telemetry`
)

SELECT 
    player_name,
    match_id,
    season_id,
    
    -- Metric 1: Total Kills
    -- We count rows where the event starts with 'LogPlayerKill'
    COUNTIF(event_type LIKE 'LogPlayerKill%') as total_kills,
    
    -- Metric 2: Total Damage Dealt
    -- Summing up the damage from all 'LogPlayerTakeDamage' events
    COALESCE(SUM(damage_dealt), 0) as total_damage,
    
    -- Metric 3: Headshots (Bonus Metric!)
    COUNTIF(event_type LIKE 'LogPlayerKill%' AND weapon_category = 'Damage_Gun' AND damage_dealt > 0) as gun_kills

FROM telemetry
WHERE player_name IS NOT NULL 
  AND player_name != 'Environment'
GROUP BY 1, 2, 3
    );
  