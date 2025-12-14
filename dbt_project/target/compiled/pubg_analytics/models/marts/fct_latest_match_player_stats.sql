SELECT 
    s.season_id,
    s.match_id,
    s.map_name,
    s.reference_player_name,
    s.game_mode,
    s.player_name,
    
    -- Rank & Survival
    s.win_place,
    CASE WHEN s.win_place = 1 THEN 'Winner' 
         WHEN s.win_place <= 10 THEN 'Top 10' 
         ELSE 'Eliminated' END as placement_tier,
    ROUND(s.time_survived_sec / 60, 2) as minutes_survived,
    
    -- Combat Efficiency
    s.kills,
    s.assists,
    s.damage_dealt,
    
    -- Calculated Metrics
    CASE WHEN s.win_place = 1 THEN s.kills -- You don't die if you win
         ELSE s.kills / 1 END as kd_ratio, 
         
    ROUND(SAFE_DIVIDE(s.damage_dealt, s.kills), 0) as damage_per_kill, -- High number = Kill Stealer or Poker
    ROUND(SAFE_DIVIDE(s.headshot_kills, s.kills), 2) * 100 as headshot_percentage,
    
    -- Playstyle Indicators
    s.total_distance_meters as mobility_score,
    s.total_healing_items_used as survival_utility_usage

FROM `pubg-analytics-2025`.`pubg_marts`.`stg_match_summary` s