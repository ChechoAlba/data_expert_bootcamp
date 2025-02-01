SELECT team_player_name , player_name, total_points_by_player_on_team 
FROM public.players_games_stats_dashboard
where aggregation_level = 'team_player_name__player_name'
order by total_points_by_player_on_team desc

SELECT game_season , player_name, total_points_by_player_on_season 
FROM public.players_games_stats_dashboard
where aggregation_level = 'game_season__player_name'
order by total_points_by_player_on_season desc

SELECT team_winner_game_name, total_winner_games
FROM public.players_games_stats_dashboard
where aggregation_level = 'team_winner_game_name'
order by total_winner_games DESC