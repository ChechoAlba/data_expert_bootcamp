CREATE TABLE players_games_stats_dashboard as

with teams_deduplicated as (
	select 
	team_id,
	nickname 
	from teams 
	group by 1,2
), games_augmented as (
select 
	gd.team_id as team_player_id,
	gd.player_name,
	coalesce (gd.pts, 0) as player_points,
	g.season as game_season,
	case 
		when g.home_team_wins = 1 then g.home_team_id
		else g.visitor_team_id
	end as team_winner_game_id,
	g.game_id
	from game_details gd 
	join games g 
	on gd.game_id = g.game_id
), games_augmented_team_names as (
	select 
	t1.nickname as team_player_name,
	ga.player_name,
	ga.player_points,
	ga.game_season,
	t2.nickname as team_winner_game_name,
	ga.game_id
	from games_augmented ga
	join teams_deduplicated t1
	on ga.team_player_id = t1.team_id 
	join teams_deduplicated t2
	on ga.team_winner_game_id = t2.team_id 
), debug_cte as (
	select * 
	from games_augmented_team_names
)
select 
CASE
           WHEN GROUPING(team_player_name) = 0
               AND GROUPING(player_name) = 0
               THEN 'team_player_name__player_name'
           WHEN GROUPING(game_season) = 0
               AND GROUPING(player_name) = 0
               THEN 'game_season__player_name'
           WHEN GROUPING(team_winner_game_name) = 0 THEN 'team_winner_game_name'
       END as aggregation_level,
team_player_name,
player_name,
case WHEN GROUPING(team_player_name) = 0
     AND GROUPING(player_name) = 0 then SUM(player_points) else NULL
end as total_points_by_player_on_team, 
game_season,
case WHEN GROUPING(game_season) = 0
     AND GROUPING(player_name) = 0 then SUM(player_points) else NULL
end as total_points_by_player_on_season, 
team_winner_game_name,
case when GROUPING(team_winner_game_name) = 0 then COUNT(distinct game_id) else NULL
end as total_winner_games
from games_augmented_team_names gatn
GROUP BY GROUPING SETS (
        (team_player_name, player_name),
        (game_season, player_name),
        (team_winner_game_name)
    )
