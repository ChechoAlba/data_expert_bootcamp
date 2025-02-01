--- A query that uses window functions on `game_details` to find out the following things:
--  - What is the most games a team has won in a 90 game stretch? 

with dedup_games as (
	select 
	gd.game_id,
	g.game_date_est as date_game,
	gd.team_id,
	case 
			when g.home_team_wins = 1 and g.home_team_id = gd.team_id then 1
			else 0
	end as team_won_game
	from game_details gd 
	join games g on gd.game_id = g.game_id 
	group by gd.game_id, g.game_date_est, gd.team_id, team_won_game
), windowed as (
	select *,
	row_number () over (partition by team_id order by date_game) as row_num,
	SUM (team_won_game)  OVER (partition by team_id order by date_game ROWS between 90 preceding AND CURRENT ROW) as total_win_games_stretch
	from dedup_games
), teams_dedup as (
	select 
	team_id,
	nickname 
	from teams 
	group by 1,2
)

select t.nickname, MAX(total_win_games_stretch) as max_total_wins_stretch
from windowed w
join teams_dedup t
on w.team_id = t.team_id
group by nickname
order by max_total_wins_stretch DESC



