--Filter only lebron games, stablish 1/0 column scored_over_10 when lebron score more than 10 points in the game
with lebron_games as (
	select 
	gd.game_id,
	case 
		when gd.pts > 10 then 1 else 0
	end as scored_over_10
	from game_details gd 
	where player_name = 'LeBron James'
),
--Get the row num when Lebron do not score over 10 points
windowed as (
	select
	g.game_date_est as game_date,
    case 
		when lg.scored_over_10 = 0 then row_number () over (order by g.game_date_est)
	end as row_unbounded
	from lebron_games lg
	join games g on lg.game_id = g.game_id 
),
--get total games between games when lebron score over 10 points
games_between_scored_over_10 as (
	select 
	row_unbounded - coalesce(lag(row_unbounded, 1) over (order by game_date),0) as rows_games_over_10_points
	from windowed
	where row_unbounded is not NULL
)
--sum total games
select MAX(rows_games_over_10_points) as total_games_over_10_points from games_between_scored_over_10

