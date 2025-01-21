with deduped as (
	select
	gd.*,
	row_number() over (partition by gd.game_id, gd.team_id, gd.player_id order by g.game_date_est) as row_num
	from game_details gd join games g on gd.game_id = g.game_id)
select game_id,
team_id,
team_abbreviation,
team_city,
player_id,
player_name,
nickname,
start_position,
comment,
min,
fgm,
fga,
fg_pct,
fg3m,
fg3a,
fg3_pct,
ftm,
fta,
ft_pct,
oreb,
dreb,
reb,
ast,
stl,
blk,
"TO",
pf,
pts,
plus_minus
from deduped
where row_num = 1