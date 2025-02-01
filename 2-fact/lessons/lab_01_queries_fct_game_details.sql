select t.*, gd.*
from fct_game_details gd join teams t
on t.team_id = gd.dim_team_id 

-- Find the player in the NBA who like was not bailed out in the most game
select gd.dim_player_name, 
COUNT(1) as num_games,
COUNT(case when dim_not_with_team then 1 end) as bailed_num,
CAST(COUNT(case when dim_not_with_team then 1 end) as REAL)/COUNT(1) as bail_pct
from fct_game_details gd 
group by 1
order by 4 desc