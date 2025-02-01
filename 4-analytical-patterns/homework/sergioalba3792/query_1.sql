insert into players_state_changing
with last_season as (
	select * from players_state_changing
	where season = 2001
),
current_season as (
	select p.player_name,
	p.is_active,
	p.current_season
	from players p 
	where p.current_season = 2002
)
select 
coalesce (cs.player_name, ls.player_name) as player_name,
coalesce (ls.first_active_season, cs.current_season) as first_active_season,
case 
	when cs.is_active then cs.current_season
	else ls.last_active_season
end as last_active_season,
case 
	when ls.player_name is null then 'New'
	when not cs.is_active and ls.last_active_season = cs.current_season - 1 then 'Retired'
	when cs.is_active and ls.last_active_season = cs.current_season - 1 then 'Continued Playing'
	when cs.is_active and ls.last_active_season < cs.current_season - 1 then 'Returned from Retirement'
	else 'Stayed Retired'
end as season_active_state,
COALESCE(ls.season_active,
          ARRAY []::INTEGER[])
         || CASE
                     when cs.is_active THEN ARRAY [cs.current_season]
                    ELSE ARRAY []::INTEGER[]
            END AS season_active,
COALESCE(cs.current_season, ls.season + 1) as season
from last_season ls
full outer join current_season cs
on ls.player_name = cs.player_name
                        