insert into actors_scd
with with_previous as (
	select actor,
		is_active,
		quality_class,
		year,
		LAG("quality_class",1) over (partition by actor order by "year") as previous_quality_class,
		LAG("is_active",1) over (partition by actor order by "year") as previous_is_active
	from actors
	where year <=2020
), with_indicators as (
	select *,
	case 
		when quality_class <> previous_quality_class then 1
		when is_active <> previous_is_active then 1
		else 0
	end as change_indicator
	from with_previous
), with_streaks as (
	select *,
	SUM(change_indicator) over (partition by actor order by year) as streak_identifier
	from with_indicators
)
select actor,
	quality_class,
	is_active,
	MIN(year) as start_year,
	MAX(year) as end_year,
	2020 as current_year
from with_streaks
group by actor,
	streak_identifier,
	is_active,
	quality_class


