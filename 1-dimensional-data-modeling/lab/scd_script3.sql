with last_year_scd as (
	select * from actors_scd 
	where current_year=2020
	and end_year=2020
), historical_scd as (
	select * from actors_scd 
	where current_year=2020
	and end_year<2020
),this_year_data as (
	select *  from actors a where a.year = 2021
), unchanged_records as (
	select 
	ty.actor,
	ty.quality_class,
	ty.is_active,
	ly.start_year,
	ty.year as end_year,
	2021 as current_year
	from this_year_data ty
	join last_year_scd ly
	on ty.actor = ly.actor
	and ty.quality_class = ly.quality_class
	and ty.is_active = ly.is_active
), changed_records as (
	select 
	ty.actor,
	unnest(ARRAY[
		ROW(
			ly.quality_class,
			ly.is_active,
			ly.start_year,
			ly.end_year
		)::actors_scd_type,
		ROW(
			ty.quality_class,
			ty.is_active,
			ty.year,
			ty.year
		)::actors_scd_type
	]) as records
	from this_year_data ty
	left join last_year_scd ly
	on ty.actor = ly.actor
	where (ty.quality_class != ly.quality_class
	or ty.is_active != ly.is_active)
), unnested_changed_records as (
	select actor,
	(records::actors_scd_type).quality_class,
	(records::actors_scd_type).is_active,
	(records::actors_scd_type).start_year,
	(records::actors_scd_type).end_year,
	2021 as current_year
	from changed_records
), new_records as (
	select 
	ty.actor,
			ty.quality_class,
			ty.is_active,
			ty.year as start_year,
			ty.year as end_year,
			2021 as current_year
	from this_year_data ty
	left join last_year_scd ly
	on ty.actor = ly.actor
	where ly.actor is NULL
)
select * from historical_scd
union all
select * from unchanged_records
union all
select * from unnested_changed_records 
union all
select * from new_records 

