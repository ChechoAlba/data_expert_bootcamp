--		- `star`: Average rating > 8.
--		- `good`: Average rating > 7 and ≤ 8.
--		- `average`: Average rating > 6 and ≤ 7.
--		- `bad`: Average rating ≤ 6.
--truncate table actors_dim
create type quality_class as enum ('star','good','average','bad')
insert into actors_dim 
with last_year as (
	select *
	from actors_dim af 
	where af.year = 1972
),
current_year as (
	select actor,
	year,
	array_agg(
		row(
		film,
		votes,
		rating,
		filmid
		)::film_properties
	) as films,
	AVG(rating) as average_rating,
	true as is_active
	from actor_films af 
	where af.year = 1973
	group by actor,year
	
),
players_films_unnested as (
	select 
	coalesce (cy.actor, ly.actor) as actor,
	case when ly.films is null 
	then cy.films
	when cy.year is not null then ly.films || cy.films
	else ly.films
	end as films,
	coalesce (cy.year, ly.year + 1) as year,
	case when cy.year is not null 
		then case when cy.average_rating>8 then 'star'
		when cy.average_rating>8 then 'star'
		when cy.average_rating>7 and cy.average_rating <= 8  then 'good'
		when cy.average_rating>6 and cy.average_rating <= 7  then 'average'
		else 'bad' end
	else ly.quality_class end as quality_class,
	coalesce (cy.is_active, FALSE) as is_active
	from current_year cy full outer join last_year ly
	on cy.actor = ly.actor
)
select 
actor,
films,
quality_class,
is_active,
year 
from players_films_unnested