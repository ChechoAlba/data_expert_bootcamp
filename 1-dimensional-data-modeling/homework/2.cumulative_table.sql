insert into actors
with last_year as (
	select *
	from actors af 
	where af.year = (
		select coalesce(MAX(year),1969) from actors 
	)
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
	where af.year = (
		select coalesce(MAX(year)+1,1970) from actors 
	)
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
		else 'bad' end::quality_class
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