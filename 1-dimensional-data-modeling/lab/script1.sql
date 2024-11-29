select * from actor_films af;

select *
from actor_films af 
where actor  = 'Felix Silla' order by year asc;

drop type film_properties
create type film_properties as (
	film text,
	votes INTEGER,
	rating FLOAT,
	film_id text
);

--1. **DDL for `actors` table:** Create a DDL for an `actors` table with the following fields:
--    - `films`: An array of `struct` with the following fields:
--		- film: The name of the film.
--		- votes: The number of votes the film received.
--		- rating: The rating of the film.
--		- filmid: A unique identifier for each film.
--
--    - `quality_class`: This field represents an actor's performance quality, determined by the average rating of movies of their most recent year. It's categorized as follows:
--		- `star`: Average rating > 8.
--		- `good`: Average rating > 7 and ≤ 8.
--		- `average`: Average rating > 6 and ≤ 7.
--		- `bad`: Average rating ≤ 6.
--    - `is_active`: A BOOLEAN field that indicates whether an actor is currently active in the film industry (i.e., making films this year).
--    
--    
--
drop table actors_dim
create table actors_dim (
	actor text,
	films film_properties[],
	quality_class text,
	is_active BOOL,
	year INTEGER,
	primary KEY(actor, year)
);

select MIN(year) from actor_films af; --1970

with last_year as (
	select *
	from actors_dim af 
	where af.year = 1969
),
current_year as (
	select *
	from actor_films af 
	where af.year = 1970
),
players_films_unnested as (
	select 
	coalesce (cy.actor, ly.actor) as actor,
	case when ly.films is null 
	then array [row(
		cy.film,
		cy.votes,
		cy.rating,
		cy.filmid
	)::film_properties]
	when cy.year is not null then ly.films || array [row(
		cy.film,
		cy.votes,
		cy.rating,
		cy.filmid
	)::film_properties]
	else ly.films
	end as films,
	coalesce (cy.year, ly.year + 1) as year
	from current_year cy full outer join last_year ly
	on cy.actor = ly.actor
)
select 
actor,
films,
(films[cardinality(films)]::film_properties).votes as temp,
year 
from players_films_unnested



