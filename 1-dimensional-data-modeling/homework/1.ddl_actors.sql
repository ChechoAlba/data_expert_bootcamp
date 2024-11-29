create type film_properties as (
	film text,
	votes INTEGER,
	rating FLOAT,
	film_id text
);
create type quality_class as enum ('star','good','average','bad');
create table actors (
	actor text,
	films film_properties[],
	quality_class quality_class,
	is_active BOOL,
	year INTEGER,
	primary KEY(actor, year)
);