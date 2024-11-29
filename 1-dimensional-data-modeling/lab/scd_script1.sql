create table actors_scd (
	actor text, 
	quality_class quality_class,
	is_active BOOLEAN,
	start_year INTEGER,
	end_year INTEGER,
	current_year INTEGER,
	primary KEY(actor, start_year)
);
create type actors_scd_type AS(
	quality_class quality_class,
	is_active BOOLEAN,
	start_year INTEGER,
	end_year INTEGER
);
drop table actors_scd;

select actor, "quality_class", is_active 
from actors a 
where a."year" = 2021;