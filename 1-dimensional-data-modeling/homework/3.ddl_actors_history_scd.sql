create table actors_scd (
	actor text, 
	quality_class quality_class,
	is_active BOOLEAN,
	start_year INTEGER,
	end_year INTEGER,
	current_year INTEGER,
	primary KEY(actor, start_year)
);