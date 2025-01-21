create table host_activity_reduced(
	month date,
	host text,
	hit_array real[],
    unique_visitors real[],
	primary key (month, host)
)