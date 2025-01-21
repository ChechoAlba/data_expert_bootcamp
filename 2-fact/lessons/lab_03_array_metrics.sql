create table array_metrics(
	user_id numeric,
	month_start date,
	metric_name text,
	metric_array real[],
	primary key (user_id, month_start, metric_name)
	
)