create table hosts_cumulated (
	host TEXT,
	host_activity_datelist DATE[],
	date DATE, 
	primary key (host, date)
)
