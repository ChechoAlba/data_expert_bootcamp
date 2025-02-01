create table user_devices_cumulated (
	user_id TEXT,
	browser_type text,
	device_activity_datelist DATE[],
	date DATE, 
	primary key (user_id, browser_type, date)
)
