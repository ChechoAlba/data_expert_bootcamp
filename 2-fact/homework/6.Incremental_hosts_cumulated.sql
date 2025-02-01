insert into hosts_cumulated

with yesterday as (
	select 
		*
	from hosts_cumulated
	where date = DATE('2023-01-30') 
), today AS(
	select 
		host,
		DATE(event_time) as date_active
	from events
	where DATE(event_time) = DATE('2023-01-31')
	and host is not NULL
	group by host, DATE(event_time)
)



select 
COALESCE(t.host, y.host) as host,
CASE when y.host_activity_datelist is null 
	then array[t.date_active]
	when t.date_active is null then y.host_activity_datelist
	else array[t.date_active] || y.host_activity_datelist
END
as host_activity_datelist,
coalesce(t.date_active, y.date + interval '1 day') as date
from today t 
full outer join yesterday y
on t.host = y.host