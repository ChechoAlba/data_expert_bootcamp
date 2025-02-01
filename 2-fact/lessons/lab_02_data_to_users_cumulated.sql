--truncate table users_cumulated
insert into users_cumulated
--Run multiple times changed the date on yesterday and today from 2022-12-31/2023-01-01 to 2023-01-30/2023-01-31 one month for lab
with yesterday as (
	select 
		*
	from users_cumulated
	where date = DATE('2023-01-30') --event time start on 2023-01-01
), today AS(
	select 
		cast(user_id as text),
		DATE(cast(event_time as TIMESTAMP)) as date_active
	from events
	where DATE(cast(event_time as TIMESTAMP)) = DATE('2023-01-31') --event time start on 2023-01-01
	and user_id is not NULL
	group by user_id, DATE(cast(event_time as TIMESTAMP))
)

select 
COALESCE(t.user_id, y.user_id) as user_id,
CASE when y.dates_active is null 
	then array[t.date_active]
	when t.date_active is null then y.dates_active
	else array[t.date_active] || y.dates_active
END
as dates_active,
coalesce(t.date_active, y.date + interval '1 day') as date
from today t 
full outer join yesterday y
on t.user_id = y.user_id