insert into user_devices_cumulated
with yesterday as (
	select *
	from user_devices_cumulated
	where date = DATE('2023-01-04')
),
today as (
select 
	cast(e.user_id as text),
	d.browser_type,
	date(e.event_time) as date_active
from events e
join devices d on e.device_id = d.device_id 
where date(e.event_time) = DATE('2023-01-05')
and e.user_id is not null
group by e.user_id,d.browser_type, date(e.event_time)
)

select 
coalesce (t.user_id, y.user_id) as user_id,
coalesce (t.browser_type, y.browser_type) as browser_type,
case when y.device_activity_datelist is null then array [t.date_active]
when t.date_active is null then y.device_activity_datelist
else array[t.date_active] || y.device_activity_datelist
end as device_activity_datelist,
date(coalesce(t.date_active, y.date + interval '1 day')) as date
from today t 
full outer join yesterday y
on t.user_id = y.user_id  and t.browser_type = y.browser_type 