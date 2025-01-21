--This was done only for January
--truncate array_metrics
insert into array_metrics
with daily_aggregate AS(
	select e.user_id,
	date(event_time) as date,
	COUNT(1) as num_site_hits
	from events e
	where date(event_time) = date('2023-01-05')
	and user_id is not null
	group by e.user_id, date(event_time)
), yesterday_array as (
	select * from array_metrics
	where month_start = DATE('2023-01-01')
)
select 
	coalesce (da.user_id, ya.user_id) as user_id,
	coalesce (ya.month_start, date_trunc('month', da.date)) as month_start,
	'site_hits' as metric_name,
	case when ya.metric_array is not null then
		ya.metric_array || ARRAY[coalesce(da.num_site_hits, 0)]
	when ya.metric_array is null
		then array_fill(0, array[coalesce(date-date(date_trunc('month', date)),0)]) || ARRAY[coalesce(da.num_site_hits, 0)]
	end as metric_array
from daily_aggregate da
full outer join yesterday_array ya 
on da.user_id = ya.user_id
on conflict (user_id, month_start, metric_name)
do update set metric_array = EXCLUDED.metric_array