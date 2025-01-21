insert into host_activity_reduced

with daily_aggregate AS(
		select e.host,
		date(e.event_time) as date,
		COUNT(1) as num_site_hits,
		COUNT(distinct e.user_id) as num_unique_vistors
		from events e
		where date(e.event_time) = date('2023-01-01')
		and e.host is not null
		group by e.host, date(e.event_time)
	), yesterday_array as (
	select * from host_activity_reduced
	where month = DATE('2023-01-01')
)
select 
	coalesce (ya.month, date_trunc('month', da.date)) as month,
	coalesce (da.host, ya.host) as host,
	case when ya.hit_array is not null then
		ya.hit_array || ARRAY[coalesce(da.num_site_hits, 0)]
	when ya.hit_array is null
		then array_fill(0, array[coalesce(date-date(date_trunc('month', date)),0)]) || ARRAY[coalesce(da.num_site_hits, 0)]
	end as hit_array,
	case when ya.unique_visitors is not null then
		ya.unique_visitors || ARRAY[coalesce(da.num_unique_vistors, 0)]
	when ya.unique_visitors is null
		then array_fill(0, array[coalesce(date-date(date_trunc('month', date)),0)]) || ARRAY[coalesce(da.num_unique_vistors, 0)]
	end as unique_visitors
from daily_aggregate da
full outer join yesterday_array ya 
on da.host = ya.host
on conflict (host, month)
do update set hit_array = EXCLUDED.hit_array, unique_visitors = EXCLUDED.unique_visitors