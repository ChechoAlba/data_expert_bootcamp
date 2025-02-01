select * from array_metrics

select cardinality(metric_array), count(1) from array_metrics group by 1

with agg as(
	
select metric_name, month_start,
ARRAY[SUM(metric_array[1]),
SUM(metric_array[2]),
SUM(metric_array[3]),
SUM(metric_array[4]),
SUM(metric_array[5])] as summed_array
from array_metrics
group by metric_name, month_start

)

select metric_name,
month_start + CAST(cast(index - 1 as TEXT) || ' day' as INTERVAL),
elem as value
from agg
cross join unnest(agg.summed_array)
with ordinality as a(elem,index)