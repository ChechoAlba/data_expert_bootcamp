with users AS(
	select * from user_devices_cumulated uc 
	where date = DATE('2023-01-31')
),
series as (
	select *
	from generate_series(DATE('2023-01-01'), DATE('2023-01-31'), interval '1 day') as series_date
),
place_holder_ints as (
	select 
	case when
		device_activity_datelist @> ARRAY[DATE(series_date)] 
		then cast(POW(2, 32 - (date - DATE(series_date))) as bigint) 
	else 0 
	end  AS placeholder_int_value,
	*
	from users
	cross join series
)
select user_id,
SUM(placeholder_int_value),
cast(cast(SUM(placeholder_int_value) as BIGINT) as BIT(32))
from place_holder_ints
group by user_id 