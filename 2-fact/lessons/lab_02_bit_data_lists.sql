with users AS(
	select * from users_cumulated uc 
	where date = DATE('2023-01-31')
),
series as (
	select *
	from generate_series(DATE('2023-01-01'), DATE('2023-01-31'), interval '1 day') as series_date
),
place_holder_ints as (
	select 
	case when
		dates_active @> ARRAY[DATE(series_date)] --check if the date is in the array
		then cast(POW(2, 32 - (date - DATE(series_date))) as bigint) --convert int in potencia de 2
	else 0 
	end  AS placeholder_int_value,
	*
	from users
	cross join series
)
select user_id,
SUM(placeholder_int_value),
cast(cast(SUM(placeholder_int_value) as BIGINT) as BIT(32)),
BIT_COUNT(cast(cast(SUM(placeholder_int_value) as BIGINT) as BIT(32))) > 0 as dim_is_monthly_active, --si es mayor a 0 estuvo activo
BIT_COUNT(cast('1111111000000000000000000000000' as bit(32)) & cast(cast(SUM(placeholder_int_value) as BIGINT) as BIT(32))) > 0 as dim_is_weekly_active, --utilizar como referencia 7bits validos
BIT_COUNT(cast('1000000000000000000000000000000' as bit(32)) & cast(cast(SUM(placeholder_int_value) as BIGINT) as BIT(32))) > 0 as dim_is_daily_active --utilizar como referencia 1 bit validos
from place_holder_ints
group by user_id 