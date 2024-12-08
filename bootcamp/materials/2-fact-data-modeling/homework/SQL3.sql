-- Datelist_integer query

with users as 
(  select * from user_devices_cumulated udc
where active_date = date('2023-01-31')
),
series as 
(
select * from generate_series(DATE('2023-01-01'),DATE('2023-01-31'),interval '1 day') as series_date
),
place_holder_ints as 
(
select 
	case when 
		device_activity_datelist @> array[DATE(series_date)] -- This creates a boolean column and checks if the dates_active array has a value equal to the series_date value. so of they are active 
	then cast(pow(2, 32 - (active_date - DATE(series_date))) as bigint)-- This is the number of days between the current date which is filtered to the 31st and the series date generated from the gernerate_series query.
	else 0
	end as placeholder_int_value,
	*
	from users cross join series  -- combines users and series and creates a placeholder_int_value for each user and date. Its based on whether a user was active on that particular date
)
select user_id,browser_type,
cast(cast(sum(placeholder_int_value) as bigint) as bit(32)) as datelist_int,
bit_count(cast(cast(sum(placeholder_int_value) as bigint) as bit(32)))
from place_holder_ints
group by user_id,browser_type