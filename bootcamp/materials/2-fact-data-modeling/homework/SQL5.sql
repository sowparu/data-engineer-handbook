-- DDL for host_activity_reduced

create table host_activity_reduced
(
host text,
month_start date,
metric_array1 real[],
metric_array2 real[],
primary key (host, month_start)
);

-- select * from events;

--truncate table host_activity_reduced

-- Incremental query to load host_activity_reduced

insert into host_activity_reduced
with daily_aggregate as (
select 
	host,
	date(event_time) as date,
	count(1) as hits,
	count(distinct user_id) as unique_visitors
from events 
where date(event_time) = date('2023-01-05')
and user_id is not null
group by 1,2
),
yesterday_array as (
select * from host_activity_reduced
where month_start = date('2023-01-01')
)
select 
	coalesce( da.host, ya.host) as host,
	coalesce(ya.month_start,date_trunc('month', da.date)) as month_start,-- first day of the month always. 
	case 
		when ya.metric_array1 is not null
			then ya.metric_array1 || array[coalesce(da.hits,0)] 
		when ya.metric_array1 is null 
			then ARRAY_FILL(0, array[coalesce(date - date(date_trunc('month',date)), 0)]) || array[coalesce(da.hits,0)] 
		end as metric_array1,
	case 
		when ya.metric_array2 is not null
			then ya.metric_array2 || array[coalesce(da.unique_visitors,0)] 
		when ya.metric_array2 is null
			then ARRAY_FILL(0, array[coalesce(date - date(date_trunc('month',date)), 0)]) || array[coalesce(da.unique_visitors,0)] 
		end as metric_array2
	from daily_aggregate da
	full outer join yesterday_array ya on
	da.host = ya.host
	
		on conflict(host, month_start)
	do update set
		 metric_array1 = excluded.metric_array1,
		 metric_array2 = excluded.metric_array2

--select * from host_activity_reduced;










