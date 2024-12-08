-- drop table user_devices_cumulated

-- DDL for user_devices_cumulated
create table user_devices_cumulated(
user_id text,
browser_type varchar(50),
active_date date,
device_activity_datelist DATE[],
primary key (user_id, browser_type, active_date)
)

-- truncate table user_devices_cumulated

-- Cumulative Query
insert into user_devices_cumulated 

with yesterday as (
select user_id::text,browser_type,active_date,device_activity_datelist
from user_devices_cumulated
where active_date = DATE('2023-01-03') 
),
today as (select 
url,
referrer, 
user_id, 
device_id,
host,
date(cast(event_time as timestamp)) as date_active
from events
where date(cast(event_time as timestamp)) = date('2023-01-04')
),
events_deduped as (
select user_id,date_active,device_id,
row_number() over (partition by user_id, date_active) as rownum
from today
),
events_new as (
select user_id,date_active,device_id
from events_deduped where rownum = 1),
devices_deduped as (
select device_id,
browser_type,
browser_version_major, 
browser_version_patch,
device_type,
device_version_major,
device_version_patch,
os_type,
os_version_major,
os_version_minor,
os_version_patch,
row_number() over (partition by 
device_id,
browser_type,
browser_version_major, 
browser_version_patch,
device_type,
device_version_major,
device_version_patch,
os_type,
os_version_major,
os_version_minor,
os_version_patch) as rownum
from devices),
devices_new as (
select device_id, browser_type 
from devices_deduped
where rownum = 1),
final as 
(select e.user_id::text, e.date_active, d.browser_type 
from events_new e
join devices_new d
on e.device_id = d.device_id
where user_id is not null)
select
coalesce(t.user_id,y.user_id) as user_id,
coalesce(t.browser_type,y.browser_type) as browser_type,
--coalesce(t.date_active, y.active_date) as active_date,
coalesce(t.date_active, y.active_date + interval '1 day') as active_date,
case when y.device_activity_datelist is null then array[t.date_active] -- if no activity for yesterday, create dates for today
	 when t.date_active is null then y.device_activity_datelist-- if no activity for today, just keep yesterday's data
	 else array[t.date_active] || y.device_activity_datelist -- if both are not null, do a concat
end as device_activity_datelist
from final t
full outer join yesterday y
on t.user_id = y.user_id























	




