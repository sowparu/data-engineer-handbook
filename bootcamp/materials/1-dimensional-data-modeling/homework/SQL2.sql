--select * from actors;

create table actors_history_scd (
 actor text,
 quality_class quality_class,
 is_active boolean,
 start_date integer,
 end_date integer,
 primary key(actor,start_date)
 )
 
 insert into actors_history_scd
 with first_step as (select actor, 
 		current_year, 
 		quality_class,
 		is_active,
 		(lag(quality_class,1) over (partition by actor order by current_year) <> quality_class) or 
 		(lag(quality_class,1) over (partition by actor order by current_year) is null) as change_in_quality_class,
 		(lag(is_active,1) over (partition by actor order by current_year) <> is_active) or 
 		(lag(is_active,1) over (partition by actor order by current_year) is null) as change_in_is_active
 from actors),
 second_step as (SELECT
	actor,
    current_year,
	quality_class,
	is_active,
	CASE WHEN change_in_quality_class OR change_in_is_active 
		THEN 1 ELSE 0 
	end as actor_stat_change_ind
	--sum(actor_stat_change_ind) over (partition by actor order by year) as actor_stat_change
FROM first_step),
third_step as (
select 
actor,
    current_year,
	quality_class,
	is_active,
	sum(actor_stat_change_ind) over (partition by actor order by current_year) as actor_stat_change
from second_step)
select actor,
		quality_class,
		is_active,
		min(current_year) as start_date,
		max(current_year) as end_date
from third_step
group by actor,quality_class,is_active
order by 1,4,5
