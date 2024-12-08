--drop type films cascade;

create type films as (
year integer,
film text,
votes real,
rating real,
filmid text
)

--Creates ENUM for quality_class
create type quality_class as enum ('star','good','average','bad')

--drop table actors

---- DDL for actors table
create table actors (
actor text,
actorid text,
current_year  integer,
films films[],
quality_class quality_class,
is_active boolean,
primary key (actorid, current_year)
)

insert into actors
with last_year as (
select * from actors 
where current_year = 1979
),
current_year as (
select actor, 
		actorid, 
		year,
		array_agg(
			array
				[row(year,film,votes,rating,filmid)::films]
				)
				as films,
		avg(rating) as rating
from actor_films 
where year = 1980
group by actor,actorid,year)
select 
coalesce(c.actor,l.actor) as actor,
coalesce(c.actorid,l.actorid) as actorid,
coalesce(c.year,l.current_year+1) as current_year,
coalesce(l.films,array[]::films[]) ||
	case when c.year is not null then c.films
	else array[]::films[]
	end as films,
case 
	when c.year is not null then
		case 
           WHEN c.rating > 8 THEN 'star'::quality_class
           WHEN c.rating BETWEEN 7 AND 8 THEN 'good'::quality_class
           WHEN c.rating BETWEEN 6 AND 7 THEN 'average'::quality_class
           ELSE 'bad'::quality_class
       	end
     else l.quality_class
end as quality_class,
c.year is not null as is_active
from current_year c
full outer join last_year l
on c.actor = l.actor

--select * from actors where current_year = 1973



