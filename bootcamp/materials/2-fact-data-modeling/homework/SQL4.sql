-- DDL for hosts_cumulated

CREATE TABLE hosts_cumulated (
    host TEXT, 
    host_activity_datelist DATE[], 
    date_current DATE, 
    PRIMARY KEY (host,date_current) 
);

-- Incremental query runs one date at a time
   
 INSERT INTO hosts_cumulated (host, host_activity_datelist, date_current)
   WITH today AS (
    SELECT '2023-01-05'::date as today -- This allows to choose a date parameter
),
daily_data AS ( -- -- This gets all the events up to today and cartesians that with today
    SELECT DISTINCT
        host,
        DATE(event_time) as event_date
    FROM events e
    CROSS JOIN today t
    WHERE host IS NOT NULL 
    AND event_time IS NOT NULL
    AND DATE(event_time) <= t.today
),
aggregated_data AS ( -- next step is to aggregate the cartesioned data grouped by host
    SELECT 
        host,
        ARRAY_AGG(DISTINCT event_date ORDER BY event_date) as host_activity_datelist,
        (SELECT today FROM today) as date_current
    FROM daily_data
    GROUP BY host
)
SELECT -- data to be inserted
    host,
    host_activity_datelist,
    date_current
FROM aggregated_data

ON CONFLICT (host, date_current) -- postgres merge activity
DO UPDATE SET 
    host_activity_datelist = EXCLUDED.host_activity_datelist;
    
--select * from hosts_cumulated hc;
   
