--DDL for struct actors_scd_type

--drop type actors_scd_type cascade;

CREATE TYPE actors_scd_type AS (
                    quality_class quality_class,
                    is_active boolean,
                    start_date INTEGER,
                    end_date INTEGER
                    )

insert into actors_history_scd 
WITH last_season_scd AS (
    SELECT * FROM actors_history_scd
    WHERE start_date = 2020
    AND end_date = 2020
),
     historical_scd AS (
        select *
        FROM actors_history_scd
        WHERE start_date = 2020
        AND end_date < 2020
     ),
     this_season_data AS (
         SELECT * FROM actors_history_scd
         WHERE start_date = 2021
     ),
     unchanged_records AS (
         SELECT
                ts.actor,
                ts.quality_class,
                ts.is_active,
                ls.start_date,
                ts.start_date as end_date
        FROM this_season_data ts
        JOIN last_season_scd ls
        ON ls.actor = ts.actor
         WHERE ts.quality_class = ls.quality_class
         AND ts.is_active = ls.is_active
     ),
     changed_records AS (
        SELECT
                ts.actor,
                UNNEST(ARRAY[
                    ROW(
                        ls.quality_class,
                        ls.is_active,
                        ls.start_date,
                        ls.end_date
                        )::actors_scd_type,
                    ROW(
                        ts.quality_class,
                        ts.is_active,
                        ts.start_date,
                        ts.end_date
                        )::actors_scd_type
                ]) as records
        FROM this_season_data ts
        LEFT JOIN last_season_scd ls
        ON ls.actor= ts.actor
         WHERE (ts.quality_class <> ls.quality_class
          OR ts.is_active <> ls.is_active)
     ),  
     unnested_changed_records AS (

         SELECT actor,
                (records::actors_scd_type).quality_class,
                (records::actors_scd_type).is_active,
                (records::actors_scd_type).start_date,
                (records::actors_scd_type).end_date
                FROM changed_records
         ),
     new_records AS (

         SELECT
            ts.actor,
                ts.quality_class,
                ts.is_active,
                ts.start_date AS start_season,
                ts.end_date AS end_season
         FROM this_season_data ts
         LEFT JOIN last_season_scd ls
             ON ts.actor = ls.actor
         WHERE ls.actor IS NULL

     )


SELECT *  FROM (
                  SELECT *
                  FROM historical_scd

                  UNION ALL

                  SELECT *
                  FROM unchanged_records

                  UNION ALL

                  SELECT *
                  FROM unnested_changed_records

                  UNION ALL

                  SELECT *
                  FROM new_records
              ) a;
              
 SELECT * FROM actors_scd_type;