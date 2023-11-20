{{ config(materialized='table') }}


SELECT * 
FROM (
    SELECT 
    *, 
    count(album) OVER (PARTITION BY  refered_team, artist) as number_of_matched_albums_by_artist
    FROM {{ref("master-table-raw")}}
    WHERE match_within_7d is True and ID is not null
    )
WHERE number_of_matched_albums_by_artist >=5