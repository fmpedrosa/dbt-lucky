
{{ config(materialized='table') }}

SELECT *,
album_count_W+album_count_L+album_count_D as  total,
album_count_W/(album_count_W+album_count_L+album_count_D) as  win_rate,
(album_count_W+album_count_D)/(album_count_W+album_count_L+album_count_D) as  inv_rate,

FROM
(
  -- #1 from_item
  SELECT 
    refered_team,
    artist,
    album,
    result_refered_team,
  FROM {{ref("master-table")}}
)
PIVOT
(
  -- #2 aggregate
  COUNT(album) AS album_count
  -- #3 pivot_column
  FOR result_refered_team in ('W','L','D')
)