{{ config(materialized='table') }}

--{% set teams = ['Corinthians', 'Santos'] %}

{% set get_teams_query %}
select distinct
visitante
from {{ref("match-results")}}
order by 1
{% endset %}

{% set results = run_query(get_teams_query) %}

{% if execute %}
{% set teams = results.columns[0].values() %}
{% endif %}


{% for team in teams %}
(
WITH  match_results_club as (
  SELECT *
  from {{ ref("match-results") }}
  where mandante = '{{team}}' or visitante = '{{team}}'
)

, intermediate_join as (
  SELECT artist,album,release_date, min(m.data) as closest_game_date
  FROM {{ ref("album-releases_raw") }} a
  LEFT JOIN match_results_club m on a.release_date <= m.data
  GROUP BY 1,2,3
)

SELECT 
'{{team}}' as refered_team,
a.*,
m.*,
DATE_DIFF(m.data, a.release_date,  DAY) as days_between_release_and_match,
DATE_DIFF(m.data, a.release_date, DAY) < 7 as match_within_7d
FROM {{ ref("album-releases_raw") }} a
LEFT JOIN intermediate_join ij on ij.release_date = a.release_date and ij.album = a.album and ij.artist = a.artist
LEFT JOIN match_results_club m on ij.closest_game_date = m.data
where a.release_date BETWEEN '2003-03-22' and '2022-11-14'
ORDER BY a.release_date desc
)
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}








