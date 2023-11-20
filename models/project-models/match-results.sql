
{{ config(materialized='table') }}

select *,
case when vencedor = '-' then 1 else 0 end as is_draw,
case when vencedor = mandante then 'mandante' 
     when vencedor = '-' then 'empate'
    else 'visitante' end as vencendor

FROM  {{ ref("match-results_raw") }}