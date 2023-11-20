
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

SELECT artist,album,notes,link,PARSE_DATE('%m/%d/%Y', date) as release_date   FROM `luckycharm23.schemas.album-releases` r where date != 'nan'

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
