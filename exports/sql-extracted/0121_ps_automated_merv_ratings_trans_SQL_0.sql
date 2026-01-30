-- Job: ps_automated_merv_ratings_trans
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/data_loaders
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4316273
-- Component ID: 4316311

with base_records as (
    select
        case when trim(lower("manufacturing location")) = 'new kensington, pa'      then 'New Kensington, PA'
            when trim(lower("manufacturing location")) = 'ogden, ut'                then 'Ogden, UT'
            when trim(lower("manufacturing location")) = 'talladega, al (tms)'      then 'Talladega, AL (TMS)'
            when trim(lower("manufacturing location")) = 'talladega, al (newberry)' then 'Talladega, AL (Newberry)'
            when trim(lower("manufacturing location")) = 'talladega, al (pope)'     then 'Talladega, AL (Pope)'
            when trim(lower("manufacturing location")) = 'talladega, al (woodland)' then 'Talladega, AL (Woodland)' end as mapped_manufacturing_location
        ,case when trim(lower("manufacturing location")) = 'new kensington, pa'     then 'pittsburgh'
            when trim(lower("manufacturing location")) = 'ogden, ut'                then 'slc'
            when trim(lower("manufacturing location")) = 'talladega, al (tms)'      then null
            when trim(lower("manufacturing location")) = 'talladega, al (newberry)' then 'newberry'
            when trim(lower("manufacturing location")) = 'talladega, al (pope)'     then 'pope'
            when trim(lower("manufacturing location")) = 'talladega, al (woodland)' then null end as manufacturing_sb_alias
        ,case when trim(lower("merv rating")) = 'merv 8' then 'MERV 8'
            when trim(lower("merv rating")) = 'merv 11' then 'MERV 11'
            when trim(lower("merv rating")) = 'merv 13' then 'MERV 13'
            when trim(lower("merv rating")) = 'odor eliminator' then 'MERV 8 Odor Eliminator' end as merv_rating
    from ${stg1_schema}.ps_automated_merv_ratings
    where trim(lower("manufacturing location")) in ('new kensington, pa', 'ogden, ut', 'talladega, al (tms)', 'talladega, al (newberry)', 'talladega, al (pope)', 'talladega, al (woodland)')
        and trim(lower("merv rating")) in ('merv 8', 'merv 11', 'merv 13', 'odor eliminator')
)

select
    mapped_manufacturing_location
    ,manufacturing_sb_alias
    ,merv_rating
    ,row_number() over (partition by mapped_manufacturing_location, merv_rating) as rn
from base_records