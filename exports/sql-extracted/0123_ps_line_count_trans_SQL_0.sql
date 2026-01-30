-- Job: ps_line_count_trans
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/data_loaders
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4316670
-- Component ID: 4316690

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
        ,case when trim(lower("line type")) = 'automated' then 'Automated'
            when trim(lower("line type")) = 'single loader' then 'Single Loader'
            when trim(lower("line type")) = 'double loader' then 'Double Loader'
            when trim(lower("line type")) = 'manual' then 'Manual' end as line_type
        ,"count of lines"::int as count_of_lines
    from ${stg1_schema}.ps_line_count
    where trim(lower("manufacturing location")) in ('new kensington, pa', 'ogden, ut', 'talladega, al (tms)', 'talladega, al (newberry)', 'talladega, al (pope)', 'talladega, al (woodland)')
        and trim(lower("line type")) in ('automated', 'single loader', 'double loader', 'manual')
        and "count of lines"::int >= 0
)

select
    mapped_manufacturing_location
    ,manufacturing_sb_alias
    ,line_type
    ,count_of_lines
    ,row_number() over (partition by mapped_manufacturing_location, line_type order by count_of_lines asc) as rn
from base_records