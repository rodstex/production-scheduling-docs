-- Job: ps_manufacturing_to_distribution_center_trans
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/data_loaders
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4312637
-- Component ID: 4312679

with base_records as (
    select
        case when trim(lower("manufacturing location")) = 'new kensington, pa'      then 'New Kensington, PA'
            when trim(lower("manufacturing location")) = 'ogden, ut'                then 'Ogden, UT'
            when trim(lower("manufacturing location")) = 'talladega, al (tms)'      then 'Talladega, AL (TMS)'
            when trim(lower("manufacturing location")) = 'talladega, al (newberry)' then 'Talladega, AL (Newberry)'
            when trim(lower("manufacturing location")) = 'talladega, al (pope)'     then 'Talladega, AL (Pope)'
            when trim(lower("manufacturing location")) = 'talladega, al (woodland)' then 'Talladega, AL (Woodland)' end as mapped_manufacturing_location
        , case when trim(lower("manufacturing location")) = 'new kensington, pa'     then 'pittsburgh'
            when trim(lower("manufacturing location")) = 'ogden, ut'                then 'slc'
            when trim(lower("manufacturing location")) = 'talladega, al (tms)'      then null
            when trim(lower("manufacturing location")) = 'talladega, al (newberry)' then 'newberry'
            when trim(lower("manufacturing location")) = 'talladega, al (pope)'     then 'pope'
            when trim(lower("manufacturing location")) = 'talladega, al (woodland)' then null end as manufacturing_sb_alias
        ,case when trim(lower("distribution center")) = 'new kensington, pa'        then 'New Kensington, PA'
            when trim(lower("distribution center")) = 'ogden, ut'                   then 'Ogden, UT'
            when trim(lower("distribution center")) = 'fresno, ca'                  then 'Fresno, CA'
            when trim(lower("distribution center")) = 'talladega, al (newberry)'    then 'Talladega, AL (Newberry)'
            when trim(lower("distribution center")) = 'talladega, al (pope)'        then 'Talladega, AL (Pope)'
            when trim(lower("distribution center")) = 'dallas, tx'                  then 'Dallas, TX'
            when trim(lower("distribution center")) = 'orlando, fl'                 then 'Orlando, FL'
            when trim(lower("distribution center")) = 'elgin, il'                   then 'Elgin, IL'
            when trim(lower("distribution center")) = 'toronto, on'                 then 'Toronto, ON' end as mapped_distribution_location
        ,case when trim(lower("distribution center")) = 'new kensington, pa'        then 'pittsburgh'
            when trim(lower("distribution center")) = 'ogden, ut'                   then 'slc'
            when trim(lower("distribution center")) = 'fresno, ca'                  then 'fresno'
            when trim(lower("distribution center")) = 'talladega, al (newberry)'    then 'newberry'
            when trim(lower("distribution center")) = 'talladega, al (pope)'        then 'pope'
            when trim(lower("distribution center")) = 'dallas, tx'                  then 'dallas'
            when trim(lower("distribution center")) = 'orlando, fl'                 then 'orlando'
            when trim(lower("distribution center")) = 'elgin, il'                   then 'chicago'
            when trim(lower("distribution center")) = 'toronto, on'                 then 'toronto' end as distribution_sb_alias
        ,"in-transit days to distribution center"::int as in_transit_days_to_distribution_center
    from ${stg1_schema}.ps_manfacturing_to_distribution_center
    where trim(lower("manufacturing location")) in ('new kensington, pa', 'ogden, ut', 'talladega, al (tms)', 'talladega, al (newberry)', 'talladega, al (pope)', 'talladega, al (woodland)')
        and trim(lower("distribution center")) in ('new kensington, pa', 'ogden, ut', 'fresno, ca', 'talladega, al (newberry)', 'talladega, al (pope)', 'dallas, tx', 'orlando, fl', 'elgin, il','toronto, on')
        and "in-transit days to distribution center"::int >= 0
)

select
    mapped_manufacturing_location
    ,manufacturing_sb_alias
    ,mapped_distribution_location
    ,distribution_sb_alias
    ,in_transit_days_to_distribution_center
    ,row_number() over (partition by mapped_manufacturing_location, mapped_distribution_location order by in_transit_days_to_distribution_center asc) as rn
from base_records