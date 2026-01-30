-- Job: ps_staff_by_day_trans
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/data_loaders
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4317385
-- Component ID: 4317407

with base_records as (
    ------------
    -- Monday --
    ------------
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
        ,case when trim(lower("grouped line type")) = 'automated' then 'Automated'
            when trim(lower("grouped line type")) = 'non-automated' then 'Non-Automated' end as grouped_line_type
        ,'Monday'                       as day_of_week_name
        ,1                              as day_of_week_int
        ,"monday count of lines"::int   as count_of_lines
    from ${stg1_schema}.ps_staff_by_day
    where trim(lower("manufacturing location")) in ('new kensington, pa', 'ogden, ut', 'talladega, al (tms)', 'talladega, al (newberry)', 'talladega, al (pope)', 'talladega, al (woodland)')
        and trim(lower("grouped line type")) in ('automated', 'non-automated')
    union all
    -------------
    -- Tuesday --
    -------------
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
        ,case when trim(lower("grouped line type")) = 'automated' then 'Automated'
            when trim(lower("grouped line type")) = 'non-automated' then 'Non-Automated' end as grouped_line_type
        ,'Tuesday'                      as day_of_week_name
        ,2                              as day_of_week_int
        ,"tuesday count of lines"::int  as count_of_lines
    from ${stg1_schema}.ps_staff_by_day
    where trim(lower("manufacturing location")) in ('new kensington, pa', 'ogden, ut', 'talladega, al (tms)', 'talladega, al (newberry)', 'talladega, al (pope)', 'talladega, al (woodland)')
        and trim(lower("grouped line type")) in ('automated', 'non-automated')
    union all
    ---------------
    -- Wednesday --
    ---------------
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
        ,case when trim(lower("grouped line type")) = 'automated' then 'Automated'
            when trim(lower("grouped line type")) = 'non-automated' then 'Non-Automated' end as grouped_line_type
        ,'Wednesday'                        as day_of_week_name
        ,3                                  as day_of_week_int
        ,"wednesday count of lines"::int    as count_of_lines
    from ${stg1_schema}.ps_staff_by_day
    where trim(lower("manufacturing location")) in ('new kensington, pa', 'ogden, ut', 'talladega, al (tms)', 'talladega, al (newberry)', 'talladega, al (pope)', 'talladega, al (woodland)')
        and trim(lower("grouped line type")) in ('automated', 'non-automated')
    union all
    --------------
    -- Thursday --
    --------------
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
        ,case when trim(lower("grouped line type")) = 'automated' then 'Automated'
            when trim(lower("grouped line type")) = 'non-automated' then 'Non-Automated' end as grouped_line_type
        ,'Thursday'                     as day_of_week_name
        ,4                              as day_of_week_int
        ,"thursday count of lines"::int as count_of_lines
    from ${stg1_schema}.ps_staff_by_day
    where trim(lower("manufacturing location")) in ('new kensington, pa', 'ogden, ut', 'talladega, al (tms)', 'talladega, al (newberry)', 'talladega, al (pope)', 'talladega, al (woodland)')
        and trim(lower("grouped line type")) in ('automated', 'non-automated')
    union all
    ------------
    -- Friday --
    ------------
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
        ,case when trim(lower("grouped line type")) = 'automated' then 'Automated'
            when trim(lower("grouped line type")) = 'non-automated' then 'Non-Automated' end as grouped_line_type
        ,'Friday'                       as day_of_week_name
        ,5                              as day_of_week_int
        ,"friday count of lines"::int   as count_of_lines
    from ${stg1_schema}.ps_staff_by_day
    where trim(lower("manufacturing location")) in ('new kensington, pa', 'ogden, ut', 'talladega, al (tms)', 'talladega, al (newberry)', 'talladega, al (pope)', 'talladega, al (woodland)')
        and trim(lower("grouped line type")) in ('automated', 'non-automated')
    union all
    --------------
    -- Saturday --
    --------------
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
        ,case when trim(lower("grouped line type")) = 'automated' then 'Automated'
            when trim(lower("grouped line type")) = 'non-automated' then 'Non-Automated' end as grouped_line_type
        ,'Saturday'                     as day_of_week_name
        ,6                              as day_of_week_int
        ,"saturday count of lines"::int as count_of_lines
    from ${stg1_schema}.ps_staff_by_day
    where trim(lower("manufacturing location")) in ('new kensington, pa', 'ogden, ut', 'talladega, al (tms)', 'talladega, al (newberry)', 'talladega, al (pope)', 'talladega, al (woodland)')
        and trim(lower("grouped line type")) in ('automated', 'non-automated')
    union all
    ------------
    -- Sunday --
    ------------
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
        ,case when trim(lower("grouped line type")) = 'automated' then 'Automated'
            when trim(lower("grouped line type")) = 'non-automated' then 'Non-Automated' end as grouped_line_type
        ,'Sunday'                       as day_of_week_name
        ,0                              as day_of_week_int
        ,"sunday count of lines"::int   as count_of_lines
    from ${stg1_schema}.ps_staff_by_day
    where trim(lower("manufacturing location")) in ('new kensington, pa', 'ogden, ut', 'talladega, al (tms)', 'talladega, al (newberry)', 'talladega, al (pope)', 'talladega, al (woodland)')
        and trim(lower("grouped line type")) in ('automated', 'non-automated')
)

select
    mapped_manufacturing_location
    ,manufacturing_sb_alias
    ,grouped_line_type
    ,day_of_week_name
    ,day_of_week_int
    ,count_of_lines
    ,row_number() over (partition by mapped_manufacturing_location, grouped_line_type, day_of_week_name order by count_of_lines asc) as rn
from base_records