-- Job: ps_capacity_by_sku_trans
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/data_loaders
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4318216
-- Component ID: 4318241

with base_records as (
    ------------------------
    -- New Kensington, PA --
    ------------------------
    select
        trim(upper("sku without merv rating")) as sku_without_merv_rating
        ,case when trim(lower("line type")) = 'automated'    then 'Automated'
            when trim(lower("line type")) = 'single loader' then 'Single Loader'
            when trim(lower("line type")) = 'double loader' then 'Double Loader'
            when trim(lower("line type")) = 'manual'        then 'Manual' end as line_type
        ,'New Kensington, PA'                           as mapped_manufacturing_location
        ,'pittsburgh'                                   as manufacturing_sb_alias
        ,"new kensington rates (units/10hrs)"::int      as production_capacity
        ,"new kensington automated tooling sets"::int   as auto_tooling_sets
    from ${stg1_schema}.ps_capacity_by_sku
    where trim(lower("line type")) in ('automated','double loader','single loader','manual')
        and "new kensington rates (units/10hrs)"::int > 0
--         and case when trim(lower("line type")) = 'automated' and "new kensington automated tooling sets" > 0 then true
--             when trim(lower("line type")) != 'automated' then true
--             else false end
    union all
    ---------------
    -- Ogden, UT --
    ---------------
    select
        trim(upper("sku without merv rating")) as sku_without_merv_rating
        ,case when trim(lower("line type")) = 'automated'    then 'Automated'
            when trim(lower("line type")) = 'single loader' then 'Single Loader'
            when trim(lower("line type")) = 'double loader' then 'Double Loader'
            when trim(lower("line type")) = 'manual'        then 'Manual' end as line_type
        ,'Ogden, UT'                                    as mapped_manufacturing_location
        ,'slc'                                          as manufacturing_sb_alias
        ,"ogden rates (units/10hrs)"::int               as production_capacity
        ,"ogden automated tooling sets"::int            as auto_tooling_sets
    from ${stg1_schema}.ps_capacity_by_sku
    where trim(lower("line type")) in ('automated','double loader','single loader','manual')
        and "ogden rates (units/10hrs)"::int > 0
--         and case when trim(lower("line type")) = 'automated' and "ogden automated tooling sets" > 0 then true
--             when trim(lower("line type")) != 'automated' then true
--             else false end
    union all
    ------------------------------
    -- Talladega, AL (Newberry) --
    ------------------------------
    select
        trim(upper("sku without merv rating")) as sku_without_merv_rating
        ,case when trim(lower("line type")) = 'automated'    then 'Automated'
            when trim(lower("line type")) = 'single loader' then 'Single Loader'
            when trim(lower("line type")) = 'double loader' then 'Double Loader'
            when trim(lower("line type")) = 'manual'        then 'Manual' end as line_type
        ,'Talladega, AL (Newberry)'                     as mapped_manufacturing_location
        ,'newberry'                                     as manufacturing_sb_alias
        ,"talladega newberry rates (units/10hrs)"::int  as production_capacity
        ,null::int                                      as auto_tooling_sets
    from ${stg1_schema}.ps_capacity_by_sku
    where trim(lower("line type")) in ('double loader','single loader','manual')
        and "talladega newberry rates (units/10hrs)"::int > 0
    union all
    --------------------------
    -- Talladega, AL (Pope) --
    --------------------------
    select
        trim(upper("sku without merv rating")) as sku_without_merv_rating
        ,case when trim(lower("line type")) = 'automated'    then 'Automated'
            when trim(lower("line type")) = 'single loader' then 'Single Loader'
            when trim(lower("line type")) = 'double loader' then 'Double Loader'
            when trim(lower("line type")) = 'manual'        then 'Manual' end as line_type
        ,'Talladega, AL (Pope)'                         as mapped_manufacturing_location
        ,'pope'                                         as manufacturing_sb_alias
        ,"talladega pope rates (units/10hrs)"::int      as production_capacity
        ,null::int                                      as auto_tooling_sets
    from ${stg1_schema}.ps_capacity_by_sku
    where trim(lower("line type")) in ('double loader','single loader','manual')
        and "talladega pope rates (units/10hrs)"::int > 0
    union all
    ------------------------------
    -- Talladega, AL (Woodland) --
    ------------------------------
    select
        trim(upper("sku without merv rating")) as sku_without_merv_rating
        ,case when trim(lower("line type")) = 'automated'    then 'Automated'
            when trim(lower("line type")) = 'single loader' then 'Single Loader'
            when trim(lower("line type")) = 'double loader' then 'Double Loader'
            when trim(lower("line type")) = 'manual'        then 'Manual' end as line_type
        ,'Talladega, AL (Woodland)'                     as mapped_manufacturing_location
        ,null::text                                     as manufacturing_sb_alias
        ,"talladega woodland rates (units/10hrs)"::int  as production_capacity
        ,null::int                                      as auto_tooling_sets
    from ${stg1_schema}.ps_capacity_by_sku
    where trim(lower("line type")) in ('double loader','single loader','manual')
        and "talladega woodland rates (units/10hrs)"::int > 0
    union all
    -------------------------
    -- Talladega, AL (TMS) --
    -------------------------
    select
        trim(upper("sku without merv rating")) as sku_without_merv_rating
        ,case when trim(lower("line type")) = 'automated'    then 'Automated'
            when trim(lower("line type")) = 'single loader' then 'Single Loader'
            when trim(lower("line type")) = 'double loader' then 'Double Loader'
            when trim(lower("line type")) = 'manual'        then 'Manual' end as line_type
        ,'Talladega, AL (TMS)'                          as mapped_manufacturing_location
        ,null::text                                     as manufacturing_sb_alias
        ,"talladega tms rates (units/10hrs)"::int       as production_capacity
        ,"talladega tms automated tooling sets"::int    as auto_tooling_sets
    from ${stg1_schema}.ps_capacity_by_sku
    where trim(lower("line type")) in ('automated','double loader','single loader','manual')
        and "talladega tms rates (units/10hrs)"::int > 0
--         and case when trim(lower("line type")) = 'automated' and "talladega tms automated tooling sets" > 0 then true
--             when trim(lower("line type")) != 'automated' then true
--             else false end
)

select
    sku_without_merv_rating
    ,line_type
    ,mapped_manufacturing_location
    ,manufacturing_sb_alias
    ,production_capacity
    ,case when line_type = 'Automated' then coalesce(auto_tooling_sets, 0) end as auto_tooling_sets
    ,row_number() over (partition by sku_without_merv_rating, line_type, mapped_manufacturing_location order by production_capacity asc) as rn
from base_records