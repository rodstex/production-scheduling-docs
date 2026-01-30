-- Job: ps_target_days_of_inventory_trans
-- Path: ROOT/filterbuy_dw/dwh/jobs/production_schedule_v4/data_loaders
-- Component: SQL 0
-- Type: SQL Query
-- Job ID: 4315890
-- Component ID: 4315913

with base_non_automated as (
    ----------------
    -- 1" Pleated --
    ----------------
    select
        case when trim(lower("distribution center")) = 'new kensington, pa'         then 'New Kensington, PA'
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
        ,'Non-Automated'    as grouped_line_type
        ,'1" Pleated'       as filter_type
        ,"one inch pleated"::text as target_days_of_inventory
    from ${stg1_schema}.ps_target_days_of_inventory_non_automated
    where trim(lower("distribution center")) in ('new kensington, pa', 'ogden, ut', 'fresno, ca', 'talladega, al (newberry)', 'talladega, al (pope)', 'dallas, tx', 'orlando, fl', 'elgin, il','toronto, on')
    union all
    ----------------
    -- 2" Pleated --
    ----------------
    select
        case when trim(lower("distribution center")) = 'new kensington, pa'         then 'New Kensington, PA'
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
        ,'Non-Automated'    as grouped_line_type
        ,'2" Pleated'       as filter_type
        ,"two inch pleated"::text as target_days_of_inventory
    from ${stg1_schema}.ps_target_days_of_inventory_non_automated
    where trim(lower("distribution center")) in ('new kensington, pa', 'ogden, ut', 'fresno, ca', 'talladega, al (newberry)', 'talladega, al (pope)', 'dallas, tx', 'orlando, fl', 'elgin, il','toronto, on')
    union all
    ----------------
    -- 4" Pleated --
    ----------------
    select
        case when trim(lower("distribution center")) = 'new kensington, pa'         then 'New Kensington, PA'
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
        ,'Non-Automated'        as grouped_line_type
        ,'4" Pleated'           as filter_type
        ,"four inch pleated"::text    as target_days_of_inventory
    from ${stg1_schema}.ps_target_days_of_inventory_non_automated
    where trim(lower("distribution center")) in ('new kensington, pa', 'ogden, ut', 'fresno, ca', 'talladega, al (newberry)', 'talladega, al (pope)', 'dallas, tx', 'orlando, fl', 'elgin, il','toronto, on')
    union all
    -----------------
    -- Whole-House --
    -----------------
    select
        case when trim(lower("distribution center")) = 'new kensington, pa'         then 'New Kensington, PA'
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
        ,'Non-Automated'    as grouped_line_type
        ,'Whole-House'      as filter_type
        ,"whole-house"::text      as target_days_of_inventory
    from ${stg1_schema}.ps_target_days_of_inventory_non_automated
    where trim(lower("distribution center")) in ('new kensington, pa', 'ogden, ut', 'fresno, ca', 'talladega, al (newberry)', 'talladega, al (pope)', 'dallas, tx', 'orlando, fl', 'elgin, il','toronto, on')
)

, base_automated as (
    ----------------
    -- 1" Pleated --
    ----------------
    select
        case when trim(lower("distribution center")) = 'new kensington, pa'         then 'New Kensington, PA'
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
        ,'Automated'        as grouped_line_type
        ,'1" Pleated'       as filter_type
        ,"one inch pleated"::text as target_days_of_inventory
    from ${stg1_schema}.ps_target_days_of_inventory_automated
    where trim(lower("distribution center")) in ('new kensington, pa', 'ogden, ut', 'fresno, ca', 'talladega, al (newberry)', 'talladega, al (pope)', 'dallas, tx', 'orlando, fl', 'elgin, il','toronto, on')
    union all
    ----------------
    -- 2" Pleated --
    ----------------
    select
        case when trim(lower("distribution center")) = 'new kensington, pa'         then 'New Kensington, PA'
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
        ,'Automated'        as grouped_line_type
        ,'2" Pleated'       as filter_type
        ,"two inch pleated"::text as target_days_of_inventory
    from ${stg1_schema}.ps_target_days_of_inventory_automated
    where trim(lower("distribution center")) in ('new kensington, pa', 'ogden, ut', 'fresno, ca', 'talladega, al (newberry)', 'talladega, al (pope)', 'dallas, tx', 'orlando, fl', 'elgin, il','toronto, on')
    union all
    ----------------
    -- 4" Pleated --
    ----------------
    select
        case when trim(lower("distribution center")) = 'new kensington, pa'         then 'New Kensington, PA'
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
        ,'Automated'            as grouped_line_type
        ,'4" Pleated'           as filter_type
        ,"four inch pleated"::text    as target_days_of_inventory
    from ${stg1_schema}.ps_target_days_of_inventory_automated
    where trim(lower("distribution center")) in ('new kensington, pa', 'ogden, ut', 'fresno, ca', 'talladega, al (newberry)', 'talladega, al (pope)', 'dallas, tx', 'orlando, fl', 'elgin, il','toronto, on')
    union all
    -----------------
    -- Whole-House --
    -----------------
    select
        case when trim(lower("distribution center")) = 'new kensington, pa'         then 'New Kensington, PA'
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
        ,'Automated'        as grouped_line_type
        ,'Whole-House'      as filter_type
        ,"whole-house"::text      as target_days_of_inventory
    from ${stg1_schema}.ps_target_days_of_inventory_automated
    where trim(lower("distribution center")) in ('new kensington, pa', 'ogden, ut', 'fresno, ca', 'talladega, al (newberry)', 'talladega, al (pope)', 'dallas, tx', 'orlando, fl', 'elgin, il','toronto, on')
)

, base_excess_distribution as (
    ----------------
    -- 1" Pleated --
    ----------------
    select
        case when trim(lower("distribution center")) = 'new kensington, pa'         then 'New Kensington, PA'
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
        ,'Excess Distribution'  as grouped_line_type
        ,'1" Pleated'           as filter_type
        ,"one inch pleated"::text     as target_days_of_inventory
    from ${stg1_schema}.ps_target_days_of_inventory_excess_distribution
    where trim(lower("distribution center")) in ('new kensington, pa', 'ogden, ut', 'fresno, ca', 'talladega, al (newberry)', 'talladega, al (pope)', 'dallas, tx', 'orlando, fl', 'elgin, il','toronto, on')
    union all
    ----------------
    -- 2" Pleated --
    ----------------
    select
        case when trim(lower("distribution center")) = 'new kensington, pa'         then 'New Kensington, PA'
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
        ,'Excess Distribution'  as grouped_line_type
        ,'2" Pleated'           as filter_type
        ,"two inch pleated"::text     as target_days_of_inventory
    from ${stg1_schema}.ps_target_days_of_inventory_excess_distribution
    where trim(lower("distribution center")) in ('new kensington, pa', 'ogden, ut', 'fresno, ca', 'talladega, al (newberry)', 'talladega, al (pope)', 'dallas, tx', 'orlando, fl', 'elgin, il','toronto, on')
    union all
    ----------------
    -- 4" Pleated --
    ----------------
    select
        case when trim(lower("distribution center")) = 'new kensington, pa'         then 'New Kensington, PA'
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
        ,'Excess Distribution'  as grouped_line_type
        ,'4" Pleated'           as filter_type
        ,"four inch pleated"::text    as target_days_of_inventory
    from ${stg1_schema}.ps_target_days_of_inventory_excess_distribution
    where trim(lower("distribution center")) in ('new kensington, pa', 'ogden, ut', 'fresno, ca', 'talladega, al (newberry)', 'talladega, al (pope)', 'dallas, tx', 'orlando, fl', 'elgin, il','toronto, on')
    union all
    -----------------
    -- Whole-House --
    -----------------
    select
        case when trim(lower("distribution center")) = 'new kensington, pa'         then 'New Kensington, PA'
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
        ,'Excess Distribution'  as grouped_line_type
        ,'Whole-House'          as filter_type
        ,"whole-house"::text          as target_days_of_inventory
    from ${stg1_schema}.ps_target_days_of_inventory_excess_distribution
    where trim(lower("distribution center")) in ('new kensington, pa', 'ogden, ut', 'fresno, ca', 'talladega, al (newberry)', 'talladega, al (pope)', 'dallas, tx', 'orlando, fl', 'elgin, il','toronto, on')
)

, base_records as (
    select
        mapped_distribution_location
        ,distribution_sb_alias
        ,grouped_line_type
        ,filter_type
        ,target_days_of_inventory::int as target_days_of_inventory
    from base_non_automated
    where target_days_of_inventory in ('1','2','3','4','5','6','7','8','9','10'
                                      ,'11','12','13','14','15','16','17','18','19','20'
                                      ,'21','22','23','24','25','26','27','28','29','30')
    union all
    select
        mapped_distribution_location
        ,distribution_sb_alias
        ,grouped_line_type
        ,filter_type
        ,target_days_of_inventory::int as target_days_of_inventory
    from base_automated
    where target_days_of_inventory in ('1','2','3','4','5','6','7','8','9','10'
                                      ,'11','12','13','14','15','16','17','18','19','20'
                                      ,'21','22','23','24','25','26','27','28','29','30')
    union all
    select
        mapped_distribution_location
        ,distribution_sb_alias
        ,grouped_line_type
        ,filter_type
        ,target_days_of_inventory::int as target_days_of_inventory
    from base_excess_distribution
    where target_days_of_inventory in ('1','2','3','4','5','6','7','8','9','10'
                                      ,'11','12','13','14','15','16','17','18','19','20'
                                      ,'21','22','23','24','25','26','27','28','29','30')
)

select
    mapped_distribution_location
    ,distribution_sb_alias
    ,grouped_line_type
    ,filter_type
    ,target_days_of_inventory
    ,row_number() over (partition by mapped_distribution_location, grouped_line_type, filter_type order by target_days_of_inventory asc) as rn
from base_records