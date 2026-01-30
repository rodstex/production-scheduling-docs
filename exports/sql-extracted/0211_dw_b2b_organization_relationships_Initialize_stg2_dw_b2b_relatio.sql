-- Job: dw_b2b_organization_relationships
-- Path: ROOT/filterbuy_dw/dwh/transformations
-- Component: Initialize stg2.dw_b2b_relationships_logic3
-- Type: SQL Query
-- Job ID: 8704741
-- Component ID: 8710091

/* Return billing addresses distinct to a customer */
with base_supplybuy_customers as (
    select
        normalized_billing_address
        ,normalized_billing_city
        ,normalized_billing_postal_code
    from ${stg2_schema}.sb_dashboard_business_customer
    where coalesce(normalized_billing_address, '') != ''
        and coalesce(normalized_billing_city, '') != ''
        and coalesce(normalized_billing_postal_code, '') != ''
    group by 1,2,3
    having count(business_customer_id) = 1
)

, supplybuy_billing_address as (
    select
        c.business_customer_id
        ,trim(lower(c.customer_name)) as customer_name
        ,bsc.normalized_billing_address
        ,bsc.normalized_billing_city
        ,bsc.normalized_billing_postal_code
    from base_supplybuy_customers bsc
        join ${stg2_schema}.sb_dashboard_business_customer c
             on c.normalized_billing_address = bsc.normalized_billing_address
            and c.normalized_billing_city = bsc.normalized_billing_city
            and c.normalized_billing_postal_code = bsc.normalized_billing_postal_code
)

, base_pipedrive_organizations as (
    select
        normalized_address
        ,normalized_city
        ,normalized_postal_code
    from ${stg2_schema}.pd_organizations
    where coalesce(normalized_address, '') != ''
        and coalesce(normalized_city, '') != ''
        and coalesce(normalized_postal_code, '') != ''
    group by 1,2,3
    having count(organization_id) = 1
)

, organization_address as (
    select
        po.organization_id
        ,trim(lower(po.name)) as organization_name
        ,bpo.normalized_address
        ,bpo.normalized_city
        ,bpo.normalized_postal_code
    from base_pipedrive_organizations bpo
        join ${stg2_schema}.pd_organizations po
             on po.normalized_address = bpo.normalized_address
            and po.normalized_city = bpo.normalized_city
            and po.normalized_postal_code = bpo.normalized_postal_code
)

, base_records as (
    select
        oa.organization_id
        ,oa.organization_name
        ,sba.business_customer_id
        ,sba.customer_name
        ,oa.organization_name = sba.customer_name as is_name_match
    from organization_address oa
        join supplybuy_billing_address sba
             on sba.normalized_billing_address = oa.normalized_address
            and sba.normalized_billing_city = oa.normalized_city
            and sba.normalized_billing_postal_code = oa.normalized_postal_code
)

select
    organization_id
    ,business_customer_id
from base_records
where is_name_match
union all
select
    organization_id
    ,business_customer_id
from base_records
where not(is_name_match)
    and (/* hard-coded matches */
       (business_customer_id = 1092709  and organization_id = 38)
    or (business_customer_id = 233090   and organization_id = 1154)
    or (business_customer_id = 1402751  and organization_id = 2473)
    or (business_customer_id = 1117259  and organization_id = 2814)
    or (business_customer_id = 1155121  and organization_id = 3307)
    or (business_customer_id = 1296921  and organization_id = 3682)
    or (business_customer_id = 581333   and organization_id = 10102)
    or (business_customer_id = 844202   and organization_id = 15913)
    or (business_customer_id = 536804   and organization_id = 16881)
    or (business_customer_id = 986560   and organization_id = 17224)
    or (business_customer_id = 858952   and organization_id = 17867)
    or (business_customer_id = 1153649  and organization_id = 18682)
    or (business_customer_id = 641011   and organization_id = 20622)
    or (business_customer_id = 596603   and organization_id = 21234)
    or (business_customer_id = 582872   and organization_id = 21653)
    or (business_customer_id = 1160706  and organization_id = 22254)
    or (business_customer_id = 1171184  and organization_id = 23230)
    or (business_customer_id = 271189   and organization_id = 24542)
    or (business_customer_id = 1196105  and organization_id = 27093)
    or (business_customer_id = 1232783  and organization_id = 36125)
    or (business_customer_id = 624589   and organization_id = 37006)
    or (business_customer_id = 1241067  and organization_id = 41052)
    or (business_customer_id = 1240678  and organization_id = 41056)
    or (business_customer_id = 1242111  and organization_id = 41057)
    or (business_customer_id = 1048661  and organization_id = 41577)
    or (business_customer_id = 1200409  and organization_id = 57040)
    or (business_customer_id = 1125053  and organization_id = 62346)
    or (business_customer_id = 1290169  and organization_id = 67751)
    or (business_customer_id = 374848   and organization_id = 69479)
    or (business_customer_id = 1190590  and organization_id = 71954)
    or (business_customer_id = 1237896  and organization_id = 40900)
    or (business_customer_id = 1103560  and organization_id = 54128)
    or (business_customer_id = 1116322  and organization_id = 9391)
    or (business_customer_id = 966363   and organization_id = 13839)
    or (business_customer_id = 1187466  and organization_id = 21759)
    or (business_customer_id = 1261057  and organization_id = 64758)
    or (business_customer_id = 1327967  and organization_id = 93627)
    or (business_customer_id = 1188540  and organization_id = 23918)
    or (business_customer_id = 1387078  and organization_id = 123620)
    or (business_customer_id = 1206142  and organization_id = 121630)
    or (business_customer_id = 1379726  and organization_id = 119977)
    or (business_customer_id = 1148721  and organization_id = 14963)
    or (business_customer_id = 1245111  and organization_id = 85115)
    or (business_customer_id = 1119878  and organization_id = 9674)
    or (business_customer_id = 1296050  and organization_id = 76726)
    or (business_customer_id = 1280152  and organization_id = 71534)
    or (business_customer_id = 1345415  and organization_id = 26467)
    or (business_customer_id = 1327514  and organization_id = 77541)
    or (business_customer_id = 1292362  and organization_id = 35412)
    or (business_customer_id = 1187469  and organization_id = 26186)
    or (business_customer_id = 1164643  and organization_id = 22523)
    or (business_customer_id = 1228722  and organization_id = 36131)
    or (business_customer_id = 1353721  and organization_id = 113209)
    or (business_customer_id = 1341574  and organization_id = 27714)
    or (business_customer_id = 1120932  and organization_id = 10078)
    or (business_customer_id = 1211559  and organization_id = 29720)
    or (business_customer_id = 1293967  and organization_id = 75648)
    or (business_customer_id = 1248813  and organization_id = 3679)
    or (business_customer_id = 641016   and organization_id = 10643)
    or (business_customer_id = 1373008  and organization_id = 106468)
    or (business_customer_id = 1274993  and organization_id = 69829)
    or (business_customer_id = 1138384  and organization_id = 15324)
    or (business_customer_id = 1266564  and organization_id = 37700)
    or (business_customer_id = 1239825  and organization_id = 40445)
    or (business_customer_id = 1260246  and organization_id = 67172)
    or (business_customer_id = 1194628  and organization_id = 27027)
    or (business_customer_id = 1362801  and organization_id = 117265)
    or (business_customer_id = 1157971  and organization_id = 54117)
    or (business_customer_id = 1112064  and organization_id = 27190)
    or (business_customer_id = 1313730  and organization_id = 77687)
    or (business_customer_id = 1296584  and organization_id = 28541)
    or (business_customer_id = 1136617  and organization_id = 15057)
    or (business_customer_id = 1368646  and organization_id = 118588)
    or (business_customer_id = 1269307  and organization_id = 10583)
    or (business_customer_id = 1211129  and organization_id = 29987)
    or (business_customer_id = 1280509  and organization_id = 71508)
    or (business_customer_id = 1261458  and organization_id = 64659)
    or (business_customer_id = 1306440  and organization_id = 80028)
    or (business_customer_id = 1408674  and organization_id = 111942)
    or (business_customer_id = 606415   and organization_id = 3257)
    or (business_customer_id = 1203087  and organization_id = 61340)
    or (business_customer_id = 1327973  and organization_id = 101785)
    or (business_customer_id = 1167785  and organization_id = 23334)
    or (business_customer_id = 1227237  and organization_id = 35236)
    or (business_customer_id = 566068   and organization_id = 20214)
    or (business_customer_id = 1307344  and organization_id = 20478)
    or (business_customer_id = 1233775  and organization_id = 37281)
    or (business_customer_id = 1332949  and organization_id = 103872)
    or (business_customer_id = 1160700  and organization_id = 22331)
    or (business_customer_id = 1380403  and organization_id = 24621)
    or (business_customer_id = 1331719  and organization_id = 103565)
    or (business_customer_id = 1150418  and organization_id = 20605)
    or (business_customer_id = 1227238  and organization_id = 35589)
    or (business_customer_id = 1225415  and organization_id = 35090)
    or (business_customer_id = 1264622  and organization_id = 67610)
    or (business_customer_id = 1383683  and organization_id = 123286)
    or (business_customer_id = 1148724  and organization_id = 27244)
    or (business_customer_id = 1158813  and organization_id = 14928)
    or (business_customer_id = 1277244  and organization_id = 65129)
    or (business_customer_id = 1292361  and organization_id = 117614)
    or (business_customer_id = 1403964  and organization_id = 19203)
    or (business_customer_id = 1153286  and organization_id = 20765)
    or (business_customer_id = 1239313  and organization_id = 39947)
    or (business_customer_id = 1235651  and organization_id = 42114)
    or (business_customer_id = 1145631  and organization_id = 18676)
    or (business_customer_id = 1323417  and organization_id = 68004)
    or (business_customer_id = 1276729  and organization_id = 70899)
    or (business_customer_id = 1273072  and organization_id = 68298)
    or (business_customer_id = 1320104  and organization_id = 93647)
    or (business_customer_id = 1389700  and organization_id = 124025)
    or (business_customer_id = 1328423  and organization_id = 102491)
    or (business_customer_id = 1157487  and organization_id = 21896)
    or (business_customer_id = 1199169  and organization_id = 22257)
    or (business_customer_id = 794889   and organization_id = 102490)
    or (business_customer_id = 1148718  and organization_id = 20610)
    or (business_customer_id = 1266565  and organization_id = 10519)
    or (business_customer_id = 1211561  and organization_id = 30010)
    or (business_customer_id = 1263437  and organization_id = 67553)
    or (business_customer_id = 1244697  and organization_id = 22529)
    or (business_customer_id = 1170725  and organization_id = 23387)
    or (business_customer_id = 473701   and organization_id = 20200)
    or (business_customer_id = 1140717  and organization_id = 13768)
    or (business_customer_id = 1080933  and organization_id = 155)
    or (business_customer_id = 1129952  and organization_id = 11453)
    or (business_customer_id = 1409473  and organization_id = 127667)
    or (business_customer_id = 1155126  and organization_id = 18106)
    or (business_customer_id = 1118097  and organization_id = 9690)
    or (business_customer_id = 1142942  and organization_id = 17850)
    or (business_customer_id = 1315952  and organization_id = 86071)
    or (business_customer_id = 1334964  and organization_id = 103338)
    or (business_customer_id = 1119875  and organization_id = 730)
    or (business_customer_id = 1096207  and organization_id = 20870)
    or (business_customer_id = 1311569  and organization_id = 84841)
    or (business_customer_id = 1231158  and organization_id = 29261)
    or (business_customer_id = 1150419  and organization_id = 20570)
    or (business_customer_id = 1301588  and organization_id = 63909)
    or (business_customer_id = 1308471  and organization_id = 33983)
    or (business_customer_id = 1165702  and organization_id = 21917)
    or (business_customer_id = 1324249  and organization_id = 85753)
    or (business_customer_id = 1333281  and organization_id = 17840)
    or (business_customer_id = 1145550 and organization_id = 18539)
    or (business_customer_id = 1207642 and organization_id = 29256)
    or (business_customer_id = 1242563 and organization_id = 39820)
    or (business_customer_id = 1326957 and organization_id = 74131)
    or (business_customer_id = 463895 and organization_id = 26711)
    or (business_customer_id = 1235207 and organization_id = 28508)
    or (business_customer_id = 1142932 and organization_id = 17775)
    or (business_customer_id = 1371979 and organization_id = 119673)
    or (business_customer_id = 1414328 and organization_id = 131634)
    or (business_customer_id = 1162761 and organization_id = 22457)
    or (business_customer_id = 1279426 and organization_id = 71367)
    or (business_customer_id = 1110893 and organization_id = 3631)
    or (business_customer_id = 1292764 and organization_id = 7649)
    or (business_customer_id = 1230063 and organization_id = 37761)
    or (business_customer_id = 1165704 and organization_id = 86302)
    or (business_customer_id = 1279962 and organization_id = 67799)
    or (business_customer_id = 1138406 and organization_id = 15445)
    or (business_customer_id = 1344107 and organization_id = 109165)
    or (business_customer_id = 1255445 and organization_id = 63996)
    or (business_customer_id = 1283573 and organization_id = 72030)
    or (business_customer_id = 1160289 and organization_id = 22251)
    or (business_customer_id = 1147862 and organization_id = 20107)
    or (business_customer_id = 1220288 and organization_id = 31748)
    or (business_customer_id = 1273437 and organization_id = 68343)
    or (business_customer_id = 1129294 and organization_id = 10539)
    or (business_customer_id = 1394055 and organization_id = 125318)
    or (business_customer_id = 1341116 and organization_id = 104651)
    or (business_customer_id = 1148301 and organization_id = 55085)
    or (business_customer_id = 1311030 and organization_id = 77198)
    or (business_customer_id = 1308877 and organization_id = 123315)
    or (business_customer_id = 1256459 and organization_id = 64039)
    or (business_customer_id = 1378128 and organization_id = 115974)
    or (business_customer_id = 612483 and organization_id = 2199)
)