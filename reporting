[[dags]]
dag_id = 'Fulfilment_View'
schedule_interval = '0 4 * * *'

[[dags]]
dag_id = 'Namshiae'
schedule_interval = '0 3 * * *'

[[dags]]
dag_id = 'noonbifugc'
schedule_interval = '@once'

[[dags]]
dag_id = 'Noon_mapping_data'
schedule_interval = '@once'


[[dags]] 
dag_id = 'PDDNEW'
#schedule_interval = '0 3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19 * * *'
schedule_interval = '30 */1 * * *'



[[dags]]
dag_id = 'uaecir5'
schedule_interval = '0 2 1 * *'



[[dags]]
dag_id = 'LSswam'
schedule_interval = '*/30 * * * *'

[[dags]]
dag_id = 'LSHH'
schedule_interval =  '*/30 * * * *'

[[dags]]
dag_id = 'NSSH'
schedule_interval = '*/30 * * * *'

[[dags]]
dag_id = 'NSSH8'
schedule_interval = '0 */1 * * *'

[[dags]]
dag_id = 'Neeraj_Paliwal_DAG'
schedule_interval = '0 */1 * * *'

[[dags]]
dag_id = 'LSas8'                                                              
schedule_interval = '0 2,8,12,16,22 * * *'

[[dags]]
dag_id = 'LSasaa8'
schedule_interval = '0 8,11,12,22 * * *'

[[dags]]
dag_id = 'NP_CIR_LATEST'
schedule_interval = '0 */1 * * *'

[[dags]]
dag_id = 'NP_DAILY'
schedule_interval = '0 1 * * *'

[[dags]]
dag_id = 'NP_2mtd'
schedule_interval = '0 * * */2 *'

[[dags]]
dag_id = 'AG_3PL'
schedule_interval = '0 */1 * * *'


[[dags]]
dag_id = 'LSasa8'
schedule_interval = '0 */1 * * *'

[[dags]]
dag_id = 'LSSnap'
schedule_interval = '45 9,10,11,12,13,14,15,16 * * *'

[[dags]]
dag_id = 'RS45'
schedule_interval = '45 2,5,7,9,10,11,12,13,14,15,16,17,19,21,23 * * *'

[[dags]]
dag_id = 'food'
schedule_interval = '*/10 * * * *'

[[dags]]
dag_id = 'NSSH9'
schedule_interval = '*/30 1,2,3,4,5,6,7 * * *'

[[dags]] 
dag_id = 'vj'
schedule_interval = '@once'

[[dags]] 
dag_id = 'logmis'
schedule_interval = '0 */6 * * *'

[[dags]] 
dag_id = 'logmis1'
schedule_interval = '@daily'

[[dags]] 
dag_id = 'Neeraj_EG'
schedule_interval = '@once'

[[dags]] 
dag_id = 'Instant1'
schedule_interval = '@once'

[[dags]] 
dag_id = 'LSv17'
schedule_interval = '35 4 * * *'

[[dags]]
dag_id = 'LS'
schedule_interval = '@once'


[[dags]]
dag_id = 'AG'
schedule_interval = '45 3,12 * * *'

[[dags]]
dag_id = 'AG_DS'
schedule_interval = '45 5 * * *'

[[dags]] 
dag_id = 'Instant_AG'
schedule_interval = '*/15 * * * *'

[[dags]]
dag_id = 'NP_mtd'
schedule_interval = '0 0 1 * *'

[[dags]]
dag_id = 'JAS'
schedule_interval = '* 2 * * *'

[[dags]]
dag_id = 'DIRECT_SHIP'
schedule_interval = '0 5 * * *'


[[dags]]
dag_id = 'WPS_Daily'
schedule_interval = '0 6 * * *'

[[dags]]
dag_id = 'WPS_Daily_8'
schedule_interval = '0 8 * * *'

[[dags]] 
dag_id = 'np_views'
schedule_interval = '0 3,4,10 * * *'

[[dags]] 
dag_id = 'NeeraJ_rtv'
schedule_interval = '0 2,14 * * *'


[[dags]]
dag_id = 'LS_Table'
schedule_interval = '@once'


[[dags]] 
dag_id = '3PL_DAG'
schedule_interval = '*/15 * * * *'

[[dags]] 
dag_id = 'NeeraJ_Noon_Daily'
schedule_interval = '*/15 * * * *'

[[dags]] 
dag_id = 'Neeraj_Auth_views'
schedule_interval = '@once'

[[dags]]
dag_id = 'NP_RTV_ll'
schedule_interval = '0 3,6,9,12,15,18,21 * * *'

#########################################################
[[droptable]]
dag_id = 'uaecir5'
task_id = 'Sales_table_drop'
tables_to_drop = ['noonbilogmis.reporting.noonbifugc_core_sales']


[[matview]]
dag_id = 'PDDNEW'
task_id = 'PDD_Breach_Raw_data'
destination_table = 'noonbilogmis.reporting.PDD_Breach_Raw_data'
sql = '''SELECT *
,case when CFC IN ('NOON/DXBG02','NOON/RUHG02','NOON/JEDG01') and flag =1 then 1 else 0 end as Grocery_Breach
,case when Carrier = 'Noon Express v2' and flag =1 then 1 else 0 end as TPL_FLow_Breach
,case when CFC Not IN ('NOON/DXBG02','NOON/RUHG02','NOON/JEDG01') and Carrier <> 'Noon Express v2' and flag =1 then 1 else 0 end as Pure_NE_Breached
FROM (
select * except(Pure_NE_Breach_flag, Overall_Breach)
, case when Country='sa' and date(ship_cutoff)>=edd then 0 else Pure_NE_Breach_flag end as flag
, case when Country='sa' and date(ship_cutoff)>=edd then 0 else Overall_Breach end as Overall_Breach 
from
(
Select lower(so.country_code) as Country
,sois.awb_nr
,so.order_nr
,soi.NDD_SDD_Flag
,soi.NDD
,soi.SDD
,soi.TDD
,DATE(coalesce(estimated_delivery_at, promised_at)) edd
,Hub.code as LM_Hub
,hub_sector.name as Hub_sector
,cz.code as Country_zone
,destination_city
,Carrier
,pw.display_name CFC
,id_service_logistics
,CASE WHEN lower(so.country_code) = 'ae' then timestamp_add(
least(ifnull(manifest1.m_created_at,TIMESTAMP_ADD(current_timestamp,INTERVAL 200000 day)),
ifnull(dx02.created_at ,TIMESTAMP_ADD(current_timestamp,INTERVAL 200000 day)), 
ifnull(Direct_ship.created_at,TIMESTAMP_ADD(current_timestamp,INTERVAL 200000 day))
),interval 4 hour)
WHEN lower(so.country_code) = 'sa' then timestamp_add(
least(ifnull(manifest1.m_created_at,TIMESTAMP_ADD(current_timestamp,INTERVAL 200000 day)),
ifnull(dx02.created_at ,TIMESTAMP_ADD(current_timestamp,INTERVAL 200000 day)), 
ifnull(Direct_ship.created_at,TIMESTAMP_ADD(current_timestamp,INTERVAL 200000 day))
),interval 3 hour)
WHEN lower(so.country_code) = 'eg' then timestamp_add(
least(ifnull(manifest1.m_created_at,TIMESTAMP_ADD(current_timestamp,INTERVAL 200000 day)),
ifnull(dx02.created_at ,TIMESTAMP_ADD(current_timestamp,INTERVAL 200000 day)), 
ifnull(Direct_ship.created_at,TIMESTAMP_ADD(current_timestamp,INTERVAL 200000 day))
),interval 2 hour) end as ship_dt
,b.shipped_at as ship_cutoff
,so.created_at customer_order_dt
,dz.code Delivery_Zone
,count(distinct sois.awb_nr ) as Total_EDD_Shipments
-- overall_breach
,count(distinct case when (date(coalesce(estimated_delivery_at, promised_at))<
(case when lower(so.country_code) = 'sa' then date(timestamp_sub(nexfa.FA_TS,Interval 2 hour)) else date(nexfa.FA_TS) end)
or date(nexfa.FA_TS) is null) then sois.awb_nr end) as Overall_Breach,
count(
distinct case when b.shipped_at >=
(cast(
CASE WHEN lower(so.country_code) = 'ae' then timestamp_add(
case when dx02.created_at is not null then dx02.created_at
when Direct_ship.created_at is not null then Direct_ship.created_at
when manifest1.m_created_at is not null then manifest1.m_created_at
 end,interval 4 hour)
WHEN lower(so.country_code) = 'sa' then timestamp_add(
case when dx02.created_at is not null then dx02.created_at
when Direct_ship.created_at is not null then Direct_ship.created_at
when manifest1.m_created_at is not null then manifest1.m_created_at end,interval 3 hour) 
WHEN lower(so.country_code) = 'eg' then timestamp_add(
case when dx02.created_at is not null then dx02.created_at
when Direct_ship.created_at is not null then Direct_ship.created_at
when manifest1.m_created_at is not null then manifest1.m_created_at end,interval 2 hour) end as timestamp))
and lost.lost_awb is null 
and
(
(case when lower(so.country_code) = 'sa' then date(timestamp_sub(nexfa.FA_TS,Interval 2 hour)) else date(nexfa.FA_TS) end)
> date(coalesce(estimated_delivery_at, promised_at)) or date(nexfa.FA_TS) is null) and InBound_ManifestNr is not null then sois.awb_nr end) as Pure_NE_Breach_flag
from (SELECT soi1.* except (estimated_delivery_at) ,CASE 
     WHEN (lower(json_extract(item_misc,"$.shipping_preferences.ndd")) in ('"free"','"paid"',"true") 
     or cs.order_type in ("vip sdd","paid sdd","vip ndd","paid ndd")) then 1
     WHEN lower(json_extract(item_misc,"$.shipping_preferences.sdd")) in ('"free"','"paid"',"true") THEN 1 ELSE 0 END AS NDD_SDD_Flag
, coalesce(lower(json_extract(item_misc,"$.shipping_preferences.ndd")), case when cs.order_type like '%ndd' then cs.order_type else null end) NDD 
, coalesce(lower(json_extract(item_misc,"$.shipping_preferences.sdd")), case when cs.order_type like '%sdd' then cs.order_type else null end)  SDD,
coalesce(lower(json_extract(item_misc,"$.shipping_preferences.tdd")), case when cs.order_type like '%tdd' then cs.order_type else null end) TDD,
FROM `noonltddwh.sales.sales_order_item` soi1
left join `noonbilogmis.reporting.noonbifugc_core_sales1` cs using(item_nr)) soi
left join `noonltddwh.sales.sales_order` so on so.id_sales_order =soi.id_sales_order
LEFT JOIN (select address_uid, address_v, max(created_at) created_ts from `noondwh.scestimates_delivery_zone.address_cache`
group by 1,2) ach_1 ON (so.address_code,so.v_customer_address) =
(ach_1.address_uid,ach_1.address_v)
left join `noondwh.scestimates_delivery_zone.address_cache` ach on (ach.address_uid,ach.address_v,ach.created_at)=(ach_1.address_uid,ach_1.address_v,ach_1.created_ts)
LEFT JOIN `noondwh.scestimates_delivery_zone.delivery_zone` dz USING(id_delivery_zone)
LEFT JOIN (SELECT distinct id_sales_order FROM `noonltddwh.sales.sales_order_address_change`) oac on oac.id_sales_order = so.id_sales_order
left join `noonltddwh.sales.sales_order_item_shipment` sois on soi.id_sales_order_item_shipment =sois.id_sales_order_item_shipment
LEFT JOIN `noonltddwh.partner.partner_warehouse` pw on sois.id_partner_warehouse_origin = pw.id_partner_warehouse
left join (select address_code, id_city from `noonltddwh.bqsync.customer_address`) as ca on so.address_code = ca.address_code
left join (select id_city, name_en as destination_city from `noonltddwh.ref.city` ) as dc on ca.id_city=dc.id_city
left join `noonltddwh.lms.item` it using(awb_nr)
left join `noonltddwh.lms.item_pkg` ipkg using(id_item)
LEFT JOIN
(
SELECT distinct awb_nr
FROM noonltddwh.lms.item it
LEFT JOIN noonltddwh.lms.item_pkg pkg USING (id_item)
WHERE CAST(json_extract(pkg.misc,'$.is_direct_delivery') AS NUMERIC) = 1
) DSDS on DSDS.awb_nr = sois.awb_nr
LEFT JOIN
(
select
 awb_nr
,container_line.unpacked_at as m_created_at,
from `noonltddwh.lms.container_line` container_line
left join `noonltddwh.lms.container_state`  container_state using (id_loc_type,loc_id)
left join `noonltddwh.lms.item` item using (id_item)
left join `noonltddwh.lms.manifest` manifest on container_line.loc_id=manifest.id_manifest
left join `noonltddwh.lms.manifest_type` manifest_type using (id_manifest_type)
left join `noonltddwh.lms.address` add using (id_address)
where id_loc_type = 5
and manifest_type.is_outbound = 0
) manifest1 on manifest1.awb_nr = sois.awb_nr
LEFT JOIN
(
SELECT sois.awb_nr AWB5,sois.created_at FROM
(
SELECT i.awb_nr,MIN(ish.created_at) created_at
FROM `noonltddwh.lms.item_state_history` ish
LEFT JOIN noonltddwh.lms.item i using(id_item)
WHERE ish.id_loc_type = 3
GROUP BY 1
) sois
LEFT JOIN noonltddwh.xms_sourcing.ship_box sb on sois.awb_nr =sb.awb_nr
LEFT JOIN noonltddwh.xms_sourcing.warehouse_lane wl on wl.id_warehouse = sb.id_warehouse_seller
WHERE wl.id_warehouse_lane_type in (3,5)
)Direct_ship On sois.awb_nr = Direct_ship.AWB5
LEFT JOIN
(
SELECT sois.awb_nr awb4,pw.display_name ,dd.created_at
FROM `noonltddwh.sales.sales_order_item_shipment` sois
LEFT JOIN  noonltddwh.sales.sales_order so using(id_Sales_order)
left JOIN `noonltddwh.partner.partner_warehouse` pw on sois.id_partner_warehouse_origin = pw.id_partner_warehouse
LEFT JOIN
(
SELECT i.awb_nr,MIN(ish.created_at) created_at
FROM `noonltddwh.lms.item_state_history` ish
LEFT JOIN noonltddwh.lms.item i using(id_item)
WHERE ish.id_loc_type = 3
GROUP BY 1
) dd on sois.awb_nr = dd.awb_nr
WHERE pw.display_name IN ('NOON/DXBG02','NOON/RUHG02','NOON/JEDG01')
) dx02 on dx02.awb4 = sois.awb_nr
left join (select awb_nr lost_awb
from (select id_item,created_at from noonltddwh.lms.item_state
where id_loc_type=1
)
left join noonltddwh.lms.item_state ish using(id_item, created_at)
left join noonltddwh.lms.hub_location hl on (ish.id_loc_type,ish.loc_id) = (1,id_hub_location)
left join noonltddwh.lms.item i using(id_item)
where id_loc_type = 1 and hl.code like "%LOST%"
)lost on lost.lost_awb= sois.awb_nr
left join (
select sois.awb_nr,reference_nr,
case
when sois.id_carrier=9 and reference_nr is null then "NE"
when sois.id_carrier =2 or tpl_carrier=2 then "Aramex"
when sois.id_carrier =6 or tpl_carrier=3 then "SMSA"
when sois.id_carrier = 43 or tpl_carrier=4 then "Saudi Post"
when tpl_carrier=5 then "IMile"
when  tpl_carrier=7 then "J&T Express"
when  tpl_carrier=8 then "Saudi Post Low Cost"
else refc.name_en
end as Carrier,
from `noonltddwh.sales.sales_order_item_shipment` sois
LEFT JOIN(
select distinct item.awb_nr , tipm.id_item , tcr.reference_nr, tcr.id_carrier as tpl_carrier from `noonltddwh.lms.item` item
LEFT JOIN `noonltddwh.lms.tpl_item_pgroup_map` tipm on tipm.id_item =item.id_item
LEFT JOIN `noonltddwh.lms.tpl_carrier_reference` tcr on tipm.id_item_pgroup = tcr.id_item
WHERE tcr.reference_nr is not null
)tpl on sois.awb_nr =tpl.awb_nr
LEFT JOIN `noonltddwh.ref.carrier` refc on sois.id_carrier=refc.id_carrier
where sois.id_carrier in (2,4,6,9,43)
) as car on sois.awb_nr =car.awb_nr
LEFT JOIN(select awb_nr , InBound_ManifestNr ,In_Created_At from `noonltddwh.lms.item`
LEFT JOIN (SELECT id_item,manifest_nr as InBound_ManifestNr ,manifest_item.created_at as In_Created_At
FROM `noonltddwh.lms.manifest_item` manifest_item
LEFT JOIN `noonltddwh.lms.manifest` using (id_manifest)
WHERE is_outbound=0) in_manifest_item USING (id_item) ) Manifest on sois.awb_nr=Manifest.awb_nr
left join `noonltddwh.lms.client_pkg_ref` client_pkg_ref on sois.awb_nr=client_pkg_ref.awb_nr
left join (select * from `noonltddwh.lms.creq_leg` where id_creq_leg_type=3) as creq_leg using (id_creq)
left join `noonltddwh.lms.hub_sector` hub_sector on hub_sector.id_hub_sector=creq_leg.id_hub_sector
left join `noonltddwh.lms.hub` hub on hub_sector.id_hub=hub.id_hub
left join `noonltddwh.lms.country_zone` as cz on cz.id_country_zone=creq_leg.id_country_zone
left join `noonltddwh.purchase_v2.purchase_order_item` poi on soi.id_sales_order_item=poi.id_sales_order_item
left join `noonltddwh.oms.purchase_item` pi on pi.purchase_item_nr = poi.purchase_item_nr
left join `noonltddwh.oms.sales_item` si using (id_sales_item)
left join (
select * except(shipped_at, estimated_delivery_at)
, case when lower(country_code) = 'ae' then timestamp_add(shipped_at, interval 4 hour)
when lower(country_code) = 'sa'  then timestamp_add(shipped_at, interval 3 hour)
when lower(country_code) = 'eg' then timestamp_add(shipped_at, interval 2 hour) end shipped_at
, case when lower(country_code) = 'ae' then timestamp_add(estimated_delivery_at, interval 4 hour)
when lower(country_code) = 'sa'  then timestamp_add(estimated_delivery_at, interval 3 hour)
when lower(country_code) = 'eg' then timestamp_add(estimated_delivery_at, interval 2 hour) end estimated_delivery_at
from
(SELECT distinct country_code, awb_nr, 
CASE 
WHEN DE_version = "new DE" then shipped_at
WHEN NDD_SDD_Flag = 1 and DE_version = "old DE" then expected_shipping_ts 
ELSE shipped_at end as shipped_at
,delivered_At,
EDD_1 as estimated_delivery_at,
id_service_logistics
from (
SELECT so.country_code, sois.awb_nr,EDD_1,id_service_logistics
,case when day = 'EDD-0' then timestamp_add(estimated_delivery_at , interval cut_off_min minute)
when day = 'EDD-1' then timestamp_add (timestamp_sub(estimated_delivery_at, interval 24 hour) , interval cut_off_min minute)
else timestamp_add(timestamp_sub(estimated_delivery_at, interval 24 hour) , interval 18 hour)
end expected_shipping_ts
,CASE WHEN lower(json_extract(item_misc,"$.shipping_preferences.ndd")) in ('"free"','"paid"',"true") then 1
      WHEN lower(json_extract(item_misc,"$.shipping_preferences.sdd")) in ('"free"','"paid"',"true") THEN 1 ELSE 0 END AS NDD_SDD_Flag
,DE_version
,MIN(shipped_at) shipped_at,MIN(delivered_At) delivered_At
FROM `noonltddwh.sales.sales_order_item` soi
LEFT JOIN noonltddwh.sales.sales_order so using(id_sales_order)
left join (select address_code, id_city from `noonltddwh.bqsync.customer_address`) as ca on ca.address_code = so.address_code
LEFT JOIN `noonltddwh.sales.sales_order_item_shipment` sois using(id_sales_order_item_shipment)
left join `noonbilogoi.battleplan_v1.item_final_state` ifs using (awb_nr)
left join (select * from `noonbilogmis.reporting.premium_services_cutoff` where type in ('NDD','VIP-NDD') and id_city is not null) ps on (ifs.origin_warehouse , ca.id_city) = (ps.warehouse_code , ps.id_city)
left join `noonltddwh.lms.creq` c using (client_ref)
LEFT JOIN `noonltddwh.purchase_v2.purchase_order_item` poi on soi.id_sales_order_item=poi.id_sales_order_item
left join `noonltddwh.oms.purchase_item` pi on pi.purchase_item_nr = poi.purchase_item_nr
left join `noonltddwh.oms.sales_item` si using (id_sales_item)
left join (
select * EXCEPT(min_target_shipped_at), case when min_target_shipped_at is not null then "new DE" else "old DE" end as DE_version from   
(select id_purchase_item,id_partner_warehouse_from ,MIN(min_target_delivered_at) EDD_1,id_service_logistics,
coalesce(MIN(min_target_shipped_at),MIN(shipped_at)) shipped_at,MAx(delivered_at) delivered_at, 
MIN(min_target_shipped_at) min_target_shipped_at
from `noonltddwh.oms.stock_item` 
left join `noonltddwh.oms.item_estimate` using(id_stock_item)
GROUP BY 1,2,4)
)as b on (b.id_purchase_item,b.id_partner_warehouse_from) =(pi.id_purchase_item,soi.id_partner_warehouse) and b.id_purchase_item is not null
GROUP BY 1,2,3,4,5,6,7
)
)
where shipped_at is not null
)as b on b.awb_nr = sois.awb_nr
left join (SELECT awb_nr , MIN(FA_TS) FA_TS FROM
(
SELECT awb_nr , FA_TS FA_TS 
FROM
noonbilogoi.Share_Point.NE_FAS
)group by 1)nexfa on nexfa.awb_nr=sois.awb_nr
where  so.id_mp <>6
and soi.id_invoice_section = 1
and so.id_cart_type = 1
and carrier IN ("NE","Noon Express v2")
and soi.id_sales_order_item_status not in (1,2,7,9)
and lost.lost_awb is null
--and (b.shipped_at <= b.delivered_At OR  b.delivered_At is null)
and DSDS.awb_nr is null
and((NDD_SDD_Flag = 0 AND DATE(case when lower(so.country_code) = 'ae' then timestamp_add(b.shipped_at, interval 4 hour)
when lower(so.country_code) = 'sa' then timestamp_add(b.shipped_at, interval 3 hour)
when lower(so.country_code) = 'eg' then timestamp_add(b.shipped_at, interval 2 hour) end) <= DATE(coalesce(estimated_delivery_at, promised_at))) OR NDD_SDD_Flag = 1)
and sois.id_carrier in (2,4,6,9,43)
group by 1,2 ,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19))'''


[[authview]]
dag_id = 'NP_Daily_10'
task_id = 'xms_sales_ltd'
destination_table = 'noonbilogmis.reporting.xms_sales_ltd'
sql = '''
select * from `noonbifugc.sales.xms_sales_ltd`
'''

[[authview]]
dag_id = 'NP_Daily_10'
task_id = 'instant_sales_30'
destination_table = 'noonbilogmis.reporting.instant_sales_30'
sql = '''
select * from `noonbifugc.sales.instant_sales_30`
'''


[[matview]]
dag_id = 'LSas8'
task_id = 'Neeraj_consolidation_rawdata'
destination_table = 'noonbilogmis.reporting.Consolidation_rawdata'
sql = '''
Select 
country_code, 
final.customer_code, 
Hub,
awb_nr,
delivered_date,
case when time_diff<=1 and b.awb_nr is not null then "delivered_together" else "not" end as Flag
From (Select country_code,customer_code,Hub,delivered_date,min_ts,max_ts, timestamp_diff(max_ts,min_ts,hour) as time_diff 
From ( select distinct hub.code as Hub, 
so.customer_code , so.country_code, date(min_delivered_at) as delivered_date, 
count(distinct awb_nr) as shipments, min(min_delivered_at) as min_ts, max(min_delivered_at) as max_ts, 
from `noonltddwh.sales.sales_order_item` soi 
left join `noonltddwh.sales.sales_order` so on so.id_sales_order =soi.id_sales_order 
left join `noonltddwh.sales.sales_order_item_shipment` sois on soi.id_sales_order_item_shipment =sois.id_sales_order_item_shipment 
left join `noonltddwh.lms.client_pkg_ref` client_pkg_ref using (awb_nr) 
left join (select * from `noonltddwh.lms.creq_leg` where id_creq_leg_type=3) as creq_leg using (id_creq) 
left join `noonltddwh.lms.hub_sector` hub_sector on hub_sector.id_hub_sector=creq_leg.id_hub_sector 
left join `noonltddwh.lms.hub` hub on hub_sector.id_hub=hub.id_hub 
left join (SELECT * FROM `noonltddwh.cache.sales_order_item_summary` 
WHERE sales_order_pdate >= date_trunc(current_date(), month)) soiss on soi.id_sales_order_item =soiss.id_sales_order_item 
where id_cart_type =1
and id_invoice_section =1 
and sois.id_carrier =9 
and soi.id_sales_order_item_status =6 
and extract(year from min_delivered_at)=extract(year from current_date())
group by 1,2,3,4 )) as final 
left join ( select distinct so.customer_code , awb_nr , min_delivered_at as delivered_ts 
from `noonltddwh.sales.sales_order_item` soi 
left join `noonltddwh.sales.sales_order` so on so.id_sales_order =soi.id_sales_order 
left join `noonltddwh.sales.sales_order_item_shipment` sois on soi.id_sales_order_item_shipment =sois.id_sales_order_item_shipment 
left join (SELECT * FROM `noonltddwh.cache.sales_order_item_summary` 
WHERE sales_order_pdate >= date_trunc(current_date(), month) ) soiss on soi.id_sales_order_item =soiss.id_sales_order_item 
where id_cart_type =1 
and id_invoice_section =1 
and sois.id_carrier =9 
and soi.id_sales_order_item_status =6 ) b on (final.customer_code,delivered_date)=(b.customer_code,date(b.delivered_ts)) 
group by 1,2,3,4,5,6
'''

[[matview]]
dag_id = 'LSas8'
task_id = 'Neeraj_consolidation_Cnt'
destination_table = 'noonbilogmis.reporting.Consolidation_Cnt'
sql = '''
SELECT lower(country_code ) as Country, 
Hub, 
delivered_date,
destination_city,
total_delivered_shipments AS consolidation_shipment , 
delivered_together AS consolidated_delivered
FROM (Select  country_code, Hub,delivered_date, destination_city, sum(delivered_shipments) as total_delivered_shipments, sum(delivered_together) as delivered_together, 
sum(other_shipments) as not_together, sum(other_shipments) as Other_Shipment, 
count(distinct case when flag=1 then customer_code end) as customers_more_than_1_DA, 
count(distinct customer_code) as total_customers_delivered 
From ( Select country_code,
customer_code,
Hub,
destination_city,delivered_date, sum(shipments) as delivered_shipments, 
sum(delivered_together) as delivered_together, 
sum(other_shipments) as other_shipments, 
max(if(other_shipments>0,1,0)) as flag from (Select 
country_code, 
final.customer_code, 
delivered_date, Hub, destination_city,
count(distinct b.awb_nr) as shipments, 
count(distinct case when time_diff<=1 and b.awb_nr is not null then b.awb_nr end) as delivered_together, 
count(distinct case when delivered_ts<>min_ts and time_diff>=1 and b.awb_nr is not null and min_ts<>max_ts 
and max_ts is not null then b.awb_nr end) as other_shipments 
From (Select country_code,customer_code,Hub,delivered_date,destination_city,min_ts,max_ts, timestamp_diff(max_ts,min_ts,hour) as time_diff 
From ( select distinct hub.code as Hub, destination_city,
so.customer_code , so.country_code, date(min_delivered_at) as delivered_date, 
count(distinct awb_nr) as shipments, min(min_delivered_at) as min_ts, max(min_delivered_at) as max_ts, 
from `noonltddwh.sales.sales_order_item` soi 
left join `noonltddwh.sales.sales_order` so on so.id_sales_order =soi.id_sales_order 
left join `noonltddwh.sales.sales_order_item_shipment` sois on soi.id_sales_order_item_shipment =sois.id_sales_order_item_shipment 
left join `noonltddwh.lms.client_pkg_ref` client_pkg_ref using (awb_nr) 
left join (select address_code, id_city from `noonltddwh.bqsync.customer_address`) as ca on so.address_code = ca.address_code
left join (select id_city, name_en as destination_city from `noonltddwh.ref.city` ) as dc on ca.id_city=dc.id_city
left join (select * from `noonltddwh.lms.creq_leg` where id_creq_leg_type=3) as creq_leg using (id_creq) 
left join `noonltddwh.lms.hub_sector` hub_sector on hub_sector.id_hub_sector=creq_leg.id_hub_sector 
left join `noonltddwh.lms.hub` hub on hub_sector.id_hub=hub.id_hub 
left join (SELECT * FROM `noonltddwh.cache.sales_order_item_summary` 
WHERE sales_order_pdate >= date_trunc(current_date(), month)) soiss on soi.id_sales_order_item =soiss.id_sales_order_item 
where id_cart_type =1
and id_invoice_section =1 
and sois.id_carrier =9 
and soi.id_sales_order_item_status =6 
and extract(year from min_delivered_at)=extract(year from current_date())
group by 1,2,3,4,5 )) as final 
left join ( select distinct so.customer_code , awb_nr , min_delivered_at as delivered_ts 
from `noonltddwh.sales.sales_order_item` soi 
left join `noonltddwh.sales.sales_order` so on so.id_sales_order =soi.id_sales_order 
left join `noonltddwh.sales.sales_order_item_shipment` sois on soi.id_sales_order_item_shipment =sois.id_sales_order_item_shipment 
left join (SELECT * FROM `noonltddwh.cache.sales_order_item_summary` 
WHERE sales_order_pdate >= date_trunc(current_date(), month) ) soiss on soi.id_sales_order_item =soiss.id_sales_order_item 
where id_cart_type =1 
and id_invoice_section =1 
and sois.id_carrier =9 
and soi.id_sales_order_item_status =6 ) b on (final.customer_code,delivered_date)=(b.customer_code,date(b.delivered_ts)) 
group by 1,2,3,4,5)group by 1,2,3,4,5) 
group by 1,2,3,4
order by country_code , hub ) 
'''


[[matview]]
dag_id = 'LSas8'
task_id = 'Neeraj_consolidation'
destination_table = 'noonbilogmis.reporting.Consolidation_order_lvl_rawdata'
sql = '''
select distinct
country_code ,
shipment_nr,
order_nr,
sois.awb_nr ,
item_nr,
ifs.pkg_value pkg_value,
DD stock_recon_cnt,
s.code as status,
estimated_delivery_at,
case when held_field_id_creq_leg is not null then 1 else 0 end as held_field_id_creq_leg,
case when held_client_id_creq_leg is not null then 1 else 0 end as held_client_id_creq_leg,
if(json_EXTRACT(json_EXTRACT(so.misc,'$.shipping_preferences'),'$.sc') is not null ,1,0) should_consolidate_shipment,
extract(date from so.created_at) as order_Date,
from `noonltddwh.sales.sales_order_item` soi
left join `noonltddwh.ref.sales_order_item_status` s on soi.id_sales_order_item_status = s.id_sales_order_item_status
left join `noonltddwh.sales.sales_order_item_shipment` sois on sois.id_sales_order_item_shipment =soi.id_sales_order_item_shipment
left join `noonbilogoi.battleplan_v1.item_final_state` ifs on ifs.awb_nr = sois.awb_nr 
left join 
(
SELECT id_item, COUNT(1) DD FROM `noonltddwh.lms.stock_recon` 
GROUP BY 1
)stock_recon on stock_recon.id_item = ifs.id_item  
left join `noonltddwh.sales.sales_order` so on so.id_sales_order =soi.id_sales_order
left join
(
SELECT c.client_ref , held_field.id_creq_leg held_field_id_creq_leg ,held_client.id_creq_leg held_client_id_creq_leg  FROM `noonltddwh.lms.creq` c
left join `noonltddwh.lms.creq_leg` cl using (id_creq)
left join (
SELECT id_creq_leg,MIN(created_at) FROM
`noonltddwh.lms.creq_leg_history`
WHERE
CAST (json_EXTRACT(DATA,
"$.id_reason_held") AS int64) = 21
AND json_EXTRACT(DATA,
"$.updated_app") LIKE "%field%"
GROUP BY 1
)held_field on held_field.id_creq_leg = cl.id_creq_leg
left join (
SELECT id_creq_leg,MIN(created_at) FROM
`noonltddwh.lms.creq_leg_history`
WHERE
CAST (json_EXTRACT(DATA,
"$.id_reason_held") AS int64) = 21
AND json_EXTRACT(DATA,
"$.updated_app") LIKE "%client%"
GROUP BY 1
)held_client on held_client.id_creq_leg = cl.id_creq_leg
WHERE id_creq_type = 1
)creq
on creq.client_ref = so.order_nr  
where soi.id_sales_order_item_status not in (7,8,9)
and so.id_cart_type = 1
and soi.id_invoice_section = 1
'''


[[authview]]
dag_id = 'Neeraj_Auth_views'
task_id = 'Neeraj_oms_stock_item'
destination_table = 'noonbilogmis.reporting.oms_stock_item'
sql = '''
SELECT * FROM `noonltddwh.oms.stock_item`
'''


[[authview]]
dag_id = 'Neeraj_Auth_views'
task_id = 'Neeraj_DA_DATA_EG'
destination_table = 'noonbilogmis.reporting.Neeraj_DA_DATA_EG'
sql = '''
SELECT da.string_field_4 as DA_Category ,temp.* FROM 
(
SELECT distinct username,
  OFPD_Date,ob.hub as hub,  assigned_requests,
  pickedup_requests, Total_Picked_Touch_Point,
  assigned_shipment, delivered_shipment,NS_assigned_shipment, NS_delivered_shipment,Total_Delivered_Touch_Point,
IF(ifnull(pickedup_requests,0)+ifnull(delivered_shipment,0)>0,1,0) AS attendance
FROM (SELECT username,
    OFPD_Date,
    COUNT(DISTINCT IF(status="closed",id_creq_leg_job,NULL)) AS pickedup_requests,
    COUNT(DISTINCT IF(client_ref IS NOT NULL,id_creq_leg_job,NULL)) assigned_requests,
    COUNT(DISTINCT IF(status="closed",id_customer,NULL)) Total_Picked_Touch_Point,
  FROM (
    SELECT
      client_ref,
      username,
      id_creq_leg_job,
      status.code AS status,
      DATE(creq_leg_job.updated_at) AS OFPD_Date,
      hub.code AS hub,
      reason.code AS reason,
      id_customer,
      creq_leg_job.is_attempt
    FROM `noonltddwh.lms.creq_leg_job` creq_leg_job
    LEFT JOIN `noonltddwh.lms.status` status USING(id_status)
    LEFT JOIN `noonltddwh.lms.creq_leg` creq_leg USING (id_creq_leg)
    LEFT JOIN `noonltddwh.lms.address` address on creq_leg.id_address=address.id_address 
    LEFT JOIN `noonltddwh.lms.user` user USING(id_user)
    LEFT JOIN `noonltddwh.lms.creq` creq USING(id_creq)
    LEFT JOIN `noonltddwh.lms.reason` reason USING(id_reason)
    LEFT JOIN `noonltddwh.lms.country_zone` country_zone USING (id_country_zone)
    LEFT JOIN `noonltddwh.lms.hub_sector` hub_sector USING (id_hub_sector)
    LEFT JOIN `noonltddwh.lms.hub` hub USING (id_hub) WHERE country_zone.id_country=3)
    GROUP BY  1,2 )ib
full JOIN (
 SELECT username,OFPD_Date, hub.code as hub,
      count(distinct(IF( A.id_item IS NOT NULL,A.id_item,null))) assigned_shipment,
      count(distinct(IF( id_manifest IS NOT NULL,A.id_item,null))) as delivered_shipment,
      count(distinct case when zpick.code like "%-N-%" and A.id_item IS NOT NULL then A.id_item end ) NS_assigned_shipment,
      count(distinct case when  id_manifest IS NOT NULL and zpick.code like "%-N-%" and A.id_item IS NOT NULL then A.id_item end ) NS_delivered_shipment,
      count(distinct(IF( id_manifest IS NOT NULL,id_customer,null))) as Total_Delivered_Touch_Point,
      FROM (SELECT id_item, DATE(created_At) AS OFPD_Date, MAX(created_at) AS created_at
      FROM `noonltddwh.lms.session_field_item` GROUP BY 1,2) as A
      LEFT JOIN (SELECT distinct id_item, awb_nr  FROM `noonltddwh.lms.item` ) as B on A.id_item =B.id_item 
      LEFT JOIN (SELECT distinct awb_nr, id_sales_order_item_shipment from `noonltddwh.sales.sales_order_item_shipment`) as C on C.awb_nr =B.awb_nr
      LEFT JOIN (SELECT distinct item_nr , id_sales_order_item_shipment from `noonltddwh.sales.sales_order_item`) using (id_sales_order_item_shipment)
      LEFT JOIN `noonltddwh.wms.exdoc_line` on exdoc_line_ref = item_nr
      LEFT JOIN `noonltddwh.wms.doc_line` dl using (id_exdoc_line)
      LEFT JOIN `noonltddwh.wms.job_line` jl on jl.id_doc_line_source = dl.id_doc_line
      LEFT JOIN `noonltddwh.wms.location`  lpick ON   jl.id_location_pick=lpick.id_location
      LEFT JOIN `noonltddwh.wms.zone`  zpick  ON  lpick.id_zone=zpick.id_zone
      LEFT JOIN `noonltddwh.lms.session_field_item` session_field_item on  (session_field_item.id_item,session_field_item.created_at)=(A.id_item,A.created_at)
      LEFT JOIN `noonltddwh.lms.session_field` session_field USING (id_session_field)
      LEFT JOIN `noonltddwh.lms.user` user USING (id_user)
      LEFT JOIN `noonltddwh.lms.hub` hub USING (id_hub)
      LEFT JOIN `noonltddwh.lms.hub_airport` hub_airport USING (id_hub_airport)
      LEFT JOIN `noonltddwh.lms.creq_leg` creq_leg ON session_field_item.id_creq_leg_customer=creq_leg.id_creq_leg
      LEFT JOIN `noonltddwh.lms.address` address on creq_leg.id_address=address.id_address 
      LEFT JOIN `noonltddwh.lms.creq` creq USING (id_creq)
      LEFT JOIN `noonltddwh.lms.country_zone` cz using(id_country_zone)
      WHERE cz.id_country=3 AND id_creq_type IN (1,3)
      GROUP BY 1,2,3
    ) ob USING (username,OFPD_Date)

)temp
left join `noonbilogoi.nefreelancer.ne_da_category` da on da.string_field_2 = temp.username
'''


[[matview]]
dag_id = 'NP_DAILY'
task_id = 'CIR_TAT_SUMMARY'
destination_table = 'noonbilogmis.reporting.CIR_TAT_SUMMARY'
sql = '''
select 
Country,
type,
round(Picked_to_warehouse_receivingTAT_90/24,1) AS Picked_TO_Wh_Rec,
round(Request_to_FATAT_90/24,1) AS Request_TO_FA,
round(Request_to_PICK_90/24,1) AS Request_TO_PICK,
from 
(select 
c.Country,
c.type,
Picked_to_warehouse_receivingTAT_Q[offset(90)] AS Picked_to_warehouse_receivingTAT_90,
Request_to_FATAT_Q[offset(90)] AS Request_to_FATAT_90,
Request_to_Pick_Q[offset(90)] AS Request_to_PICK_90,
from
(select 
Country,
type,
APPROX_QUANTILES (Picked_to_warehouse_receiving,100) as Picked_to_warehouse_receivingTAT_Q,
APPROX_QUANTILES (Request_to_FA,100) as Request_to_FATAT_Q,
APPROX_QUANTILES (Request_to_Pick,100) as Request_to_Pick_Q,
APPROX_QUANTILES (Picked_to_warehouse_receiving_D,100) as Picked_to_warehouse_receivingTAT_Q_D,
APPROX_QUANTILES (Request_to_FA_D,100) as Request_to_FATAT_Q_D,
from 
(
SELECT DISTINCT 
a.Request_Id,
a.Client_ref,
a.awb_nr,
a.Country as Country,
a.Hub,
a.Hub_Sector,
a.User,
a.first_attempt,
a.picked_date,
a.Request_CreatedAt,
a.return_manifest_unpacked_at,
a.Failed_Reason,
a.CIR_dispatch,
a.Picked_to_warehouse_receiving,
a.Request_to_FA,
a.Request_to_Pick,
a.Picked_to_warehouse_receiving_D,
a.Request_to_FA_D,
case when origin_id_city = dst_id_city then "same_city"
 
 else "other_city"
 END as type,
FROM 
(SELECT DISTINCT cir_base_view.creq_nr Request_Id,
client_ref,
firstmile_package.awb_nr as awb_nr,
ifs.id_item ,
cir_base_view.created_at Request_CreatedAt,
cir_base_view.updated_at Request_Updated_At,
cir_base_view.updated_at Item_Updated_At,
ist.updated_at as return_manifest_unpacked_at,
last_hl_ts,
timestamp_diff(if(min_Dispatch_TS is null, last_hl_ts, min_Dispatch_TS), cir_base_view.created_at, hour ) as CIR_dispatch,
timestamp_diff(ist.updated_at, attempts.picked_date, hour) as Picked_to_warehouse_receiving,
timestamp_diff( attempts.first_attempt,cir_base_view.created_at, hour) as Request_to_FA, 
timestamp_diff( attempts.picked_date,cir_base_view.created_at, hour) as Request_to_Pick,
timestamp_diff( attempts.picked_date,cir_base_view.created_at, day) as Request_to_Pick_D,
timestamp_diff(ist.updated_at, attempts.picked_date, day) as Picked_to_warehouse_receiving_D,
timestamp_diff( attempts.first_attempt,cir_base_view.created_at, day) as Request_to_FA_D, 
Creq_Type,
Creq_Leg_Type,
Country,
Hub,
Hub_Sector,
Country_Zone,
User,
attempts.attempt_count,
attempts.first_attempt,
attempts.last_attempt,
attempts.picked_date,
Failed_Reason,
cir_base_view.scheduled_date as scheduled_date,
Creq_Leg_Status,
Creq_Leg_Job_Status,
Status Item_Status,
loc_type,
location_entity,
pw.code as dst_WH_code,
pw.display_name as dst_WH_display_name,
if(hub like "%RUH-A%",9,if(hub like "%JED-A%",10,if(hub like "%DMM-A%",11,if(hub like "%DXB-A%",1,if(hub like "%CAI-A%",144,0))))) as origin_id_city,
pw.id_city as dst_id_city,
IF(current_location IS NULL,Hub,current_location) current_location,
CASE WHEN Final_Status IS NULL AND Creq_Leg_Status like 'Closed' THEN 'Pickup Done'
WHEN Final_Status IS NULL AND Creq_Leg_Status like 'Closed' THEN 'Pickup Done'
WHEN Final_Status IS NULL AND Creq_Leg_Status like 'Canceled' THEN 'Pickup Cancelled'
WHEN Final_Status IS NULL AND Creq_Leg_Status like 'Held' THEN 'Pickup Held'
WHEN Final_Status IS NULL AND Creq_Leg_Status like 'Opened' AND Creq_Leg_Job_Status like 'Opened' THEN 'Out For Pickup'
WHEN Final_Status IS NULL AND Creq_Leg_Status like 'Opened' THEN 'Pending for Pickup'
ELSE Final_Status End as Final_Status
FROM (select * from `noonbilogoi.LOGKSA_DATA.cir_creq_baseview` where date(created_at) between Current_date()-7 and Current_date() ) as cir_base_view
LEFT JOIN `noonbilogoi.LOGKSA_DATA.firstmile_package` as firstmile_package
ON cir_base_view.creq_nr = firstmile_package.creq_nr
LEFT JOIN (select awb_nr , id_item from `noonbilogoi.battleplan_v1.item_final_state`) ifs  on ifs.awb_nr = firstmile_package.awb_nr
LEFT JOIN `noonltddwh.lms.item_state` ist using (id_item)
LEFT JOIN  (select id_item, max(created_at) as last_hl_ts from `noonltddwh.lms.item_state_history` where id_loc_type=1 group by 1) using (id_item)
LEFT JOIN (select creq_nr , id_creq , id_creq_leg from `noonltddwh.lms.creq` 
left join (select id_creq, id_creq_leg from `noonltddwh.lms.creq_leg`
where id_creq_leg_type = 2 ) using (id_creq))as creq on creq.creq_nr = cir_base_view.creq_nr
left join `noonltddwh.lms.creq_leg` as cl_return on creq.id_creq_leg=cl_return.id_creq_leg
left join `noonltddwh.lms.address` add on cl_return.id_address=add.id_address
left join `noonltddwh.partner.partner_warehouse` pw on add.address_uid=pw.code
left join (select distinct id_item, min(ish.created_at) as min_Dispatch_TS from `noonltddwh.lms.item_state_history` ish left join `noonltddwh.lms.creq_leg` using (id_creq_leg) where id_creq_leg_type = 4 and id_loc_type in (3,4) group by 1
) using (id_item)
LEFT JOIN (SELECT creq_nr,
count(client_ref) attempt_count,
MIN(updated_at) first_attempt,
MAX(updated_at) last_attempt,
MAX(CASE WHEN Creq_Leg_Status like 'Closed' and Creq_Leg_Job_Status like 'Closed' THEN updated_at End) picked_date
FROM `noonbilogoi.LOGKSA_DATA.cir_transactions`
group by creq_nr) attempts on attempts.creq_nr = cir_base_view.creq_nr
)a)
group by 1,2)c)
'''



[[authview]]
dag_id = 'Neeraj_Auth_views'
task_id = 'Neeraj_Hub_Performance_new_Report_Query13_eg'
destination_table = 'noonbilogmis.reporting.Hub_Performance_new_Report_Query13_eg'
sql = '''
SELECT hub,Total_OFD,Fake_Attempts,Fake_Attempts_per
FROM (
select 1 as d,  Hub,count(awb_nr) Total_OFD,count(case when Fake_Attempts not in ( 'no_fake_attempt') then awb_nr end) Fake_Attempts,concat(round(count(case when Fake_Attempts not in ( 'no_fake_attempt') then awb_nr end)*100/count(awb_nr),2),'%') Fake_Attempts_per
from noonbilogmis.reporting.Fake_Attempt_Report_MTD 
where  country = 'eg' and OFD_Date = current_date -1
group by 1,2
union all
select 2 as d, 'Total' Hub,count(awb_nr) Total_OFD,count(case when Fake_Attempts not in ( 'no_fake_attempt') then awb_nr end) Fake_Attempts,concat(round(count(case when Fake_Attempts not in ( 'no_fake_attempt') then awb_nr end)*100/count(awb_nr),2),'%') Fake_Attempts_per
from noonbilogmis.reporting.Fake_Attempt_Report_MTD 
where  country = 'eg' and OFD_Date = current_date -1
group by 1,2
)
order by d
'''

[[authview]]
dag_id = 'Neeraj_Auth_views'
task_id = 'base_table_lms_manifest_type'
destination_table = 'noonbilogmis.base_table.manifest_type'
sql = '''
SELECT * FROM `noonltddwh.lms.manifest_type` 
'''



[[authview]]
dag_id = 'Neeraj_Auth_views'
task_id = 'Neeraj_Hub_Performance_new_Report_Query1_eg'
destination_table = 'noonbilogmis.reporting.Hub_Performance_new_Report_Query1_eg'
sql = '''
SELECT 'Noon DAs Prod' AS KPI, cast(round((SUM(number_delivered) + SUM(Total_pickup))/count(distinct username),0) as string) Value FROM noonbilogmis.reporting.Hub_Performance_Report_MTD_Final WHERE OFD_Dt = current_date -1 and substr(username,1,4) = 'noon' and username not like '%misrouted%' and substr(username,1,6) not in ('noony2')
UNION ALL
SELECT 'Noon Vehicle Utilization' as KPI,Concat(round((count(distinct username)*100)/40,2),'%') Value FROM noonbilogmis.reporting.Hub_Performance_Report_MTD_Final WHERE OFD_Dt = current_date -1 and substr(username,1,4) = 'noon' and username not like '%misrouted%'
UNION ALL
SELECT 'OFD Del %' KPI, Concat(round(SUM(d.number_delivered)*100 / SUM(d.Total_OFD),2),'%') Value FROM noonbilogmis.reporting.Hub_Performance_Report_MTD_Final d WHERE OFD_Dt = current_date -1 and hub is not null and hub not in ('CAI-Y2')
UNION ALL
SELECT 'Del Count' , cast(SUM(d.number_delivered) as string) FROM noonbilogmis.reporting.Hub_Performance_Report_MTD_Final d WHERE OFD_Dt = current_date -1 and hub is not null and hub not in ('CAI-Y2')
UNION ALL
SELECT 'PDD Breach Count', CAST(SUM(logistics_breach_ne) as string)
 FROM noonbilogmis.reporting.PDD_Breach_Report_MTD_mail
 where estimated_delivery_at = current_date -1
 and Dispatch_Center not in ('CAI-T1')
UNION ALL
SELECT 'PDD Breach%', concat(round(SUM(logistics_breach_ne)*100/count(awb_nr ),2),'%')
 FROM noonbilogmis.reporting.PDD_Breach_Report_MTD_mail
 where estimated_delivery_at = current_date -1
 and Dispatch_Center not in ('CAI-T1')
UNION ALL
Select 'FA Del %', Concat(round(COUNT(distinct case when end_status ='Delivered' then awb_nr end)*100/Count(awb_nr),2),'%') From
 
 (
 SELECT ofd.hub,ofd.hub_sector , ofd.ofd_date,hubr.hub_subsector , ofd.end_status , ofd.awb_nr FROM (SELECT id_item, MIN(d.created_at) created_at FROM `noonltddwh.lms.session_field_item` d GROUP BY 1) T INNER JOIN `noonltddwh.lms.session_field_item` sfi on T.id_item = sfi.id_item and T.created_at = sfi.created_at INNER JOIN noonltddwh.lms.item ie ON sfi.id_item = ie.id_item INNER JOIN `noonbilogmis.reporting.OFD_Forward` ofd on ie.awb_nr = ofd.awb_nr and DATE(sfi.created_at ) = ofd.ofd_date LEFT JOIN ( SELECT awb_nr , hub.code as Hub,hss.code hub_subsector from `noonltddwh.lms.client_pkg_ref` client_pkg_ref LEFT JOIN(select * from `noonltddwh.lms.creq_leg` where id_creq_leg_type=3) as creq_leg using (id_creq) LEFT JOIN `noonltddwh.lms.hub_sector` hub_sector on hub_sector.id_hub_sector=creq_leg.id_hub_sector LEFT JOIN `noonltddwh.lms.hub` hub on hub_sector.id_hub=hub.id_hub left join `noonltddwh.lms.hub_subsector` hss using(id_hub_subsector) ) Hubr ON ie.awb_nr = hubr.awb_nr WHERE DATE(ofd.ofd_date ) >= DATE_TRUNC(CURRENT_DATE() -1, MONTH) GROUP BY 1,2,3,4,5,6)
 where ofd_date = current_date -1
 and hub_subsector not like '%RTV%'
 and hub not in ('CAI-Y2')

UNION ALL
Select 'FA Del Count', CAST(COUNT(distinct case when end_status ='Delivered' then awb_nr end) as string) From
 
 (
 SELECT ofd.hub,ofd.hub_sector , ofd.ofd_date,hubr.hub_subsector , ofd.end_status , ofd.awb_nr FROM (SELECT id_item, MIN(d.created_at) created_at FROM `noonltddwh.lms.session_field_item` d GROUP BY 1) T INNER JOIN `noonltddwh.lms.session_field_item` sfi on T.id_item = sfi.id_item and T.created_at = sfi.created_at INNER JOIN noonltddwh.lms.item ie ON sfi.id_item = ie.id_item INNER JOIN `noonbilogmis.reporting.OFD_Forward` ofd on ie.awb_nr = ofd.awb_nr and DATE(sfi.created_at ) = ofd.ofd_date LEFT JOIN ( SELECT awb_nr , hub.code as Hub,hss.code hub_subsector from `noonltddwh.lms.client_pkg_ref` client_pkg_ref LEFT JOIN(select * from `noonltddwh.lms.creq_leg` where id_creq_leg_type=3) as creq_leg using (id_creq) LEFT JOIN `noonltddwh.lms.hub_sector` hub_sector on hub_sector.id_hub_sector=creq_leg.id_hub_sector LEFT JOIN `noonltddwh.lms.hub` hub on hub_sector.id_hub=hub.id_hub left join `noonltddwh.lms.hub_subsector` hss using(id_hub_subsector) ) Hubr ON ie.awb_nr = hubr.awb_nr WHERE DATE(ofd.ofd_date ) = CURRENT_DATE() -1 GROUP BY 1,2,3,4,5,6)
 where ofd_date = current_date -1
 and hub_subsector not like '%RTV%'
 and hub not in ('CAI-Y2')
 
UNION ALL

SELECT 'NV-OFD %', Concat(round((t1.cnt)*100 /t.cnt,2),"%")
FROM (
SELECT ofd_date , count(1)cnt 
FROM `noonbilogmis.reporting.OFD_Forward` 
where ofd_date = current_Date -1
group by 1
) t 
left join 
(
WITH D as (SELECT    i.awb_nr,lt.code loc_type , ish.*,hl.code Hub_loc,hl.location_code,row_number() over(partition by ish.id_item order by ish.created_at ) rw FROM      `noonltddwh.lms.item_state_history` ish left join `noonltddwh.lms.hub_location` hl  on (ish.id_loc_type,ish.loc_id) = (1,id_hub_location) LEFT JOIN `noonltddwh.lms.creq_leg` cl using(id_creq_leg) LEFT JOIN `noonltddwh.lms.item` i on ish.id_item = i.id_item LEFT JOIN `noonltddwh.lms.hub_sector` h using(id_hub_sector) LEFT JOIN `noonltddwh.lms.country_zone` cz using(id_country_zone) LEFT JOIN noonltddwh.lms.loc_type lt using(id_loc_type) where      substr(cz.code,0,2) ='EG' and Date(ish.created_at) >= current_date -100 )SELECT DATE(OFD_dt ) dt, COUNT(1) Cnt FROM ( SELECT t1.awb_nr,t1.created_at OFD_dt,t2.created_at Hub_dt,t2.Hub_loc,T2.location_code,ofd.end_status , ofd.username FROM (SELECT d.* FROM (SELECT awb_nr,DATE(created_at) ct, MIN(created_at) created_at FROM d WHERE id_loc_type = 2 GROUP BY 1,2 ) t4 INNER JOIN d d ON t4.awb_nr = d.awb_nr and t4.created_at = d.created_at)t1 left join d t2 on t1.id_item = t2.id_item and t1.rw-1 = t2.rw LEFT JOIN `noonbilogmis.reporting.OFD_Forward` ofd on ofd.awb_nr = t1.awb_nr and ofd.ofd_date = DATE(t1.created_at) WHERE t1.id_loc_type = 2 and DATE(t1.created_at ) = current_Date -1 and t2.Hub_loc like '%-OVF%' ) group by 1
) t1 on t.ofd_date = t1.dt
'''


[[authview]]
dag_id = 'Neeraj_Auth_views'
task_id = 'Neeraj_Hub_Performance_Report_MTD_Final'
destination_table = 'noonbilogmis.reporting.Hub_Performance_Report_MTD_Final'
sql = '''
SELECT    distinct coalesce(ofd_date,ofp_date) OFD_Dt, coalesce(forw.username,CIR.username) username,  coalesce(forw.Hub,CIR.hub) Hub
          ,SUM(Total_OFD) Total_OFD
          ,SUM(total_touchpoints) total_touchpoints
          ,SUM(total_Del_touchpoints) total_Del_touchpoints
          ,SUM(number_delivered) number_delivered
          ,SUM(number_address_changed) number_address_changed,SUM(number_address_misrouted) number_address_misrouted
          ,SUM(number_customer_canceled) number_customer_canceled,SUM(number_held_consolidation) number_held_consolidation
          ,SUM(number_Not_Attempted) number_Not_Attempted
          ,SUM(number_rescheduled) number_rescheduled
          ,SUM(number_RTO) number_RTO
          ,SUM(number_unreachable) number_unreachable
          ,SUM(number_partner_not_available_or_wh_closed) number_partner_not_available_or_wh_closed
          ,SUM(number_attempts_exhausted) number_attempts_exhausted
          ,SUM(und_delivered) und_delivered
          
          ,SUM(NDD_Total_OFD) NDD_Total_OFD
          ,SUM(NDD_Total_Delivered) NDD_Total_Delivered
          ,SUM(VIP_Total_OFD) VIP_Total_OFD
          
          ,SUM(VIP_Total_Delivered) VIP_Total_Delivered
          ,SUM(Loyalty_Total_OFD) Loyalty_Total_OFD
          ,SUM(Loyalty_Total_Delivered) Loyalty_Total_Delivered
          
          ,SUM(Total_assigned) Total_assigned
          ,SUM(Total_pickup) Total_pickup
FROM      (
SELECT  hub,username,ofd_date,country
        ,COUNT(distinct awb_nr) Total_OFD
        ,count(distinct case when end_status='Delivered' then awb_nr end) as number_delivered
        ,count(distinct case when end_status IN ('address_changed','address_changed_reroute','address_changed_warning') then awb_nr end) as     
        number_address_changed
        ,count(distinct case when end_status='address_misrouted' then awb_nr end) as number_address_misrouted
        ,count(distinct case when end_status='customer_canceled' then awb_nr end) as number_customer_canceled
        ,count(distinct case when end_status='held_consolidation' then awb_nr end) as number_held_consolidation
        ,count(distinct case when end_status='Not_Attempted' then awb_nr end) as number_Not_Attempted
        ,count(distinct case when end_status='rescheduled' then awb_nr end) as number_rescheduled
        ,count(distinct case when end_status='RTO' then awb_nr end) as number_RTO
        ,count(distinct case when end_status like 'unreachable%' then awb_nr end) as number_unreachable
        ,count(distinct case when end_status='partner_not_available_or_wh_closed' then awb_nr end) as number_partner_not_available_or_wh_closed
        ,count(distinct case when end_status='attempts_exhausted' then awb_nr end) as number_attempts_exhausted
        ,count(distinct case when end_status NOT IN 
        ('Delivered','address_changed','address_changed_reroute','address_changed_warning','address_misrouted','customer_canceled','held_consolidation') 
        then awb_nr end) as und_delivered
        ,COUNT(DISTINCT total_touchpoints) total_touchpoints
        ,COUNT(DISTINCT CASE WHEN  end_status='Delivered' THEN total_touchpoints END ) total_Del_touchpoints
        
        ,COUNT(distinct case when ndd_awb is not null then awb_nr end) NDD_Total_OFD
        ,COUNT(distinct case when ndd_awb is not null and  end_status='Delivered' then awb_nr end) NDD_Total_Delivered
        
        ,COUNT(distinct case when VIP_awb is not null then awb_nr end) VIP_Total_OFD
        ,COUNT(distinct case when VIP_awb is not null and  end_status='Delivered' then awb_nr end) VIP_Total_Delivered
        
        ,COUNT(distinct case when Loyalty_awb is not null then awb_nr end) Loyalty_Total_OFD
        ,COUNT(distinct case when Loyalty_awb is not null and  end_status='Delivered' then awb_nr end) Loyalty_Total_Delivered
        
        
FROM    `noonbilogmis.reporting.OFD_Forward` mt
LEFT JOIN
(
SELECT distinct sois.awb_nr ndd_awb
FROM `noonltddwh.sales.sales_order_item_shipment` sois
left join `noonltddwh.sales.sales_order` so using(id_sales_order)
where upper(so.misc) like "%NDD%" 
) NDD on mt.awb_nr = NDD.ndd_awb 
LEFT JOIN
(
SELECT distinct sois.awb_nr VIP_awb
FROM `noonltddwh.sales.sales_order_item_shipment` sois
left join `noonltddwh.sales.sales_order` so using(id_sales_order)
where upper(so.misc) like "%VIP%" 
) VIP on mt.awb_nr = VIP.VIP_awb 
LEFT JOIN
(
SELECT distinct awb_nr Loyalty_awb
FROM `noonbilogoi.UAE_data.loyalty_uae` where country = 'eg'
) Loyalty on mt.awb_nr = Loyalty.Loyalty_awb 
WHERE   country = 'eg'
AND     ofd_date >= DATE_TRUNC(CURRENT_DATE()-1, MONTH)
GROUP BY 1,2,3,4
) Forw
full join
(
SELECT Hub,OFP_Date,username,count(1) Total_assigned,SUM(case when status = 'closed' then 1 else 0 end) as Total_pickup
FROM
(SELECT
client_ref,
username,
id_creq_leg_job,
status.code AS status,
case when reason.code = 'rescheduled' then 0 else 1 end as flag,
DATE(timestamp_add(creq_leg_job.updated_at,interval 2 hour) ) AS OFP_Date,
hub.code AS hub,
reason.code AS reason,
id_customer,
creq_leg_job.is_attempt
FROM (
SELECT creq_leg_job.*
FROM (
SELECT id_creq_leg,DATE(updated_at) Dt, MAX(id_creq_leg_job) dt1
FROM `noonltddwh.lms.creq_leg_job` creq_leg_job
GROUP BY 1,2
) T INNER JOIN `noonltddwh.lms.creq_leg_job` creq_leg_job ON  creq_leg_job.id_creq_leg_job = T.dt1
) creq_leg_job
LEFT JOIN `noonltddwh.lms.status` status USING(id_status)
LEFT JOIN `noonltddwh.lms.creq_leg` creq_leg USING (id_creq_leg)
LEFT JOIN `noonltddwh.lms.address` address on creq_leg.id_address=address.id_address
LEFT JOIN `noonltddwh.lms.user` user USING(id_user)
LEFT JOIN `noonltddwh.lms.creq` creq USING(id_creq)
LEFT JOIN `noonltddwh.lms.reason` reason USING(id_reason)
LEFT JOIN `noonltddwh.lms.country_zone` country_zone USING (id_country_zone)
LEFT JOIN `noonltddwh.lms.hub_sector` hub_sector USING (id_hub_sector)
LEFT JOIN `noonltddwh.lms.hub` hub USING (id_hub)
WHERE country_zone.id_country=3
and id_creq_type =2
and DATE(timestamp_add(creq_leg_job.updated_at,interval 2 hour) ) >= DATE_TRUNC(CURRENT_DATE()-1, MONTH)
AND status.code NOT IN ('opened','pending')
)
where flag = 1
GROUP BY 1,2,3
) CIR ON (forw.hub,forw.username,forw.ofd_date) = (CIR.Hub,CIR.username,CIR.ofp_date)
GROUP BY 1,2,3
'''


[[authview]]
dag_id = 'Neeraj_Auth_views'
task_id = 'Neeraj_PDD_Breach_Report_MTD_mail'
destination_table = 'noonbilogmis.reporting.PDD_Breach_Report_MTD_mail'
sql = '''
Select
so.country_code as Country,DATE(estimated_delivery_at) estimated_delivery_at,
Dispatch_Center,
hub_sector,
sois.created_at,
sois.awb_nr,
CASE WHEN Received_by_LPC IS NULL THEN Received_by_LPC2 ELSE Received_by_LPC END AS Received_by_LPC,
Received_by_Hub,
hubw2.Received_by_W2 ,
CASE WHEN T.awb is NOT NULL THEN 'tpl_carrier_switch' end as tpl_flag,
count(distinct case when sois.id_carrier =9 then sois.awb_nr end ) as total_shippable_awb_ne,
count(distinct case when b.shipped_at>=(cast(sois.created_at as timestamp)) and sois.created_at is not null and (nexfa.FA_Date>date(estimated_delivery_at) or nexfa.FA_Date is null) and sois.id_carrier in (9) and soish.id is null then sois.awb_nr  end) as logistics_breach_ne,
count(distinct case when  (nexfa.FA_Date>date(estimated_delivery_at) or nexfa.FA_Date is null) and soish.id is null and sois.id_carrier in (9) then sois.awb_nr  end) as Overall_Breach
from  `noonltddwh.sales.sales_order_item` soi
LEFT JOIN `noonltddwh.sales.sales_order` so on soi.id_sales_order=so.id_sales_order
LEFT JOIN `noonltddwh.sales.sales_order_item_shipment` sois on soi.id_sales_order_item_shipment=sois.id_sales_order_item_shipment
LEFT JOIN
(
select id_sales_order_item id,MAX(created_at)created_at FROM `noonltddwh.sales.sales_order_item_status_history` where id_sales_order_item_status = 6 group by 1
)soish on soi.id_sales_order_item = soish.id and DATE(soish.created_at) <= date(estimated_delivery_at)
LEFT JOIN
(
SELECT awb_nr awb
FROM   `noonltddwh.lms.item_state_history` s
        inner join
        noonltddwh.lms.item using(id_item)
        left join
        noonltddwh.lms.loc_type lt using(id_loc_type)
where s.id_loc_type = 18
) T ON T.awb = sois.awb_nr 
LEFT JOIN `noonltddwh.purchase_v2.purchase_order_item` poi on soi.id_sales_order_item =poi.id_sales_order_item
LEFT JOIN `noonltddwh.oms.purchase_item` pi USING (purchase_item_nr)
LEFT JOIN `noonltddwh.oms.stock_item` b using (id_purchase_item)
LEFT JOIN `noonltddwh.oms.sales_item` si using (id_sales_item)
LEFT JOIN (SELECT awb_nr, DATE(FA_TS) FA_Date FROM `noonbilogoi.LMS_CORE.NE_FA`) nexfa on nexfa.AWB_nr=sois.awb_nr
LEFT JOIN(select awb_nr , InBound_ManifestNr ,In_Created_At from `noonltddwh.lms.item` 
LEFT JOIN (SELECT id_item,manifest_nr as InBound_ManifestNr ,manifest_item.created_at  as In_Created_At,  FROM `noonltddwh.lms.manifest_item` manifest_item
LEFT JOIN `noonltddwh.lms.manifest` using (id_manifest)
          WHERE is_outbound=0)  in_manifest_item  USING (id_item)
) Manifest on sois.awb_nr=Manifest.awb_nr
left join (select awb_nr , hub.code as Dispatch_Center,hub_sector.code hub_sector   from `noonltddwh.lms.item` item
left join `noonltddwh.lms.item_pkg` item_pkg on item_pkg.id_item=item.id_item
left join `noonltddwh.lms.creq_leg`  as creq_legx using (id_creq_leg)
left join `noonltddwh.lms.creq` creq using (id_creq)
left join (select * from  `noonltddwh.lms.creq_leg` where id_creq_leg_type =3) as creq_leg using(id_creq)
left join `noonltddwh.lms.hub_sector` as hub_sector on hub_sector.id_hub_sector =creq_leg.id_hub_sector
left join `noonltddwh.lms.hub` hub using (id_hub)) as hubs on  sois.awb_nr = hubs.awb_nr 
LEFT JOIN
(
select awb_nr,
min(ish.created_at) as Received_by_hub 
from `noonltddwh.lms.item_state_history` ish
left join `noonltddwh.lms.hub_location` hl  on (ish.id_loc_type,ish.loc_id) = (1,id_hub_location) 
left join `noonltddwh.lms.item` using (id_item)
where id_loc_type = 1 and hl.code not like "%Z1%" AND hl.code not like "%Z2%" AND hl.code not like "%W1%" AND hl.code not like "%W2%" 
group by 1
) as hubr on sois.awb_nr=hubr.awb_nr
LEFT JOIN
(
select awb_nr,
min(ish.created_at) as Received_by_LPC
from `noonltddwh.lms.item_state_history` ish
left join `noonltddwh.lms.hub_location` hl  on (ish.id_loc_type,ish.loc_id) = (1,id_hub_location) 
left join `noonltddwh.lms.item` using (id_item)
where id_loc_type = 1 and hl.code like "%Z1%" 
group by 1
) as hubr1 on sois.awb_nr=hubr1.awb_nr
LEFT JOIN
(
select awb_nr,
min(ish.created_at) as Received_by_LPC2
from `noonltddwh.lms.item_state_history` ish
left join `noonltddwh.lms.hub_location` hl  on (ish.id_loc_type,ish.loc_id) = (1,id_hub_location) 
left join `noonltddwh.lms.item` using (id_item)
where id_loc_type = 1 and hl.code like "%Z2%" 
group by 1
) as hubr2 on sois.awb_nr=hubr2.awb_nr
LEFT JOIN
(
select awb_nr,
min(ish.created_at) as Received_by_W2
from `noonltddwh.lms.item_state_history` ish
left join `noonltddwh.lms.hub_location` hl  on (ish.id_loc_type,ish.loc_id) = (1,id_hub_location) 
left join `noonltddwh.lms.item` using (id_item)
where id_loc_type = 1 and hl.code like "%W2%" 
group by 1
) as hubw2 on sois.awb_nr=hubw2.awb_nr
where date(estimated_delivery_at) < current_Date()
AND date(estimated_delivery_at) >= date_add(DATE_TRUNC(CURRENT_DATE()-1, MONTH),interval -1 day)
and InBound_ManifestNr is not null
and date(In_Created_At)<current_date()
and id_sales_order_item_status not in (1,7,9)
and id_invoice_section = 1
and b.shipped_at <= b.delivered_at 
and id_cart_type = 1
and so.country_code IN ('EG')
group by 1,2,3,4,5,6,7,8,9,10
'''


[[authview]]
dag_id = 'Neeraj_Auth_views'
task_id = 'Neeraj_Shipments_Allocation4'
destination_table = 'noonbilogmis.reporting.Shipments_Allocation4'
sql = '''
SELECT hub.Hub , hub.hub_sector , hub.country_zone, DATE(sois.created_at) ship_dt,pw.display_name ,city.name_en city ,COUNT(1) Cnt
FROM  noonltddwh.sales.sales_order_item_shipment sois
      LEFT JOIN
      `noonltddwh.partner.partner_warehouse` pw on sois.id_partner_warehouse_origin = pw.id_partner_warehouse 
      LEFT JOIN
      (
      SELECT awb_nr aw, origin_hub  FROM `noonbilogmis.reporting.EG_Pending_Data` 
      ) pd ON pd.aw = sois.awb_nr 
      left join
      noonltddwh.sales.sales_order so using(id_sales_order)
      left join `noonltddwh.bqsync.customer_address` as ca on ca.address_code=so.address_code
      left join `noonltddwh.ref.city` as city on ca.id_city=city.id_city
      left join
      noonltddwh.lms.item i using(awb_nr)
      left join
      (
      select id_item, MIN(ish.created_at) as Total_lpc_Receiving
      from `noonltddwh.lms.item_state_history` ish
      left join `noonltddwh.lms.hub_location`  hl  on (ish.id_loc_type,ish.loc_id) = (1,id_hub_location) 
      where id_loc_type = 1 and hl.code like "%Z1%" 
      group by 1
      ) t on i.id_item = t.id_item
      LEFT JOIN
      (
      select id_item, MAX(ish.created_at) as Total_hub_Receiving
      from `noonltddwh.lms.item_state_history` ish
      left join `noonltddwh.lms.hub_location`  hl  on (ish.id_loc_type,ish.loc_id) = (1,id_hub_location) 
      where id_loc_type = 1 and hl.code not like "%Z1%" 
      group by 1
      ) t1 ON t.id_item = t1.id_item and Total_HUB_Receiving > Total_LPC_Receiving
LEFT JOIN (
SELECT awb_nr , hub.code as Hub, hub_sector.code hub_sector, country_zone.code country_zone
from `noonltddwh.lms.client_pkg_ref` client_pkg_ref
LEFT JOIN(select * from `noonltddwh.lms.creq_leg` where id_creq_leg_type=3) as creq_leg using (id_creq)
LEFT JOIN `noonltddwh.lms.hub_sector` hub_sector on hub_sector.id_hub_sector=creq_leg.id_hub_sector
LEFT JOIN `noonltddwh.lms.hub` hub on hub_sector.id_hub=hub.id_hub
left join noonltddwh.lms.country_zone country_zone using(id_country_zone)
) hub on hub.awb_nr = i.awb_nr  
where   DATE(sois.created_at ) >= DATE_TRUNC(CURRENT_DATE() -1, MONTH)
and so.country_code = 'EG'
and hub.Hub is not null
GROUP BY 1,2,3,4,5,6
'''




[[authview]]
dag_id = 'Neeraj_Auth_views'
task_id = 'Neeraj_LPC_MF_Pack_To_Unpack_Raw_Weekly'
destination_table = 'noonbilogmis.reporting.LPC_MF_Pack_To_Unpack_Raw_Weekly'
sql = '''
SELECT *,timestamp_diff(unpack_dt,manifest_dt.manifest_dt,Hour) TAT
FROM (
SELECT i.awb_nr,substr(pw.display_name,6,100) wh_type,m.manifest_nr  , timestamp_add(MIN(ish.created_at),interval 2 hour)  manifest_dt
FROM 
(SELECT id_item id1,MIN(created_at) dt FROM `noonltddwh.lms.item_state_history` WHERE id_loc_type = 5 GROUP BY 1) t12
INNER JOIN  `noonltddwh.lms.item_state_history` ish ON ish.id_item = t12.id1 and ish.created_at = t12.dt
left join noonltddwh.lms.item i using(id_item)
LEFT JOIN noonltddwh.sales.sales_order_item_shipment sois using(awb_nr)
left join `noonltddwh.partner.partner_warehouse` pw ON sois.id_partner_warehouse_origin = pw.id_partner_warehouse
left join `noonltddwh.lms.hub_location` hl  on (ish.id_loc_type,ish.loc_id) = (1,id_hub_location)
left join noonltddwh.lms.item_pkg ip on ish.id_item = ip.id_item
left join noonltddwh.lms.creq_leg cl on ip.id_creq_leg = cl.id_creq_leg
left join noonltddwh.lms.creq c on cl.id_creq = c.id_creq
left join `noonltddwh.lms.hub_sector` hs using(id_hub_sector)
left join `noonltddwh.lms.country_zone` cz using(id_country_zone)
left join `noonltddwh.lms.manifest` m on m.id_manifest = ish.loc_id
where id_loc_type = 5 and id_creq_type = 1
and substr(cz.code,0,2) = 'EG'
group by 1,2,3
) Manifest_dt
LEFT JOIN
(
SELECT i.awb_nr awb,hl.code Hub , timestamp_add(MIN(ish.created_at),interval 2 hour) unpack_dt
FROM (SELECT id_item dd,MIN(created_At) dt FROM  `noonltddwh.lms.item_state_history` where id_loc_type = 1 GROUP BY 1)t
INNER JOIN `noonltddwh.lms.item_state_history` ish on ish.id_item = t.dd and ish.created_At = t.dt
left join noonltddwh.lms.item i using(id_item)
left join `noonltddwh.lms.hub_location` hl  on (ish.id_loc_type,ish.loc_id) = (1,id_hub_location)
where id_loc_type = 1 and hl.code IN ('CAI-Z1-TRIAGE','CAI-Z2-TRIAGE')
group by 1,2
) unpack ON  Manifest_dt.awb_nr = unpack.awb
where DATE(Manifest_dt.manifest_dt) between current_Date -8 And current_Date -1
and unpack.unpack_dt IS NOT NULL
'''

[[authview]]
dag_id = 'Neeraj_Auth_views'
task_id = 'Neeraj_LPC_MF_Pack_To_Unpack_TAT_yesterday'
destination_table = 'noonbilogmis.reporting.LPC_MF_Pack_To_Unpack_TAT_yesterday'
sql = '''
SELECT DISTINCT wh_type, PERCENTILE_CONT(TAT, 0.9) OVER(PARTITION BY wh_type) AS TA_90
FROM (
SELECT *,timestamp_diff(unpack_dt,manifest_dt.manifest_dt,Hour) TAT
FROM (
SELECT i.awb_nr,substr(pw.display_name,6,100) wh_type,m.manifest_nr  , timestamp_add(MIN(ish.created_at),interval 2 hour)  manifest_dt
FROM 
(SELECT id_item id1,MIN(created_at) dt FROM `noonltddwh.lms.item_state_history` WHERE id_loc_type = 5 GROUP BY 1) t12
INNER JOIN  `noonltddwh.lms.item_state_history` ish ON ish.id_item = t12.id1 and ish.created_at = t12.dt
left join noonltddwh.lms.item i using(id_item)
LEFT JOIN noonltddwh.sales.sales_order_item_shipment sois using(awb_nr)
left join `noonltddwh.partner.partner_warehouse` pw ON sois.id_partner_warehouse_origin = pw.id_partner_warehouse
left join `noonltddwh.lms.hub_location` hl  on (ish.id_loc_type,ish.loc_id) = (1,id_hub_location)
left join noonltddwh.lms.item_pkg ip on ish.id_item = ip.id_item
left join noonltddwh.lms.creq_leg cl on ip.id_creq_leg = cl.id_creq_leg
left join noonltddwh.lms.creq c on cl.id_creq = c.id_creq
left join `noonltddwh.lms.hub_sector` hs using(id_hub_sector)
left join `noonltddwh.lms.country_zone` cz using(id_country_zone)
left join `noonltddwh.lms.manifest` m on m.id_manifest = ish.loc_id
where id_loc_type = 5 and id_creq_type = 1
and substr(cz.code,0,2) = 'EG'
group by 1,2,3
) Manifest_dt
LEFT JOIN
(
SELECT i.awb_nr awb,hl.code Hub , timestamp_add(MIN(ish.created_at),interval 2 hour) unpack_dt
FROM (SELECT id_item dd,MIN(created_At) dt FROM  `noonltddwh.lms.item_state_history` where id_loc_type = 1 GROUP BY 1)t
INNER JOIN `noonltddwh.lms.item_state_history` ish on ish.id_item = t.dd and ish.created_At = t.dt
left join noonltddwh.lms.item i using(id_item)
left join `noonltddwh.lms.hub_location` hl  on (ish.id_loc_type,ish.loc_id) = (1,id_hub_location)
where id_loc_type = 1 and hl.code IN ('CAI-Z1-TRIAGE','CAI-Z2-TRIAGE')
group by 1,2
) unpack ON  Manifest_dt.awb_nr = unpack.awb
where DATE(Manifest_dt.manifest_dt) = current_Date -1
and unpack.unpack_dt IS NOT NULL
)
'''

[[authview]]
dag_id = 'Neeraj_Auth_views'
task_id = 'Neeraj_LPC_Receive_To_HT_TAT_Yesterday'
destination_table = 'noonbilogmis.reporting.LPC_Receive_To_HT_TAT_Yesterday'
sql = '''
Select Distinct wh_type, Percentile_cont(LPC_PG, 0.9 Respect Nulls) Over (partition by wh_type) AS LPC_PG,
  Percentile_cont(PG_HT, 0.9 Respect Nulls) Over (partition by wh_type) AS PG_HT,
  Percentile_cont(LPC_HT, 0.9 Respect Nulls) Over (partition by wh_type) AS LPC_HT
  From (
  SELECT LPC.awb_nr,wh_type  , LPC_rec_tm,PG.PG_group_tm,timestamp_add(ht.created_at,interval 2 hour) ht_time
  ,timestamp_diff(PG.PG_group_tm,LPC_rec_tm,hour) LPC_PG
  ,timestamp_diff(timestamp_add(ht.created_at,interval 2 hour),PG.PG_group_tm,hour) PG_HT
  ,timestamp_diff(timestamp_add(ht.created_at,interval 2 hour),LPC_rec_tm,hour) LPC_HT
  FROM
  (
  SELECT i.awb_nr,hl.code Hub,substr(pw.display_name,6,100) wh_type  , timestamp_add(MIN(ish.created_at),interval 2 hour) LPC_rec_tm
  FROM `noonltddwh.lms.item_state_history` ish
  left join noonltddwh.lms.creq_leg cl using(id_creq_leg)
  left join noonltddwh.lms.creq c using(id_creq)
  left join noonltddwh.lms.item i using(id_item)
  left join noonltddwh.sales.sales_order_item_shipment sois using(awb_nr)
  left join `noonltddwh.partner.partner_warehouse` pw ON sois.id_partner_warehouse_origin = pw.id_partner_warehouse
  left join `noonltddwh.lms.hub_location` hl  on (ish.id_loc_type,ish.loc_id) = (1,id_hub_location)
  where id_loc_type = 1 and hl.code = 'CAI-Z1-TRIAGE' and id_creq_type = 1
  group by 1,2,3
  ) LPC
  LEFT JOIN
  (
  SELECT T1.*
  FROM (
  SELECT id_item,timestamp_add(MIN(ish.created_at),interval 2 hour) c 
  FROM  
  (
  SELECT ish.id_item id ,MIN(ish.created_at )dt
  FROM  `noonltddwh.lms.item_state_history` ish
  left join `noonltddwh.lms.hub_location` hl  on (ish.id_loc_type,ish.loc_id) = (1,id_hub_location)
  LEFT JOIN noonltddwh.lms.item i using(id_item)
  where id_loc_type = 1 and hl.code = 'CAI-Z1-TRIAGE' 
  GROUP BY 1
  ) t INNER JOIN  `noonltddwh.lms.item_state_history` ish on t.id = ish.id_item and t.dt <ish.created_at 
    WHERE  id_loc_type = 3 
  GROUP BY 1
  ) t INNER JOIN
  (
  SELECT i.awb_nr , id_item,loc_id , timestamp_add(MIN(ish.created_at),interval 2 hour) PG_group_tm
  FROM 
  (
  SELECT ish.id_item id ,MIN(ish.created_at )dt
  FROM  `noonltddwh.lms.item_state_history` ish
  left join `noonltddwh.lms.hub_location` hl  on (ish.id_loc_type,ish.loc_id) = (1,id_hub_location)
  LEFT JOIN noonltddwh.lms.item i using(id_item)
  where id_loc_type = 1 and hl.code = 'CAI-Z1-TRIAGE' 
  GROUP BY 1
  ) t INNER JOIN  `noonltddwh.lms.item_state_history` ish on t.id = ish.id_item and t.dt <ish.created_at 
  left join noonltddwh.lms.item i using(id_item)
  where id_loc_type = 3
  GROUP BY 1,2,3
  ) T1 ON t.id_item = T1.id_item and t.c = T1.PG_group_tm 
  ) PG ON LPC.awb_nr = PG.awb_nr
  left join `noonbilogoi.LOGKSA_DATA.pgroup` pgroup on pgroup.id_item =  PG.loc_id
  LEFT JOIN noonltddwh.lms.item iw on PG.loc_id = iw.id_item
  left join noonltddwh.lms.hub_transfer ht on ht.hub_transfer_nr = pgroup.hta_start
  where date(LPC_rec_tm) = current_Date-1
  )
'''

[[authview]]
dag_id = 'Neeraj_Auth_views'
task_id = 'Neeraj_LPC_Receive_To_HT_Raw_Weekly'
destination_table = 'noonbilogmis.reporting.LPC_Receive_To_HT_Raw_Weekly'
sql = '''
SELECT LPC.awb_nr,wh_type  , LPC_rec_tm,PG.PG_group_tm,timestamp_add(ht.created_at,interval 2 hour) ht_time
  ,timestamp_diff(PG.PG_group_tm,LPC_rec_tm,hour) LPC_PG
  ,timestamp_diff(timestamp_add(ht.created_at,interval 2 hour),PG.PG_group_tm,hour) PG_HT
  ,timestamp_diff(timestamp_add(ht.created_at,interval 2 hour),LPC_rec_tm,hour) LPC_HT
  FROM
  (
  SELECT i.awb_nr,hl.code Hub,substr(pw.display_name,6,100) wh_type  , timestamp_add(MIN(ish.created_at),interval 2 hour) LPC_rec_tm
  FROM `noonltddwh.lms.item_state_history` ish
  left join noonltddwh.lms.creq_leg cl using(id_creq_leg)
  left join noonltddwh.lms.creq c using(id_creq)
  left join noonltddwh.lms.item i using(id_item)
  left join noonltddwh.sales.sales_order_item_shipment sois using(awb_nr)
  left join `noonltddwh.partner.partner_warehouse` pw ON sois.id_partner_warehouse_origin = pw.id_partner_warehouse
  left join `noonltddwh.lms.hub_location` hl  on (ish.id_loc_type,ish.loc_id) = (1,id_hub_location)
  where id_loc_type = 1 and hl.code = 'CAI-Z1-TRIAGE' and id_creq_type = 1
  group by 1,2,3
  ) LPC
  LEFT JOIN
  (
  SELECT T1.*
  FROM (
  SELECT id_item,timestamp_add(MIN(ish.created_at),interval 2 hour) c 
  FROM  
  (
  SELECT ish.id_item id ,MIN(ish.created_at )dt
  FROM  `noonltddwh.lms.item_state_history` ish
  left join `noonltddwh.lms.hub_location` hl  on (ish.id_loc_type,ish.loc_id) = (1,id_hub_location)
  LEFT JOIN noonltddwh.lms.item i using(id_item)
  where id_loc_type = 1 and hl.code = 'CAI-Z1-TRIAGE' 
  GROUP BY 1
  ) t INNER JOIN  `noonltddwh.lms.item_state_history` ish on t.id = ish.id_item and t.dt <ish.created_at 
    WHERE  id_loc_type = 3 
  GROUP BY 1
  ) t INNER JOIN
  (
  SELECT i.awb_nr , id_item,loc_id , timestamp_add(MIN(ish.created_at),interval 2 hour) PG_group_tm
  FROM 
  (
  SELECT ish.id_item id ,MIN(ish.created_at )dt
  FROM  `noonltddwh.lms.item_state_history` ish
  left join `noonltddwh.lms.hub_location` hl  on (ish.id_loc_type,ish.loc_id) = (1,id_hub_location)
  LEFT JOIN noonltddwh.lms.item i using(id_item)
  where id_loc_type = 1 and hl.code = 'CAI-Z1-TRIAGE' 
  GROUP BY 1
  ) t INNER JOIN  `noonltddwh.lms.item_state_history` ish on t.id = ish.id_item and t.dt <ish.created_at 
  left join noonltddwh.lms.item i using(id_item)
  where id_loc_type = 3
  GROUP BY 1,2,3
  ) T1 ON t.id_item = T1.id_item and t.c = T1.PG_group_tm 
  ) PG ON LPC.awb_nr = PG.awb_nr
  left join `noonbilogoi.LOGKSA_DATA.pgroup` pgroup on pgroup.id_item =  PG.loc_id
  LEFT JOIN noonltddwh.lms.item iw on PG.loc_id = iw.id_item
  left join noonltddwh.lms.hub_transfer ht on ht.hub_transfer_nr = pgroup.hta_start
  where date(LPC_rec_tm) between current_Date-8 And current_Date-1
'''

[[authview]]
dag_id = 'Neeraj_Auth_views'
task_id = 'Neeraj_LPC_Receive_To_TPL_TAT_Yesterday'
destination_table = 'noonbilogmis.reporting.LPC_Receive_To_TPL_TAT_Yesterday'
sql = '''
SELECT DISTINCT wh_type, PERCENTILE_CONT(LPC_TPL, 0.9 RESPECT NULLS) OVER(PARTITION BY wh_type) AS LPC_TPL,
PERCENTILE_CONT(LPC_PG, 0.9 RESPECT NULLS) OVER(PARTITION BY wh_type) AS LPC_PG,
PERCENTILE_CONT(TPL_PG, 0.9 RESPECT NULLS) OVER(PARTITION BY wh_type) AS TPL_PG
FROM (SELECT distinct LPC.awb_nr ,wh_type,TPL_Loc_time,LPC.created_at PG_datetime,Manifest_time, 
timestamp_diff(TPL_Loc_time,LPC.created_at ,hour) LPC_TPL, timestamp_diff(Manifest_time,LPC.created_at ,hour)
LPC_PG, timestamp_diff(Manifest_time,TPL_Loc_time ,hour) TPL_PG 
FROM (SELECT Hubr.awb_nr,ish.id_item,substr(pw.display_name,6,100) wh_type ,
MIN(ish.created_at ) created_at from `noonltddwh.lms.item_state_history` ish 
INNER JOIN(select * from `noonltddwh.lms.creq_leg` where id_creq_leg_type=3) as creq_leg using (id_creq_leg) 
left join `noonltddwh.lms.hub_location`  hl  on (ish.id_loc_type,ish.loc_id) = (1,id_hub_location)
left join noonltddwh.lms.user u on ish.id_user_updater = u.id_user 
left join noonltddwh.lms.item i on ish.id_item = i.id_item 
left join noonltddwh.sales.sales_order_item_shipment sois using(awb_nr)
left join `noonltddwh.partner.partner_warehouse` pw ON sois.id_partner_warehouse_origin = pw.id_partner_warehouse 
LEFT JOIN(SELECT awb_nr , hub.code as Hub,id_creq_leg,substr(cz.code,0,2) country from `noonltddwh.lms.client_pkg_ref` client_pkg_ref
LEFT JOIN(select * from `noonltddwh.lms.creq_leg` where id_creq_leg_type=3) as creq_leg using (id_creq)
LEFT JOIN `noonltddwh.lms.hub_sector` hub_sector on hub_sector.id_hub_sector=creq_leg.id_hub_sector 
LEFT JOIN `noonltddwh.lms.hub` hub on hub_sector.id_hub=hub.id_hub 
LEFT JOIN `noonltddwh.lms.country_zone` cz using(id_country_zone)) Hubr ON Hubr.awb_nr = i.awb_nr 
WHERE hubr.hub like '%T1%' and hubr.id_creq_leg = ish.id_creq_leg 
and id_loc_type = 1 and hl.code like "%Z1%" and country = 'EG'GROUP BY 1,2,3)  LPC
LEFT JOIN(SELECT ish.id_item , MAX(ish.created_at ) TPL_Loc_time 
FROM  `noonltddwh.lms.item_state_history` ish 
INNER JOIN(select * from `noonltddwh.lms.creq_leg`
where id_creq_leg_type=3) as creq_leg using (id_creq_leg)
left join `noonltddwh.lms.hub_location`  hl  on (ish.id_loc_type,ish.loc_id) = (1,id_hub_location)
WHERE  hl.code like "%TPL%" and id_loc_type = 1 
GROUP BY 1) TPL ON TPL.id_item = LPC.id_item 
LEFT JOIN(SELECT ish.id_item , MAX(ish.created_at ) Manifest_time 
FROM  `noonltddwh.lms.item_state_history`  ish  
INNER JOIN(select * from `noonltddwh.lms.creq_leg` 
where id_creq_leg_type=3) as creq_leg using (id_creq_leg)
WHERE   id_loc_type = 3 
GROUP BY 1) man ON man.id_item = LPC.id_item
WHERE DATE(LPC.created_at) = current_Date -1 and tpl.id_item is not null)
'''

[[authview]]
dag_id = 'Neeraj_Auth_views'
task_id = 'Neeraj_LPC_Receive_To_TPL_rawdata_Weekly'
destination_table = 'noonbilogmis.reporting.LPC_Receive_To_TPL_rawdata_Weekly'
sql = '''
SELECT
  DISTINCT lpc.awb_nr,manifest_nr,
  wh_type,
  TIMESTAMP_ADD(tpl_loc_time,INTERVAL 2 hour) tpl_loc_time,
  TIMESTAMP_ADD(lpc.created_at,INTERVAL 2 hour) lpc_time,
  TIMESTAMP_ADD(manifest_time,INTERVAL 2 hour) pg_time,
  TIMESTAMP_DIFF(tpl_loc_time,lpc.created_at,hour) lpc_tpl,
  TIMESTAMP_DIFF(manifest_time,lpc.created_at,hour) lpc_pg,
  TIMESTAMP_DIFF(manifest_time,tpl_loc_time,hour) tpl_pg
FROM (
  SELECT
    hubr.awb_nr,
    ish.id_item,
    SUBSTR(pw.display_name,6,100) wh_type,
    MIN(ish.created_at ) created_at
  FROM
    `noonltddwh.lms.item_state_history` ish
   

  INNER JOIN (
    SELECT
      *
    FROM
      `noonltddwh.lms.creq_leg`
    WHERE
      id_creq_leg_type=3) AS creq_leg
  USING
    (id_creq_leg)
  LEFT JOIN
    `noonltddwh.lms.hub_location` hl
  ON
    ( ish.id_loc_type,
      ish.loc_id) = (1,
      id_hub_location)
  LEFT JOIN
    noonltddwh.lms.user u
  ON
    ish.id_user_updater = u.id_user
  LEFT JOIN
    noonltddwh.lms.item i
  ON
    ish.id_item = i.id_item
  LEFT JOIN
    noonltddwh.sales.sales_order_item_shipment sois
  USING
    (awb_nr)
  LEFT JOIN
    `noonltddwh.partner.partner_warehouse` pw
  ON
    sois.id_partner_warehouse_origin = pw.id_partner_warehouse
  LEFT JOIN (
    SELECT
      awb_nr,
      hub.code AS hub,
      id_creq_leg,
      SUBSTR(cz.code,0,2) country
    FROM
      `noonltddwh.lms.client_pkg_ref` client_pkg_ref
    LEFT JOIN (
      SELECT
        *
      FROM
        `noonltddwh.lms.creq_leg`
      WHERE
        id_creq_leg_type=3) AS creq_leg
    USING
      (id_creq)
    LEFT JOIN
      `noonltddwh.lms.hub_sector` hub_sector
    ON
      hub_sector.id_hub_sector=creq_leg.id_hub_sector
    LEFT JOIN
      `noonltddwh.lms.hub` hub
    ON
      hub_sector.id_hub=hub.id_hub
    LEFT JOIN
      `noonltddwh.lms.country_zone` cz
    USING
      (id_country_zone)) hubr
  ON
    hubr.awb_nr = i.awb_nr
  WHERE
    hubr.hub LIKE '%T1%'
    AND hubr.id_creq_leg = ish.id_creq_leg
    AND id_loc_type = 1
    AND hl.code LIKE "%Z1%"
    AND country = 'EG'
  GROUP BY
    1,
    2,
    3) lpc
LEFT JOIN (
  SELECT
    ish.id_item,
    MAX(ish.created_at ) tpl_loc_time
  FROM
    `noonltddwh.lms.item_state_history` ish
  INNER JOIN (
    SELECT
      *
    FROM
      `noonltddwh.lms.creq_leg`
    WHERE
      id_creq_leg_type=3) AS creq_leg
  USING
    (id_creq_leg)
  LEFT JOIN
    `noonltddwh.lms.hub_location` hl
  ON
    ( ish.id_loc_type,
      ish.loc_id) = (1,
      id_hub_location)
  WHERE
    hl.code LIKE "%TPL%"
    AND id_loc_type = 1
  GROUP BY
    1) tpl
ON
  tpl.id_item = lpc.id_item
LEFT JOIN (
  SELECT
    ish.id_item,
    MAX(ish.created_at ) manifest_time
  FROM
    `noonltddwh.lms.item_state_history` ish
  INNER JOIN (
    SELECT
      *
    FROM
      `noonltddwh.lms.creq_leg`
    WHERE
      id_creq_leg_type=3) AS creq_leg
  USING
    (id_creq_leg)
  WHERE
    id_loc_type = 3
  GROUP BY
    1) man
ON
  man.id_item = lpc.id_item
  
    LEFT JOIN
  (
  SELECT i.id_item,m.manifest_nr 
  FROM 
  (
  SELECT id_item id, MIN(ish.created_at) dt
  FROM `noonltddwh.lms.item_state_history` ish
  where ish.id_loc_type = 5
  GROUP BY 1
  ) t INNER JOIN  `noonltddwh.lms.item_state_history` ish on t.id = ish.id_item and t.dt = ish.created_at 
  left join noonltddwh.lms.item i using(id_item)
  left join `noonltddwh.lms.manifest` m on m.id_manifest = ish.loc_id
  where id_loc_type = 5
  ) manifest on lpc.id_item = manifest.id_item
WHERE
  DATE(lpc.created_at) >=current_Date - 8
  AND DATE(lpc.created_at) <=current_Date - 1
  AND tpl.id_item IS NOT NULL
'''

[[authview]]
dag_id = 'Neeraj_Auth_views'
task_id = 'Neeraj_RTO_To_WH_TAT_Yesterday'
destination_table = 'noonbilogmis.reporting.RTO_To_WH_TAT_Yesterday'
sql = '''
SELECT DISTINCT Hub
,PERCENTILE_CONT(RTO_Conversion_TO_HT_Creation, 0.9 RESPECT NULLS) OVER(partition by Hub) AS RTO_Conversion_TO_HT_Creation 
,PERCENTILE_CONT(RTO_Conversion_To_LPC_Receiving, 0.9 RESPECT NULLS) OVER(partition by Hub) AS RTO_Conversion_To_LPC_Receiving
,PERCENTILE_CONT(RTO_Conversion_to_WH_Receive, 0.9 RESPECT NULLS) OVER(partition by Hub) AS RTO_Conversion_to_WH_Receive
,PERCENTILE_CONT(HT_to_LPC, 0.9 RESPECT NULLS) OVER(partition by Hub) AS HT_to_LPC
,PERCENTILE_CONT(LPC_to_WH_Receive, 0.9 RESPECT NULLS) OVER(partition by Hub) AS LPC_to_WH_Receive
FROM 
(
SELECT T.*,ma.manifest_nr
,timestamp_diff(HT_datetime,RTO_LegDate,hour) RTO_Conversion_TO_HT_Creation
,timestamp_diff(LPC_Receiving,RTO_LegDate,hour) RTO_Conversion_To_LPC_Receiving
,timestamp_diff(manifest_dt,RTO_LegDate,hour) RTO_Conversion_to_WH_Receive
,timestamp_diff(LPC_Receiving,HT_datetime,hour) HT_to_LPC
,timestamp_diff(manifest_dt,LPC_Receiving,hour) LPC_to_WH_Receive 
FROM (SELECT pd.destination_hub  Hub,pd.awb_nr,MIN(RTO_LegDate) RTO_LegDate ,MIN(manifest_dt) manifest_dt,MIN(Last_LPC_Receiving) LPC_Receiving,MIN(hta_start_ts) HT_datetime,MAX(manifested_dt) manifested_dt 
FROM `noonbilogmis.reporting.EG_Pending_Data` pd 
LEFT JOIN noonltddwh.lms.item i using(awb_nr)
LEFT JOIN
(
SELECT distinct awb_nr
FROM `noonltddwh.xms_sourcing.ship_box`  sb 
LEFT JOIN `noonltddwh.xms_sourcing.warehouse_lane`  wl on wl.id_warehouse = sb.id_warehouse_seller
WHERE wl.id_warehouse_lane_type =3
) tt on tt.awb_nr = pd.awb_nr 
LEFT JOIN(SELECT sh.id_item,m.manifest_nr,timestamp_add(MAX(sh.created_at),interval 2 hour) manifest_dt  
FROM  `noonltddwh.lms.item_state_history`  sh LEFT JOIN `noonltddwh.lms.manifest` m on m.id_manifest = sh.loc_id
LEFT JOIN noonltddwh.lms.creq_leg cl using(id_creq_leg)
WHERE cl.id_creq_leg_type = 4 and sh.id_loc_type = 5 group by 1,2 ) t on i.id_item = t.id_item  
and RTO_LegDate<manifest_dt 
LEFT JOIN(SELECT sh.id_item,timestamp_add(MAX(sh.created_at),interval 2 hour) manifested_dt  
FROM  `noonltddwh.lms.item_state_history`  sh LEFT JOIN `noonltddwh.lms.manifest` m on m.id_manifest = sh.loc_id
LEFT JOIN noonltddwh.lms.creq_leg cl using(id_creq_leg)
WHERE cl.id_creq_leg_type = 4 and sh.id_loc_type = 6 group by 1) manifested on i.id_item = manifested.id_item
and RTO_LegDate<manifested_dt 
LEFT JOIN (select ish.id_item, timestamp_add(ish.created_at,interval 2 hour) as Last_LPC_Receiving 
from `noonltddwh.lms.item_state_history` ish 
left join `noonltddwh.lms.hub_location`  hl  on (ish.id_loc_type,ish.loc_id) = (1,id_hub_location) 
left join noonltddwh.lms.item i on ish.id_item = i.id_item
where id_loc_type = 1 and hl.code  like "%Z1%") LPC on LPC.id_item = i.id_item and RTO_LegDate<=Last_LPC_Receiving 
LEFT JOIN(SELECT i.id_item,hta_start_ts hta_start_ts
FROM (SELECT i1.*FROM (SELECT id_item,ish.created_at HT_datetime
FROM `noonltddwh.lms.item_state_history` ish 
where  ish.id_loc_type = 3 ) i
INNER JOIN `noonltddwh.lms.item_state_history` i1 on i.id_item = i1.id_item 
and i.HT_datetime = i1.created_at )ish 
left join noonltddwh.lms.item i using(id_item) 
left join `noonbilogoi.LOGKSA_DATA.pgroup` pgroup on pgroup.id_item =  ish.loc_id 
left join noonltddwh.lms.hub_transfer ht on ht.hub_transfer_nr = pgroup.hta_start) HT ON HT.id_item = i.id_item
and RTO_LegDate <= hta_start_ts
where DATE(RTO_LegDate) = current_date - 1
and pd.country = 'eg' and t.id_item is not null and pd.destination_hub not in ('CAI-T1') GROUP BY 1,2) T  LEFT JOIN
(
SELECT i.awb_nr ,m.manifest_nr 
FROM `noonltddwh.lms.item_state_history` ish
left join noonltddwh.lms.item i using(id_item)
LEFT JOIN `noonltddwh.lms.manifest` m on m.id_manifest = ish.loc_id
LEFT JOIN noonltddwh.lms.creq_leg cl using(id_creq_leg)
WHERE cl.id_creq_leg_type = 4 and ish.id_loc_type = 5
) ma on ma.awb_nr  = t.awb_nr WHERE HT_datetime IS NOT NULL
)
'''

[[authview]]
dag_id = 'Neeraj_Auth_views'
task_id = 'Neeraj_RTO_To_WH_Raw_Weekly'
destination_table = 'noonbilogmis.reporting.RTO_To_WH_Raw_Weekly'
sql = '''
SELECT T.*,ma.manifest_nr
,timestamp_diff(HT_datetime,RTO_LegDate,hour) RTO_Conversion_TO_HT_Creation
,timestamp_diff(LPC_Receiving,RTO_LegDate,hour) RTO_Conversion_To_LPC_Receiving
,timestamp_diff(manifest_dt,RTO_LegDate,hour) RTO_Conversion_to_WH_Receive
,timestamp_diff(LPC_Receiving,HT_datetime,hour) HT_to_LPC
,timestamp_diff(manifest_dt,LPC_Receiving,hour) LPC_to_WH_Receive 
FROM (SELECT pd.destination_hub  Hub,pd.awb_nr,MIN(RTO_LegDate) RTO_LegDate ,MIN(manifest_dt) manifest_dt,MIN(Last_LPC_Receiving) LPC_Receiving,MIN(hta_start_ts) HT_datetime,MAX(manifested_dt) manifested_dt ,MIN(Received_by_W2) Received_by_W2
FROM `noonbilogmis.reporting.EG_Pending_Data` pd 
LEFT JOIN noonltddwh.lms.item i using(awb_nr)
LEFT JOIN
(
select awb_nr,
min(timestamp_add(ish.created_at,interval 2 hour)) as Received_by_W2
from `noonltddwh.lms.item_state_history` ish
LEFT JOIN noonltddwh.lms.creq_leg cl using(id_creq_leg)
left join `noonltddwh.lms.hub_location` hl  on (ish.id_loc_type,ish.loc_id) = (1,id_hub_location) 
left join `noonltddwh.lms.item` using (id_item)
where id_loc_type = 1 and hl.code like "%W2%" and cl.id_creq_leg_type = 4
group by 1
) as hubw2 on pd.awb_nr=hubw2.awb_nr


LEFT JOIN
(
SELECT distinct awb_nr
FROM `noonltddwh.xms_sourcing.ship_box`  sb 
LEFT JOIN `noonltddwh.xms_sourcing.warehouse_lane`  wl on wl.id_warehouse = sb.id_warehouse_seller
WHERE wl.id_warehouse_lane_type =3
) tt on tt.awb_nr = pd.awb_nr 
LEFT JOIN(SELECT sh.id_item,m.manifest_nr,timestamp_add(MAX(sh.created_at),interval 2 hour) manifest_dt  
FROM  `noonltddwh.lms.item_state_history`  sh LEFT JOIN `noonltddwh.lms.manifest` m on m.id_manifest = sh.loc_id
LEFT JOIN noonltddwh.lms.creq_leg cl using(id_creq_leg)
WHERE cl.id_creq_leg_type = 4 and sh.id_loc_type = 5 group by 1,2 ) t on i.id_item = t.id_item  
and RTO_LegDate<manifest_dt 
LEFT JOIN(SELECT sh.id_item,timestamp_add(MAX(sh.created_at),interval 2 hour) manifested_dt  
FROM  `noonltddwh.lms.item_state_history`  sh LEFT JOIN `noonltddwh.lms.manifest` m on m.id_manifest = sh.loc_id
LEFT JOIN noonltddwh.lms.creq_leg cl using(id_creq_leg)
WHERE cl.id_creq_leg_type = 4 and sh.id_loc_type = 6 group by 1) manifested on i.id_item = manifested.id_item
and RTO_LegDate<manifested_dt 
LEFT JOIN (select ish.id_item, timestamp_add(ish.created_at,interval 2 hour) as Last_LPC_Receiving 
from `noonltddwh.lms.item_state_history` ish 
left join `noonltddwh.lms.hub_location`  hl  on (ish.id_loc_type,ish.loc_id) = (1,id_hub_location) 
left join noonltddwh.lms.item i on ish.id_item = i.id_item
where id_loc_type = 1 and hl.code  like "%Z1%") LPC on LPC.id_item = i.id_item and RTO_LegDate<=Last_LPC_Receiving 
LEFT JOIN(SELECT i.id_item,hta_start_ts hta_start_ts
FROM (SELECT i1.*FROM (SELECT id_item,ish.created_at HT_datetime
FROM `noonltddwh.lms.item_state_history` ish 
where  ish.id_loc_type = 3 ) i
INNER JOIN `noonltddwh.lms.item_state_history` i1 on i.id_item = i1.id_item 
and i.HT_datetime = i1.created_at )ish 
left join noonltddwh.lms.item i using(id_item) 
left join `noonbilogoi.LOGKSA_DATA.pgroup` pgroup on pgroup.id_item =  ish.loc_id 
left join noonltddwh.lms.hub_transfer ht on ht.hub_transfer_nr = pgroup.hta_start) HT ON HT.id_item = i.id_item
and RTO_LegDate <= hta_start_ts
where DATE(RTO_LegDate) >= current_date - 8 and tt.awb_nr is null
and DATE(RTO_LegDate) <= current_date - 1
and pd.country = 'eg' and t.id_item is not null and pd.destination_hub not in ('CAI-T1') GROUP BY 1,2) T  LEFT JOIN
(
SELECT i.awb_nr ,m.manifest_nr 
FROM `noonltddwh.lms.item_state_history` ish
left join noonltddwh.lms.item i using(id_item)
LEFT JOIN `noonltddwh.lms.manifest` m on m.id_manifest = ish.loc_id
LEFT JOIN noonltddwh.lms.creq_leg cl using(id_creq_leg)
WHERE cl.id_creq_leg_type = 4 and ish.id_loc_type = 5
) ma on ma.awb_nr  = t.awb_nr WHERE HT_datetime IS NOT NULL
'''


[[matview]]
dag_id = 'AG_3PL'
task_id = 'with_TPL'
destination_table = 'noonbilogmis.reporting.NS_3pl_handover'
sql = '''
select 
distinct
tpl.awb_nr,
 ifs.country, ifs.hub , ifs.client_ref , date(ifs.promised_at) as EDD, ifs.current_location ,tpl.carrier, ifs.shipment_creation_TS, r_ish.created_at as received_at_logistics_TS,gatway_hub1,
 date(tpl.created_at) as Handover_date , gatway_hub2 , tpl_nr,tpl_manifest_created_at
 --unpacked_at
from noonltddwh.lms.item it 
---
left join (select it.awb_nr , it.id_item,pgroup.id_item as pgroup_id_item, c.code as carrier, thi.created_at,handover_nr as tpl_nr, th.created_at as tpl_manifest_created_at
from `noonltddwh.lms.tpl_handover` th
left join `noonltddwh.lms.carrier` c using(id_carrier)
left join `noonltddwh.lms.tpl_handover_item` thi using(id_tpl_handover)
left join `noonltddwh.lms.item` pgroup on pgroup.id_item = thi.id_item
left join  `noonltddwh.lms.tpl_item_pgroup_map`  ipm on (thi.id_item) = (ipm.id_item_pgroup)
left join `noonltddwh.lms.item` it on (ipm.id_item) = (it.id_item)
where thi.is_outbound = 1 ) tpl using (id_item)
Left join `noonltddwh.lms.container_line` cline using (id_item) --on cline.id_item = pgroup_id_item
left join `noonltddwh.lms.container_state` con_state using (id_loc_type, loc_id)
left join `noonbilogoi.battleplan_v1.item_final_state` ifs on ifs.id_item = tpl.id_item 
left join (
select id_item, min(ish.created_at) as created_at, 
from `noonbilogmis.base_table.lms_item`
left join `noonbilogmis.base_table.lms_item_state_history` ish using (id_item)
left join `noonbilogmis.base_table.lms_hub_location` hl on (ish.id_loc_type, ish.loc_id) = (1, hl.id_hub_location)
where ish.id_loc_type = 1
group by 1) r_ish on r_ish.id_item = tpl.id_item 
-----
left join
(select awb_nr , gatway_hub2, gatway_hub1
from (
SELECT awb_nr ,hl.code gatway_hub2,
FROM 
--g2--
(SELECT id_item,MAX(ish.created_at) created_at
FROM `noonltddwh.lms.item_state_history` ish
LEFT JOIN `noonltddwh.lms.hub_location` hl on ish.loc_id = hl.id_hub_location
LEFT JOIN `noonltddwh.lms.creq_leg` cl using(id_creq_leg)
WHERE ish.id_loc_type = 1 and substr(hl.code,5,1) ='G' and cl.id_creq_leg_type = 3
GROUP BY 1
) dd inner join `noonltddwh.lms.item_state_history` ish on dd.id_item = ish.id_item and dd.created_at = ish.created_at
LEFT JOIN noonltddwh.lms.creq_leg cl using(id_creq_leg)
LEFT JOIN noonltddwh.lms.hub_sector hs on hs.id_hub_sector = cl.id_hub_sector
LEFT JOIN `noonltddwh.lms.hub_location` hl on ish.loc_id = hl.id_hub_location
LEFT JOIN noonltddwh.lms.item i on ish.id_item = i.id_item
WHERE ish.id_loc_type = 1 and substr(hl.code,5,1) ='G'
)g2
--g1---
left join (
SELECT awb_nr as awb_1,hl.code gatway_hub1,
FROM (
SELECT id_item ,Min(ish.created_at) created_at
FROM `noonltddwh.lms.item_state_history` ish
LEFT JOIN `noonltddwh.lms.hub_location` hl on ish.loc_id = hl.id_hub_location
LEFT JOIN `noonltddwh.lms.creq_leg` cl using(id_creq_leg)
WHERE ish.id_loc_type = 1 and substr(hl.code,5,1) ='G' and cl.id_creq_leg_type = 3 
GROUP BY 1
) dd inner join `noonltddwh.lms.item_state_history` ish on dd.id_item = ish.id_item and dd.created_at = ish.created_at
LEFT JOIN noonltddwh.lms.creq_leg cl using(id_creq_leg)
LEFT JOIN noonltddwh.lms.hub_sector hs on hs.id_hub_sector = cl.id_hub_sector
LEFT JOIN `noonltddwh.lms.hub_location` hl on ish.loc_id = hl.id_hub_location
LEFT JOIN noonltddwh.lms.item i on ish.id_item = i.id_item
WHERE ish.id_loc_type = 1 and substr(hl.code,5,1) ='G'
) g1 on g1.awb_1 = g2.awb_nr) d on d.awb_nr = tpl.awb_nr
-----
where date(tpl.created_at) >= Current_date - 7
order by 2 desc
'''


[[authview]]
dag_id = 'Neeraj_Auth_views'
task_id = 'Neeraj_CIR_Picked_To_LPC_TAT_Yesterday'
destination_table = 'noonbilogmis.reporting.CIR_Picked_To_LPC_TAT_Yesterday'
sql = '''
SELECT
  Hub,
  P1[
OFFSET
  (90)] AS Creation_to_FirstAttempt,
  P2[
OFFSET
  (90)] AS Creation_to_PickedUp,
  P3[
OFFSET
  (90)] AS Creation_to_Manifest,
  P4[
OFFSET
  (90)] AS Creation_to_Manifested,
  P6[
OFFSET
  (90)] AS Picked_to_Manifest,
  P7[
OFFSET
  (90)] AS Picked_to_Manifested,
  P8[
OFFSET
  (90)] AS Picked_to_Refund,
  P9[
OFFSET
  (90)] AS Manifest_to_Refund,
  P11[
OFFSET
  (90)] pick_to_lpc,
  P12[
OFFSET
  (90)] LPC_TO_manifested,
  P13[
OFFSET
  (90)] pgroup_to_LPC,
  P14[
OFFSET
  (90)] pick_to_pgroup
FROM (
  SELECT
    Hub,
    APPROX_QUANTILES(Creation_to_FirstAttempt, 100) P1,
    APPROX_QUANTILES(Creation_to_PickedUp, 100) P2,
    APPROX_QUANTILES(Creation_to_Manifest, 100) P3,
    APPROX_QUANTILES(Creation_to_Manifested, 100) P4,
    APPROX_QUANTILES(Creation_to_Picked, 100) P5,
    APPROX_QUANTILES(Picked_to_Manifest, 100) P6,
    APPROX_QUANTILES(Picked_to_Manifested, 100) P7,
    APPROX_QUANTILES(Picked_to_Refund, 100) P8,
    APPROX_QUANTILES(Manifest_to_Refund, 100) P9,
    APPROX_QUANTILES(Manifested_to_Refund, 100) P10,
    APPROX_QUANTILES(Pick_to_LPC, 100) P11,
    APPROX_QUANTILES(LPC_TO_manifested, 100) P12,
    APPROX_QUANTILES(pgroup_to_LPC, 100) P13,
    APPROX_QUANTILES(pick_to_pgroup, 100) P14
  FROM (
    SELECT
      Hub,
      EXTRACT(month
      FROM
        Creation) AS Month,
      creq_nr,
      TIMESTAMP_DIFF(FirstAttempt,Creation,hour) AS Creation_to_FirstAttempt,
      TIMESTAMP_DIFF(PickedUp,Creation,hour) AS Creation_to_PickedUp,
      TIMESTAMP_DIFF(Manifest,Creation,hour) AS Creation_to_Manifest,
      TIMESTAMP_DIFF(Manifested,Creation,hour) AS Creation_to_Manifested,
      TIMESTAMP_DIFF(PickedUp,Creation,hour) AS Creation_to_Picked,
      TIMESTAMP_DIFF(Manifest,PickedUp,hour) AS Picked_to_Manifest,
      TIMESTAMP_DIFF(Manifested,PickedUp,hour) AS Picked_to_Manifested,
      TIMESTAMP_DIFF(Refund,PickedUp,hour) AS Picked_to_Refund,
      TIMESTAMP_DIFF(Refund,Manifest,hour) AS Manifest_to_Refund,
      TIMESTAMP_DIFF(Refund,Manifested,hour) AS Manifested_to_Refund,
      TIMESTAMP_DIFF(Received_by_LPC,PickedUp,hour) AS Pick_to_LPC,
      TIMESTAMP_DIFF(Manifested,Received_by_LPC,hour) AS LPC_TO_manifested,
      TIMESTAMP_DIFF(Received_by_LPC,pgroup_dt,hour) pgroup_to_LPC,
      TIMESTAMP_DIFF(pgroup_dt,PickedUp,hour) AS pick_to_pgroup
    FROM (
      SELECT
        DISTINCT Hub,
        creq_nr,
        pgroup_dt,
        client_ref,
        Manifest,
        Creation,
        FirstAttempt,
        PickedUp,
        Manifested,
        Refund,
        Received_by_LPC,
        manifest_nr,
        awb_nr
      FROM
        `noonltddwh.lms.creq`
      LEFT JOIN
        `noonltddwh.lms.creq_leg`
      USING
        (id_creq )
      LEFT JOIN (
        SELECT
          id_creq_leg,
          hub.code AS Hub
        FROM
          `noonltddwh.lms.creq_leg`
        LEFT JOIN
          `noonltddwh.lms.hub_sector`
        USING
          (id_hub_sector )
        LEFT JOIN
          `noonltddwh.lms.hub` hub
        USING
          (id_hub))
      USING
        (id_creq_leg )
      LEFT JOIN (
        SELECT
          DISTINCT creq_nr,
          CASE
            WHEN EXTRACT(dayofweek FROM MIN(created_at)) =6 THEN TIMESTAMP(DATE_ADD((DATE(MIN(created_at))),INTERVAL 1 day))
            WHEN EXTRACT(hour
          FROM
            MIN(created_at))>=6 THEN TIMESTAMP_ADD(TIMESTAMP(DATE_ADD((DATE(MIN(created_at))),INTERVAL 1 day)),INTERVAL 6 hour)
          ELSE
          MIN(created_at)
        END
          AS Creation
        FROM
          `noonltddwh.lms.creq`
        WHERE
          id_status NOT IN (6,
            2)
        GROUP BY
          1)
      USING
        (creq_nr)
      LEFT JOIN (
        SELECT
          *
        FROM (
          SELECT
            id_creq,
            MIN(CASE
                WHEN json_EXTRACT(DATA, '$.num_attempts')='1' THEN creq_leg_history.created_at
            END
              ) AS Attempted
          FROM
            `noonltddwh.lms.creq` creq
          LEFT JOIN
            `noonltddwh.lms.creq_leg`
          USING
            (id_creq)
          LEFT JOIN
            `noonltddwh.lms.creq_leg_history` creq_leg_history
          USING
            (id_creq_leg)
          GROUP BY
            1))
      USING
        (id_creq)
      LEFT JOIN (
        SELECT
          id_creq,a.awb_nr,
          MIN(a.created_at) AS PickedUp,
          MIN(Received_by_LPC) Received_by_LPC
        FROM
          `noonltddwh.lms.item` a
        LEFT JOIN
          `noonltddwh.lms.item_pkg` b
        USING
          (id_item)
        LEFT JOIN
          `noonltddwh.lms.creq_leg`
        USING
          (id_creq_leg)
        LEFT JOIN
          `noonltddwh.lms.creq`
        USING
          (id_creq)
        LEFT JOIN (
          SELECT
            awb_nr,
            MIN(ish.created_at) AS Received_by_LPC
          FROM
            `noonltddwh.lms.item_state_history` ish
          LEFT JOIN
            `noonltddwh.lms.hub_location` hl
          ON
            (ish.id_loc_type,
              ish.loc_id) = (1,
              id_hub_location)
          LEFT JOIN
            `noonltddwh.lms.item` i
          USING
            (id_item)
          WHERE
            id_loc_type = 1
            AND hl.code LIKE "%Z1%"
          GROUP BY
            1) LPC
        ON
          LPC.awb_nr = a.awb_nr
        GROUP BY
          1,2)
      USING
        (id_creq)
      LEFT JOIN (
        SELECT
          id_creq,
          a.created_at AS Manifested
        FROM
          `noonltddwh.lms.item_state_history` a
        LEFT JOIN
          `noonltddwh.lms.item_pkg` b
        USING
          (id_item)
        LEFT JOIN
          `noonltddwh.lms.creq_leg` cleg
        ON
          cleg.id_creq_leg=b.id_creq_leg
        LEFT JOIN
          `noonltddwh.lms.creq`
        USING
          (id_creq)
        WHERE
          id_loc_type=6
          AND a.id_status=7)
      USING
        (id_creq)
      LEFT JOIN (
        SELECT
          c.id_creq,
          a.created_at AS Manifest,
          m.manifest_nr
        FROM
          `noonltddwh.lms.item_state_history` a
        LEFT JOIN
          `noonltddwh.lms.item_pkg` b
        USING
          (id_item)
        LEFT JOIN
          `noonltddwh.lms.creq_leg` cleg
        ON
          cleg.id_creq_leg=b.id_creq_leg
        LEFT JOIN
          `noonltddwh.lms.creq` c
        USING
          (id_creq)
        LEFT JOIN
          `noonltddwh.lms.manifest` m
        ON
          m.id_manifest = a.loc_id
        WHERE
          id_loc_type=5
          AND m.id_manifest_type = 2 )
      USING
        (id_creq)
      LEFT JOIN (
        SELECT
          DISTINCT return_nr,
          wrml2.created_at AS Refund,
          wrjl.reason_code
        FROM
          `noonltddwh.sales.sales_order_item` soi
        LEFT JOIN
          `noonltddwh.sales.sales_order` so
        USING
          (id_sales_order)
        LEFT JOIN
          `noonltddwh.sales.sales_order_item_shipment` sois
        USING
          (id_sales_order_item_shipment)
        LEFT JOIN
          `noonltddwh.oms.sales_item` si
        ON
          soi.item_nr = si.sales_item_nr
        LEFT JOIN
          `noonltddwh.oms.purchase_item` pi
        USING
          (id_sales_item)
        LEFT JOIN
          `noonltddwh.oms.wms_return_job_line` wrjl
        USING
          (id_purchase_item)
        LEFT JOIN
          `noonltddwh.oms.wms_return_job` wrj
        USING
          (id_wms_return_job)
        LEFT JOIN
          `noonltddwh.oms.wms_return_manifest` wrm
        USING
          (id_wms_return_manifest)
        LEFT JOIN
          `noonltddwh.oms.wms_return_manifest_received_line` wrml
        ON
          wrml.id_wms_return_manifest=wrm.id_wms_return_manifest
        LEFT JOIN
          `noonltddwh.sales_return.sales_return_receipt_item` srri
        ON
          srri.id_sales_order_item=soi.id_sales_order_item
        LEFT JOIN
          `noonltddwh.ref.carrier` cr
        ON
          cr.id_carrier = wrm.id_carrier
        LEFT JOIN
          `noonltddwh.sales_return.sales_return_request_item` srrq
        ON
          srrq.id_sales_order_item=soi.id_sales_order_item
        LEFT JOIN
          `noonltddwh.sales_return.sales_return_request` srr
        USING
          (id_sales_return_request)
        LEFT JOIN
          `noonltddwh.oms.wms_return_manifest_received_line` wrml2
        ON
          wrml2.manual_mp_reference_nr = srr.awb_nr
        WHERE
          wrjl.reason_code IS NULL )
      ON
        client_ref =return_nr
      LEFT JOIN (
        SELECT
          creq_nr,
          MIN(FA) AS FirstAttempt
        FROM (
          SELECT
            *
          FROM (
            SELECT
              creq_nr,
              MIN(creq_leg_history.created_at) AS FA
            FROM
              `noonltddwh.lms.creq` creq
            LEFT JOIN
              `noonltddwh.lms.creq_leg`
            USING
              (id_creq)
            LEFT JOIN (
              SELECT
                *,
                json_EXTRACT(DATA,
                  '$.num_attempts')='1' AS attempt
              FROM
                `noonltddwh.lms.creq_leg_history`) creq_leg_history
            USING
              (id_creq_leg)
            WHERE
              attempt=TRUE
            GROUP BY
              1)
          UNION ALL (
            SELECT
              creq_nr,
              MIN(manifest.created_at) AS FA
            FROM
              `noonltddwh.lms.creq` creq
            LEFT JOIN
              `noonltddwh.lms.manifest` AS manifest
            USING
              (id_creq)
            WHERE
              id_manifest_type=1
            GROUP BY
              1) )
        GROUP BY
          1 )
      USING
        (creq_nr)
      LEFT JOIN (
        SELECT
          c.creq_nr,
          MIN(ish.created_at ) pgroup_dt
        FROM
          `noonltddwh.lms.item_state_history` ish
        LEFT JOIN
          `noonltddwh.lms.creq_leg` cl
        USING
          (id_creq_leg)
        LEFT JOIN
          `noonltddwh.lms.creq` c
        USING
          (id_creq)
        WHERE
          ish.id_loc_type = 3
        GROUP BY
          1 ) pgroup
      USING
        (creq_nr)
      WHERE
        DATE(Creation) = current_Date -1
        AND id_creq_type =2 )AS A )
  GROUP BY
    1)
'''

[[authview]]
dag_id = 'Neeraj_Auth_views'
task_id = 'Neeraj_CIR_Picked_To_LPC_Raw_Weekly'
destination_table = 'noonbilogmis.reporting.CIR_Picked_To_LPC_Raw_Weekly'
sql = '''
SELECT
        DISTINCT Hub,
        creq_nr,
        pgroup_dt,
        client_ref,
        Manifest,
        Creation,
        FirstAttempt,
        PickedUp,
        Manifested,
        Refund,
        Received_by_LPC,
        Received_by_W2,
        manifest_nr,
        awb_nr
      FROM
        `noonltddwh.lms.creq`
      LEFT JOIN
        `noonltddwh.lms.creq_leg`
      USING
        (id_creq )
        LEFT JOIN `noonltddwh.lms.country_zone` cz using(id_country_zone)
      LEFT JOIN (
        SELECT
          id_creq_leg,
          hub.code AS Hub
        FROM
          `noonltddwh.lms.creq_leg` cl
        LEFT JOIN
          `noonltddwh.lms.hub_sector` hs
        on cl.id_hub_sector = hs.id_hub_sector 
        LEFT JOIN
          `noonltddwh.lms.hub` hub
        USING
          (id_hub))
      USING
        (id_creq_leg )
      LEFT JOIN (
        SELECT
          DISTINCT creq_nr,
          CASE
            WHEN EXTRACT(dayofweek FROM MIN(created_at)) =6 THEN TIMESTAMP(DATE_ADD((DATE(MIN(created_at))),INTERVAL 1 day))
            WHEN EXTRACT(hour
          FROM
            MIN(created_at))>=6 THEN TIMESTAMP_ADD(TIMESTAMP(DATE_ADD((DATE(MIN(created_at))),INTERVAL 1 day)),INTERVAL 6 hour)
          ELSE
          MIN(created_at)
        END
          AS Creation
        FROM
          `noonltddwh.lms.creq`
        WHERE
          id_status NOT IN (6,
            2)
        GROUP BY
          1)
      USING
        (creq_nr)
      LEFT JOIN (
        SELECT
          *
        FROM (
          SELECT
            id_creq,
            MIN(CASE
                WHEN json_EXTRACT(DATA, '$.num_attempts')='1' THEN creq_leg_history.created_at
            END
              ) AS Attempted
          FROM
            `noonltddwh.lms.creq` creq
          LEFT JOIN
            `noonltddwh.lms.creq_leg`
          USING
            (id_creq)
          LEFT JOIN
            `noonltddwh.lms.creq_leg_history` creq_leg_history
          USING
            (id_creq_leg)
          GROUP BY
            1))
      USING
        (id_creq)
      LEFT JOIN (
        SELECT
          id_creq,a.awb_nr,
          MIN(a.created_at) AS PickedUp,
          MIN(Received_by_LPC) Received_by_LPC,
          MIN(Received_by_W2) Received_by_W2
        FROM
          `noonltddwh.lms.item` a
        LEFT JOIN
          `noonltddwh.lms.item_pkg` b
        USING
          (id_item)
        LEFT JOIN
          `noonltddwh.lms.creq_leg`
        USING
          (id_creq_leg)
        LEFT JOIN
          `noonltddwh.lms.creq`
        USING
          (id_creq)
        LEFT JOIN (
          SELECT
            awb_nr,
            MIN(ish.created_at) AS Received_by_LPC
            ,MIN(Received_by_W2.Received_by_W2) Received_by_W2
          FROM
            `noonltddwh.lms.item_state_history` ish
          LEFT JOIN
            `noonltddwh.lms.hub_location` hl
          ON
            (ish.id_loc_type,
              ish.loc_id) = (1,
              id_hub_location)
          LEFT JOIN
            `noonltddwh.lms.item` i
           
          USING
            (id_item)
             LEFT JOIN
            (
            select awb_nr awb,
min(ish.created_at) as Received_by_W2
from `noonltddwh.lms.item_state_history` ish
left join `noonltddwh.lms.hub_location` hl  on (ish.id_loc_type,ish.loc_id) = (1,id_hub_location) 
left join `noonltddwh.lms.item` using (id_item)
where id_loc_type = 1 and hl.code like "%W2%" 
group by 1
            
            ) Received_by_W2 on Received_by_W2.awb = i.awb_nr
          WHERE
            id_loc_type = 1
            AND hl.code LIKE "%Z1%"
          GROUP BY
            1) LPC
        ON
          LPC.awb_nr = a.awb_nr
        GROUP BY
          1,2)
      USING
        (id_creq)
        
      LEFT JOIN (
        SELECT
          id_creq,
          a.created_at AS Manifested
        FROM
          `noonltddwh.lms.item_state_history` a
        LEFT JOIN
          `noonltddwh.lms.item_pkg` b
        USING
          (id_item)
        LEFT JOIN
          `noonltddwh.lms.creq_leg` cleg
        ON
          cleg.id_creq_leg=b.id_creq_leg
        LEFT JOIN
          `noonltddwh.lms.creq`
        USING
          (id_creq)
        WHERE
          id_loc_type=6
          AND a.id_status=7)
      USING
        (id_creq)
      LEFT JOIN (
        SELECT
          c.id_creq,
          a.created_at AS Manifest,
          m.manifest_nr
        FROM
          `noonltddwh.lms.item_state_history` a
        LEFT JOIN
          `noonltddwh.lms.item_pkg` b
        USING
          (id_item)
        LEFT JOIN
          `noonltddwh.lms.creq_leg` cleg
        ON
          cleg.id_creq_leg=b.id_creq_leg
        LEFT JOIN
          `noonltddwh.lms.creq` c
        USING
          (id_creq)
        LEFT JOIN
          `noonltddwh.lms.manifest` m
        ON
          m.id_manifest = a.loc_id
        WHERE
          id_loc_type=5
          AND m.id_manifest_type = 2 )
      USING
        (id_creq)
      LEFT JOIN (
        SELECT
          DISTINCT return_nr,
          wrml2.created_at AS Refund,
          wrjl.reason_code
        FROM
          `noonltddwh.sales.sales_order_item` soi
        LEFT JOIN
          `noonltddwh.sales.sales_order` so
        USING
          (id_sales_order)
        LEFT JOIN
          `noonltddwh.sales.sales_order_item_shipment` sois
        USING
          (id_sales_order_item_shipment)
        LEFT JOIN
          `noonltddwh.oms.sales_item` si
        ON
          soi.item_nr = si.sales_item_nr
        LEFT JOIN
          `noonltddwh.oms.purchase_item` pi
        USING
          (id_sales_item)
        LEFT JOIN
          `noonltddwh.oms.wms_return_job_line` wrjl
        USING
          (id_purchase_item)
        LEFT JOIN
          `noonltddwh.oms.wms_return_job` wrj
        USING
          (id_wms_return_job)
        LEFT JOIN
          `noonltddwh.oms.wms_return_manifest` wrm
        USING
          (id_wms_return_manifest)
        LEFT JOIN
          `noonltddwh.oms.wms_return_manifest_received_line` wrml
        ON
          wrml.id_wms_return_manifest=wrm.id_wms_return_manifest
        LEFT JOIN
          `noonltddwh.sales_return.sales_return_receipt_item` srri
        ON
          srri.id_sales_order_item=soi.id_sales_order_item
        LEFT JOIN
          `noonltddwh.ref.carrier` cr
        ON
          cr.id_carrier = wrm.id_carrier
        LEFT JOIN
          `noonltddwh.sales_return.sales_return_request_item` srrq
        ON
          srrq.id_sales_order_item=soi.id_sales_order_item
        LEFT JOIN
          `noonltddwh.sales_return.sales_return_request` srr
        USING
          (id_sales_return_request)
        LEFT JOIN
          `noonltddwh.oms.wms_return_manifest_received_line` wrml2
        ON
          wrml2.manual_mp_reference_nr = srr.awb_nr
        WHERE
          wrjl.reason_code IS NULL )
      ON
        client_ref =return_nr
      LEFT JOIN (
        SELECT
          creq_nr,
          MIN(FA) AS FirstAttempt
        FROM (
          SELECT
            *
          FROM (
            SELECT
              creq_nr,
              MIN(creq_leg_history.created_at) AS FA
            FROM
              `noonltddwh.lms.creq` creq
            LEFT JOIN
              `noonltddwh.lms.creq_leg`
            USING
              (id_creq)
            LEFT JOIN (
              SELECT
                *,
                json_EXTRACT(DATA,
                  '$.num_attempts')='1' AS attempt
              FROM
                `noonltddwh.lms.creq_leg_history`) creq_leg_history
            USING
              (id_creq_leg)
            WHERE
              attempt=TRUE
            GROUP BY
              1)
          UNION ALL (
            SELECT
              creq_nr,
              MIN(manifest.created_at) AS FA
            FROM
              `noonltddwh.lms.creq` creq
            LEFT JOIN
              `noonltddwh.lms.manifest` AS manifest
            USING
              (id_creq)
            WHERE
              id_manifest_type=1
            GROUP BY
              1) )
        GROUP BY
          1 )
      USING
        (creq_nr)
      LEFT JOIN (
        SELECT
          c.creq_nr,
          MIN(ish.created_at ) pgroup_dt
        FROM
          `noonltddwh.lms.item_state_history` ish
        LEFT JOIN
          `noonltddwh.lms.creq_leg` cl
        USING
          (id_creq_leg)
        LEFT JOIN
          `noonltddwh.lms.creq` c
        USING
          (id_creq)
        WHERE
          ish.id_loc_type = 3
        GROUP BY
          1 ) pgroup
      USING
        (creq_nr)
      WHERE
        DATE(Creation)>=current_date -10
        AND DATE(creation) <= current_Date -3
        AND id_creq_type =2
        AND cz.id_country = 3
        AND id_creq_leg_type = 1
'''


[[authview]]
dag_id = 'Neeraj_Auth_views'
task_id = 'Neeraj_AWB_creation_to_MF'
destination_table = 'noonbilogmis.reporting.AWB_creation_to_MF'
sql = '''
SELECT  i.awb_nr,substr(hs.code,0,6) Hub ,timestamp_add(i.created_at ,interval 2 hour) created_at,
timestamp_add(ish.Manifest_dt ,interval 2 hour) Manifest_dt, wh_type 
FROM 
(
SELECT t1.*,id_creq_leg
FROM (
SELECT id_item,MIN(ish.created_at ) Manifest_dt
FROM `noonltddwh.lms.item_state_history` ish
where ish.id_loc_type = 5
GROUP BY 1
) t1 INNER JOIN `noonltddwh.lms.item_state_history` ish on t1.id_item = ish.id_item and ish.created_at = t1.Manifest_dt
) ish
left join noonltddwh.lms.item i using(id_item)
left join 
(
SELECT substr(pw.display_name,6,100) wh_type,sois.awb_nr awb21
FROM  `noonltddwh.sales.sales_order_item_shipment` sois
left join `noonltddwh.partner.partner_warehouse` pw on sois.id_partner_warehouse_origin = pw.id_partner_warehouse 
)tt on tt.awb21 = i.awb_nr 
LEFT JOIN `noonltddwh.lms.creq_leg` cl using(id_creq_leg)
LEFT JOIN noonltddwh.lms.creq c using(id_creq)
LEFT JOIN `noonltddwh.lms.hub_sector` hs using(id_hub_sector)
left join `noonltddwh.lms.country_zone` cz using(id_country_zone)
WHERE cl.id_creq_leg_type = 3
and c.id_creq_type = 1
and substr(cz.code,0,2) = 'EG'
and date(timestamp_add(i.created_at ,interval 2 hour) ) >= current_date-8
and date(timestamp_add(i.created_at ,interval 2 hour) ) <= current_date-1
'''

[[authview]]
dag_id = 'Neeraj_Auth_views'
task_id = 'Neeraj_LPC_Receive_To_TPL_rawdata_Yesterday_CAI_03_04'
destination_table = 'noonbilogmis.reporting.LPC_Receive_To_TPL_rawdata_Yesterday_CAI_03_04'
sql = '''
WITH d as (
SELECT ish.*,timestamp_add(ish.created_at,interval 2 hour) LPC_rec_tm
,substr(pw.display_name,6,100) WH,i.awb_nr ,substr(hl.code,0,6) HL_Code,id_creq_leg_type,hl.code h_l_code
FROM `noonltddwh.lms.item_state_history` ish
LEFT JOIN noonltddwh.lms.item i using(id_item)
left join noonltddwh.sales.sales_order_item_shipment sois using(awb_nr)
left join `noonltddwh.partner.partner_warehouse` pw ON sois.id_partner_warehouse_origin = pw.id_partner_warehouse
left join noonltddwh.lms.creq_leg cl using(id_creq_leg)
left join noonltddwh.lms.creq c using(id_creq)
left join `noonltddwh.lms.hub_location` hl  on (ish.id_loc_type,ish.loc_id) = (1,id_hub_location)
WHERE id_creq_type = 1 and substr(pw.display_name,6,100) IN ('CAI04','CAI03')
AND DATE(ish.created_at)  = current_Date -1
)
SELECT *
  ,timestamp_diff(Z2_PG_group_tm,Z2_LPC_time,hour) Z2_LPC_PG
  ,timestamp_diff(Z2_ht_tt,Z2_PG_group_tm,hour) Z2_PG_HT
  ,timestamp_diff(Z2_ht_tt,Z2_LPC_time,hour) Z2_LPC_HT

  ,timestamp_diff(Z1_PG_group_tm,Z1_LPC_time,hour) Z1_LPC_PG
  ,timestamp_diff(Z1_ht_tt,Z1_PG_group_tm,hour) Z1_PG_HT
  ,timestamp_diff(Z1_ht_tt,Z1_LPC_time,hour) Z1_LPC_HT


FROM (
SELECT Z2.awb_nr,manifest_nr , Z2.LPC_rec_tm Z2_LPC_time,Z1.LPC_rec_tm Z1_LPC_time,MIN(pg.PG_group_tm) Z2_PG_group_tm, MIN(pg.ht_tt) Z2_ht_tt,MIN(pg1.PG_group_tm) Z1_PG_group_tm,MIN(pg1.ht_tt) Z1_ht_tt
FROM 
(
SELECT Z2.awb_nr, Z2.LPC_rec_tm 
FROM (
SELECT d.*
FROM (
SELECT  awb_nr,MIN(LPC_rec_tm) dt
FROM    d
WHERE id_loc_type = 1 and hl_code = 'CAI-Z2'
GROUP BY 1
) Z2 inner join d on Z2.awb_nr = d.awb_nr and z2.dt = d.LPC_rec_tm
)Z2) Z2
LEFT JOIN
(
SELECT Z1.awb_nr, Z1.LPC_rec_tm
FROM (
SELECT d.*
FROM (
SELECT  awb_nr,MIN(LPC_rec_tm) dt
FROM    d
WHERE id_loc_type = 1 and hl_code = 'CAI-Z1'
GROUP BY 1
) Z1 inner join d on Z1.awb_nr = d.awb_nr and z1.dt = d.LPC_rec_tm
)Z1) Z1 ON Z2.awb_nr = Z1.awb_nr
LEFT JOIN
(
SELECT  PG.awb_nr,timestamp_add(PG.Created_At,interval 2 hour) PG_group_tm,timestamp_add(ht.created_at,interval 2 hour) ht_tt
FROM    d PG
left join `noonbilogoi.LOGKSA_DATA.pgroup` pgroup on pgroup.id_item =  PG.loc_id
LEFT JOIN noonltddwh.lms.item iw on PG.loc_id = iw.id_item
left join noonltddwh.lms.hub_transfer ht on ht.hub_transfer_nr = pgroup.hta_start
WHERE id_loc_type = 3 
) pg  ON pg.awb_nr = Z2.awb_nr and Z2.LPC_rec_tm < pg.PG_group_tm
LEFT JOIN
(
SELECT  PG.awb_nr,timestamp_add(PG.Created_At,interval 2 hour) PG_group_tm,timestamp_add(ht.created_at,interval 2 hour) ht_tt
FROM    d PG
left join `noonbilogoi.LOGKSA_DATA.pgroup` pgroup on pgroup.id_item =  PG.loc_id
LEFT JOIN noonltddwh.lms.item iw on PG.loc_id = iw.id_item
left join noonltddwh.lms.hub_transfer ht on ht.hub_transfer_nr = pgroup.hta_start
WHERE id_loc_type = 3 
) pg1  ON pg1.awb_nr = Z1.awb_nr and Z1.LPC_rec_tm < pg1.PG_group_tm
 LEFT JOIN
  (
  SELECT i.awb_nr,m.manifest_nr 
  FROM 
  (
  SELECT id_item id, MIN(ish.created_at) dt
  FROM `noonltddwh.lms.item_state_history` ish
  where ish.id_loc_type = 5
  GROUP BY 1
  ) t INNER JOIN  `noonltddwh.lms.item_state_history` ish on t.id = ish.id_item and t.dt = ish.created_at 
  left join noonltddwh.lms.item i using(id_item)
  left join `noonltddwh.lms.manifest` m on m.id_manifest = ish.loc_id
  where id_loc_type = 5
  ) manifest on Z2.awb_nr = manifest.awb_nr
  LEFT JOIN
 ( 
  SELECT  awb_nr
FROM    d
WHERE  h_l_code LIKE "%TPL%"
    AND id_loc_type = 1
  ) tpl on tpl.awb_nr = Z2.awb_nr

  LEFT JOIN
 ( 
  SELECT  awb_nr
FROM    d
WHERE  hl_code NOT IN ('CAI-Z1','CAI-Z2')  and hl_code is not null
    AND id_loc_type = 1
  ) tpl1 on tpl1.awb_nr = Z2.awb_nr

  
where tpl.awb_nr is not null  and tpl1.awb_nr is null
GROUP BY 1,2,3,4
)
'''


[[matview]]
dag_id = 'NeeraJ_Noon_Daily'
task_id = 'NeeraJ_Noon_Daily_example'
destination_table = 'noonbilogmis.reporting.NeeraJ_Noon_Daily_LMS_pending'
sql = '''
SELECT 
*,case 
     when timestamp_diff(current_date(),date(EDD),day) between 0 and 5 then "EDD-5 days"
     when timestamp_diff(current_date(),date(EDD),day) between 5 and 10 then "EDD-10 dayss"
     when timestamp_diff(current_date(),date(EDD),day)> 10 then  "greater than 10 days"
     end as Ageing
FROM `noonbilogoi.SA.AsahuDateDailyView` 
Where ItemFinalStatus IN ('exported','confirmed','shipped')
'''

[[authview]]
dag_id = 'Neeraj_Auth_views'
task_id = 'Neeraj_LPC_Receive_To_HT_Raw_Yesterday_CAI03_04'
destination_table = 'noonbilogmis.reporting.LPC_Receive_To_HT_Raw_Yesterday_CAI03_04'
sql = '''
WITH d as (
SELECT ish.*,timestamp_add(ish.created_at,interval 2 hour) LPC_rec_tm
,substr(pw.display_name,6,100) WH,i.awb_nr ,substr(hl.code,0,6) HL_Code,id_creq_leg_type
FROM `noonltddwh.lms.item_state_history` ish
LEFT JOIN noonltddwh.lms.item i using(id_item)
left join noonltddwh.sales.sales_order_item_shipment sois using(awb_nr)
left join `noonltddwh.partner.partner_warehouse` pw ON sois.id_partner_warehouse_origin = pw.id_partner_warehouse
left join noonltddwh.lms.creq_leg cl using(id_creq_leg)
left join noonltddwh.lms.creq c using(id_creq)
left join `noonltddwh.lms.hub_location` hl  on (ish.id_loc_type,ish.loc_id) = (1,id_hub_location)
WHERE id_creq_type = 1 and substr(pw.display_name,6,100) IN ('CAI04','CAI03')
AND DATE(ish.created_at)  = current_Date -1
)
SELECT *
  ,timestamp_diff(Z2_PG_group_tm,Z2_LPC_time,hour) Z2_LPC_PG
  ,timestamp_diff(Z2_ht_tt,Z2_PG_group_tm,hour) Z2_PG_HT
  ,timestamp_diff(Z2_ht_tt,Z2_LPC_time,hour) Z2_LPC_HT

  ,timestamp_diff(Z1_PG_group_tm,Z1_LPC_time,hour) Z1_LPC_PG
  ,timestamp_diff(Z1_ht_tt,Z1_PG_group_tm,hour) Z1_PG_HT
  ,timestamp_diff(Z1_ht_tt,Z1_LPC_time,hour) Z1_LPC_HT


FROM (
SELECT Z2.awb_nr,manifest_nr , Z2.LPC_rec_tm Z2_LPC_time,Z1.LPC_rec_tm Z1_LPC_time,MIN(pg.PG_group_tm) Z2_PG_group_tm, MIN(pg.ht_tt) Z2_ht_tt,MIN(pg1.PG_group_tm) Z1_PG_group_tm,MIN(pg1.ht_tt) Z1_ht_tt
FROM 
(
SELECT Z2.awb_nr, Z2.LPC_rec_tm 
FROM (
SELECT d.*
FROM (
SELECT  awb_nr,MIN(LPC_rec_tm) dt
FROM    d
WHERE id_loc_type = 1 and hl_code = 'CAI-Z2'
GROUP BY 1
) Z2 inner join d on Z2.awb_nr = d.awb_nr and z2.dt = d.LPC_rec_tm
)Z2) Z2
LEFT JOIN
(
SELECT Z1.awb_nr, Z1.LPC_rec_tm
FROM (
SELECT d.*
FROM (
SELECT  awb_nr,MIN(LPC_rec_tm) dt
FROM    d
WHERE id_loc_type = 1 and hl_code = 'CAI-Z1'
GROUP BY 1
) Z1 inner join d on Z1.awb_nr = d.awb_nr and z1.dt = d.LPC_rec_tm
)Z1) Z1 ON Z2.awb_nr = Z1.awb_nr
LEFT JOIN
(
SELECT  PG.awb_nr,timestamp_add(PG.Created_At,interval 2 hour) PG_group_tm,timestamp_add(ht.created_at,interval 2 hour) ht_tt
FROM    d PG
left join `noonbilogoi.LOGKSA_DATA.pgroup` pgroup on pgroup.id_item =  PG.loc_id
LEFT JOIN noonltddwh.lms.item iw on PG.loc_id = iw.id_item
left join noonltddwh.lms.hub_transfer ht on ht.hub_transfer_nr = pgroup.hta_start
WHERE id_loc_type = 3 
) pg  ON pg.awb_nr = Z2.awb_nr and Z2.LPC_rec_tm < pg.PG_group_tm
LEFT JOIN
(
SELECT  PG.awb_nr,timestamp_add(PG.Created_At,interval 2 hour) PG_group_tm,timestamp_add(ht.created_at,interval 2 hour) ht_tt
FROM    d PG
left join `noonbilogoi.LOGKSA_DATA.pgroup` pgroup on pgroup.id_item =  PG.loc_id
LEFT JOIN noonltddwh.lms.item iw on PG.loc_id = iw.id_item
left join noonltddwh.lms.hub_transfer ht on ht.hub_transfer_nr = pgroup.hta_start
WHERE id_loc_type = 3 
) pg1  ON pg1.awb_nr = Z1.awb_nr and Z1.LPC_rec_tm < pg1.PG_group_tm
 LEFT JOIN
  (
  SELECT i.awb_nr,m.manifest_nr 
  FROM 
  (
  SELECT id_item id, MIN(ish.created_at) dt
  FROM `noonltddwh.lms.item_state_history` ish
  where ish.id_loc_type = 5
  GROUP BY 1
  ) t INNER JOIN  `noonltddwh.lms.item_state_history` ish on t.id = ish.id_item and t.dt = ish.created_at 
  left join noonltddwh.lms.item i using(id_item)
  left join `noonltddwh.lms.manifest` m on m.id_manifest = ish.loc_id
  where id_loc_type = 5
  ) manifest on Z2.awb_nr = manifest.awb_nr
GROUP BY 1,2,3,4
)
'''



[[matview]]
dag_id = 'NP_CIR_LATEST'
task_id = 'NP_CIR_LATEST_DB'
destination_table = 'noonbilogmis.reporting.NP_CIR_LATEST_DB'
sql = '''
SELECT
client_ref,
num_attempts,
TIMESTAMP_ADD(creq_leg.created_at,INTERVAL 330 MINUTE)as created_at,
TIMESTAMP_ADD(max(latest_updated_at),INTERVAL 330 MINUTE) as updated_at,
TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP_ADD(creq_leg.created_at,INTERVAL 330 MINUTE) ,day) AS aging,
hub_sector.code AS hub_sector,
substring (hub_sector.code,
0,
6) AS hub,
src_address.address_uid AS src_address_uid,
dst_address.address_uid AS dst_address_uid,
reason.code AS held_reason_code,
country.code AS country,
id_reason_held,
job_status,
username,
case when country.code = "sa" and extract(hour from TIMESTAMP_ADD(creq_leg.created_at,INTERVAL 330 MINUTE)) < 10 Then "Created Before Cutoff"
when country.code = "ae" and extract(hour from TIMESTAMP_ADD(creq_leg.created_at,INTERVAL 330 MINUTE)) < 12 Then "Created Before Cutoff"
when country.code = "eg" and extract(hour from TIMESTAMP_ADD(creq_leg.created_at,INTERVAL 330 MINUTE)) < 12 Then "Created Before Cutoff"
else "Created after Cutoff"
end as cut_off,
IF
(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(),creq_leg.created_at,day)<6,
CAST(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(),creq_leg.created_at,day) AS string),
CONCAT("`",(FLOOR(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(),creq_leg.created_at,day)/5))*5,"-",(FLOOR(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(),creq_leg.created_at,day)/5)+1)*5)) AS aging_group,
IF
(num_attempts>=3
OR id_reason_held IN (2,
18,
19),
"calling_closure_required",
IF
(hub_sector.code LIKE "%T1%",
"TPL",
IF
(num_attempts=0 AND reason.code not in ('address_changed' , 'address_misrouted'),
"Pending__first_attempt",
IF
(reason.code in ('address_changed' , 'address_misrouted'),
"Address Changed/Misrouted",
IF
((num_attempts>0
AND username is null
AND id_reason_held IS NULL)
OR (id_reason_held IN (3)
),
"pending_for_reassigning",
IF
( id_reason_held=1
AND DATE_DIFF(scheduled_date,CURRENT_DATE(),day)<4,
"held_reschedule",
IF
( id_reason_held=1
AND DATE_DIFF(scheduled_date,CURRENT_DATE(),day)>=4,
"Audit",
IF
( id_reason_held=5,
"address_calling_required",
IF
( id_reason_held=4,
"address_check_reassigning_required",
"check"))))))))) AS status
FROM
`noonltddwh.lms.creq_leg` creq_leg
LEFT JOIN
`noonltddwh.lms.hub_sector` hub_sector
USING
(id_hub_sector)
LEFT JOIN
`noonltddwh.lms.creq` creq
USING
(id_creq)
LEFT JOIN
`noonltddwh.lms.creq_type` creq_type
USING
(id_creq_type)
LEFT JOIN
`noonltddwh.lms.address` src_address
ON
id_address_src=src_address.id_address
LEFT JOIN
`noonltddwh.lms.address` dst_address
ON
id_address_dst=dst_address.id_address
LEFT JOIN
`noonltddwh.lms.reason` reason
ON
id_reason_held=reason.id_reason
LEFT JOIN
`noonltddwh.lms.country` country
ON
dst_address.id_country=country.id_country
left join 
(select 
client_ref,
clj.id_creq_leg, 
clj.id_creq_leg_job,
clj.updated_at as latest_updated_at,
user.username,
reason.code as reason,
s.code as job_status
from `noonltddwh.lms.creq_leg_job_active` as  clja
left join `noonltddwh.lms.creq_leg_job` as  clj on (clj.id_creq_leg , clj.id_creq_leg_job)  = (clja.id_creq_leg , clja.id_creq_leg_job)
left join noonltddwh.lms.user as  user on clj.id_user = user.id_user
left join `noonltddwh.lms.status`as s on clj.id_status = s.id_status
left join `noonltddwh.lms.creq_leg` as cl on clj.id_creq_leg = cl.id_creq_leg
left join `noonltddwh.lms.creq` using (id_creq)
left join `noonltddwh.lms.reason` reason on reason.id_reason = clj.id_reason) abc using (client_ref) 
WHERE
id_creq_leg_type=1
AND creq_leg.id_status NOT IN (4,
6)
AND DATE(creq_leg.created_at)>="2022-01-01"
AND id_creq_type=2
group by 1,2,3,5,6,7,8,9,10,11,12,13,14,15,16,17
'''

[[authview]]
dag_id = 'Neeraj_EG'
task_id = 'Neeraj_FAKE_Attempt_Report'
destination_table = 'noonbilogmis.reporting.Fake_Attempt_Report_MTD'
sql = '''
SELECT 
*
FROM 
(SELECT distinct awb_nr,Id_leg, country, client_ref,ofd_date,username,Hub,calls,omw,reachable,unreachable,ud_reason,usr_reason, firstdel_awb, Fake_Attempts, flag, updated_by FROM (SELECT ofd_data.country,ofd_data.awb_nr,client_ref,ofd_date,username,ofd_data.hub Hub,calls,omw ,reachable,unreachable,ofd_status ud_reason ,reason usr_reason,firstdel.awb_nr firstdel_awb,Id_leg, dd,clh_data as updated_by,CASE WHEN ofd_data.ofd_status IN ("Delivered","rto_marked","unrechable") THEN "No Fake Attempt"
WHEN calls IS NULL AND reason IS NOT NULL THEN "UD without Call"
WHEN calls = 0 AND reason IS NOT NULL THEN "UD without Call"
WHEN reachable >= 1 AND ofd_status IS NULL THEN "No UD Reachable Call"
WHEN reason IN ("address_misrouted","address_changed_warning","address_changed_reroute","address_changed")
and del_dt < ofd_data.ofd_date
AND ofd_data.awb_nr <> firstdel.awb_nr THEN "Address Change Already Delivered"
WHEN usr_created_at < IVR_Call AND DATE(usr_created_at) = DATE(IVR_call) THEN "Calling Validated"
WHEN ofd_status IS NULL AND IVR_Call IS NOT NULL THEN 'Calling Validated' ELSE "No Fake Attempt"End as Fake_Attempts,
ROW_NUMBER() OVER (PARTITION BY ofd_data.awb_nr,ofd_data.ofd_date ORDER BY ofd_data.ofd_date DESC) row_num,case when lower(json_extract(data,"$.updated_app")) like "%field%" then 'field'
     when lower(json_extract(data,"$.updated_app")) is null then 'null'
     when lower(json_extract(data,"$.updated_app")) like "%client%" then 'CX'
     when lower(json_extract(data,"$.updated_app")) like "noon@client.express.noon.team" then 'CS'
     else 'others'
end as flag FROM (
select * except (created_at), cast(created_at as timestamp) as session_field_item_created_at FROM
( SELECT ofd.* from `noonbilogoi.MJ.OFD` ofd
left join `noonltddwh.lms.creq_leg` using (id_creq_leg) 
left join `noonltddwh.lms.creq` creq using (id_creq) 
left join `noonltddwh.lms.client` client using (id_client)
left join (SELECT it.awb_nr,sfi.is_delivery_block,demo.id_creq_leg_type clt, sfi.id_address,
    cust.code customer_type, sf.created_at, id_session_field FROM `noonltddwh.lms.session_field` sf
LEFT JOIN `noonltddwh.lms.session_field_item` sfi USING (id_session_field)
LEFT JOIN `noonltddwh.lms.item` it USING (id_item)
LEFT JOIN `noonltddwh.lms.address` addr USING (id_address)
LEFT JOIN `noonltddwh.lms.customer` cus USING (id_customer)
LEFT JOIN `noonltddwh.lms.customer_type` cust USING (id_customer_type)
left join `noonltddwh.lms.creq_leg`  demo on demo.id_creq_leg = sfi.id_creq_leg 
)  a using  (id_session_field,awb_nr)
left join (select username, lms_roles from `noonltddwh.lms.user` 
left join `noonltddwh.lms.user_misc` using (id_user) ) using (username) 
where id_creq_type =1
and clt = 3
and is_delivery_block =0
)
left join (select awb_nr,client_ref,country,creq,creq_leg_type from `noonbilogoi.battleplan_v1.item_final_state`) using (awb_nr)
left join
(
SELECT sfi.*,
MIN(temp.id_leg) Id_leg
FROM `noonltddwh.lms.session_field_item` sfi
LEFT JOIN (
SELECT id_creq_leg id_leg, created_at ct_at from 
`noonltddwh.lms.creq_leg_history` clh
WHERE json_extract(data,"$.updated_app") = '"client"'
) temp on sfi.id_creq_leg = temp.id_leg AND sfi.created_at >= temp.ct_at AND Date(sfi.created_at ) = date ( temp.ct_at)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11

)
using (id_item,id_session_field)

where creq = "LM"  and is_delivery_block = 0
) ofd_data
left join(select awb_nr,timestamp_add(clh.created_at,interval 4 hour) as usr_created_at, data as clh_data from `noonbilogoi.MJ.OFD`
left join `noonltddwh.lms.creq_leg_history` clh using (id_creq_leg)
-- where lower(json_extract(data,"$.updated_app")) like "%field%"
) using (awb_nr)left join `noonltddwh.lms.item` item using (id_item,awb_nr)
left join `noonltddwh.lms.item_pkg` ip using (id_item)
left join `noonltddwh.lms.creq_leg_history` clh on (clh.id_creq_leg = ip.id_creq_leg)
LEFT JOIN (
SELECT awb_nr1, cast(Received_by_LPC as timestamp) Received_by_LPC
FROM(
SELECT awb_nr1,
CASE
WHEN SUBSTR(awb_nr1,14,1) = "E" THEN timestamp_add(Received_by_LPC,interval 2 hour)
WHEN SUBSTR(awb_nr1,14,1) = "S" THEN timestamp_add(Received_by_LPC,interval 3 hour)
WHEN SUBSTR(awb_nr1,14,1) = "A" THEN timestamp_add(Received_by_LPC,interval 4 hour)
ELSE
timestamp_add(Received_by_LPC,interval 4 hour)
END as Received_by_LPC
FROM (
select awb_nr awb_nr1,min(ish.created_at) as Received_by_LPC
from `noonltddwh.lms.item_state_history` ish
left join `noonltddwh.lms.hub_location` hl on (ish.id_loc_type,ish.loc_id) = (1,id_hub_location)
left join `noonltddwh.lms.item` using (id_item)
where id_loc_type = 1
and substr(CAST(substr(hl.code,0,6) AS String),5,1)
not in ("G","Z","Y","D","T")
group by 1))
) t on t.awb_nr1 = ofd_data.awb_nr
-- and t.Received_by_LPC < ofd_data.session_field_item_created_at
LEFT JOIN(
SELECT awb_nr, cast(DateTIME(dd) as timestamp) dd
from(
Select awb_nr, max(CASE
WHEN SUBSTR(awb_nr,14,1) = "E" THEN timestamp_add(ish.created_at,interval 2 hour)
WHEN SUBSTR(awb_nr,14,1) = "S" THEN timestamp_add(ish.created_at,interval 3 hour)
WHEN SUBSTR(awb_nr,14,1) = "A" THEN timestamp_add(ish.created_at,interval 4 hour)
ELSE
timestamp_add(ish.created_at,interval 3 hour)
END) dd
From `noonltddwh.lms.item_state_history` ish
LEFT JOIN `noonltddwh.lms.item` i using (id_item)
LEFT JOIN `noonltddwh.lms.creq_leg` cl using (id_creq_leg)
where cl.id_creq_leg_type = 3 group by 1) t
) RTO_Check
ON RTO_Check.awb_nr = ofd_data.awb_nr
-- and RTO_Check.dd > ofd_data.session_field_item_created_at
LEFT JOIN `noonbilogoi.LOGKSA_DATA.location_by_country` lbc on lbc.branch_code = ofd_data.hub
LEFT JOIN (
SELECT awb_nr,
client_ref,
Hub_Sector,
ofd_date del_dt
FROM (
SELECT awb_nr,
client_ref,
ofd_date,
Hub_Sector,
ROW_NUMBER() OVER (PARTITION BY client_ref ORDER BY ofd_date DESC) row_num
FROM noonbilogoi.LOGKSA_DATA.lms2_ofd_data
WHERE Final_Status like "Delivered" AND ofd_date >= DATE_ADD(CURRENT_DATE(),INTERVAL -90 DAY)) WHERE row_num = 1) firstdel using(client_ref,Hub_Sector)
LEFT JOIN (
SELECT Unique_ID,
Cust_Category,
MAX(Date_Time) as IVR_Call FROM `noonbilogoi.nefreelancer.ne_calling_outcom_history`
WHERE lower(Cust_Disposition) like '%reschedule%' and lower(Cust_Category) in ('unreachable','sivvi_customer_cancel','prepaid','attempts_exhausted','kul_customer_cancel','kul_address_change','sivvi_high_value','sivvi_address_change','address_change_hub_scetor','oda','attempt_exhausted','address_change','customer_cancel','oda_not_for_eg_ae','high_value','ndr')
GROUP BY 1,2) calling on Unique_ID = client_ref
WHERE 
 ofd_data.ofd_date > '2021-12-31'
-- extract(month from ofd_data.ofd_date) = extract(month from current_date())
-- AND Received_by_LPC is not null
-- AND RTO_Check.dd is not null
AND
substr(CAST(substr(ofd_data.hub,0,6) AS String),5,1) not in ("G","Z","Y","D","T")
and lower(json_extract(clh_data,"$.updated_app")) like "%field%"
-- and lower(json_extract(clh_data,"$.updated_by")) not like "%customer@noon.com%"-- "updated_by":"customer@noon.com"
)
WHERE Fake_Attempts <> "No Fake Attempt"
-- AND ((country = 'AE' and Hub not like '%-B%') or (country = 'SA' and Hub not like '%-F%') or country = 'EG')
and flag not in ('CS','CX','OFD','null')
and not (calls <> 0 and omw <> 0)
)
WHERE case when Id_leg is not null and Fake_Attempts = 'UD without Call' then 0 else 1 end = 1
and country = 'eg'
'''


[[authview]]
dag_id = 'Neeraj_EG'
task_id = 'Neeraj_Bagging_compliance'
destination_table = 'noonbilogmis.reporting.Bagging_compliance'
sql = '''
select distinct ofd.* , scan_type
,GA_nr
,pgroup_type
,bagging_created_by
,if ( pgroup_type='hub_sector', "DA_BaggeingDone" ,"Direct handover") as BaggingCheck
,if ( pgroup_type='hub_sector', "Yes" ,"No") as bagging_check 
,if( scan_type is not null , "Inventory Done" ,"Inventory Not Done") as inventoryCheck
,if( scan_type is not null , "Yes" ,"No") as inventory_Check 
from (select
 awb_nr,  username, ofd_status,  h.code as hub , OFD_Date,ofd.omw ,ofd.calls 
from `noonbilogmis.reporting.OFD` ofd
left join (select id_item, promised_at , cod_value  from `noonltddwh.lms.item_pkg` ) using (id_item)
left join(select id_creq, id_creq_leg, id_country_zone from   `noonltddwh.lms.creq_leg`) as cl using(id_creq_leg)
left join (select id_country_zone, id_country from  `noonltddwh.lms.country_zone`) as cz using(id_country_zone)
left join (select id_country,code as country from `noonltddwh.lms.country`) as ctr using (id_country)
left join (select id_creq, client_ref ,  id_creq_type from `noonltddwh.lms.creq`) as c using ( id_creq)
left join (select id_creq_type, code as creq_type from  `noonltddwh.lms.creq_type`) as ct using (id_creq_type)
left join (select username, lms_roles from `noonltddwh.lms.user`
left join  `noonltddwh.lms.user_misc` using (id_user) ) using (username)
left join `noonltddwh.lms.session_field` using (id_session_field)
left join `noonltddwh.lms.hub` h using (id_hub)
where creq_type='b2c_delivery' and Date(OFD_Date) >= DATE_TRUNC(CURRENT_DATE()-1, MONTH) and country='eg'
and substr(client_ref,6,1) <> "D") ofd
left join (select baggingx.* from  (select awb_nr, max(GA_date) as GA_date from
(select distinct i.awb_nr,pgroup.awb_nr as GA_nr, date(pgroup.created_at) as GA_date,item_pgroup_type.code as  pgroup_type
from `noonltddwh.lms.item_state_history`  ish
left join `noonltddwh.lms.item` i using (id_item)
left join (select * from `noonltddwh.lms.item` where id_item_type = 2) pgroup on (ish.id_loc_type,ish.loc_id) = (3,pgroup.id_item)
left join (select * from `noonltddwh.lms.item_pgroup` where id_item_pgroup_type in ( 3,7)) ptype on pgroup.id_item = ptype.id_item
left join `noonltddwh.lms.item_pgroup_type` as item_pgroup_type using (id_item_pgroup_type)
where id_item_pgroup_type in (3,7)
)group by 1
)
left join  (select distinct i.awb_nr,pgroup.awb_nr as GA_nr, date(pgroup.created_at) as GA_date,item_pgroup_type.code as  pgroup_type,user.username as bagging_created_by
from `noonltddwh.lms.item_state_history`  ish
left join `noonltddwh.lms.item` i using (id_item)
left join `noonltddwh.lms.user` as user on user.id_user=ish.id_user_updater 
left join (select * from `noonltddwh.lms.item` where id_item_type = 2) pgroup on (ish.id_loc_type,ish.loc_id) = (3,pgroup.id_item)
left join (select * from `noonltddwh.lms.item_pgroup` where id_item_pgroup_type in ( 3,7)) ptype on pgroup.id_item = ptype.id_item
left join `noonltddwh.lms.item_pgroup_type` as item_pgroup_type using (id_item_pgroup_type)
where id_item_pgroup_type in (3,7)) as baggingx using (awb_nr,GA_date)) using (awb_nr)
LEFT JOIN (SELECT DISTINCT scan_line.scanned_code,scan_type.code as scan_type,date(timestamp_add(scan_line.created_at,interval 2 hour)) created_at,user.username FROM `noonltddwh.lms.scan` scan LEFT JOIN `noonltddwh.lms.scan_type` scan_type using(id_scan_type)
LEFT JOIN `noonltddwh.lms.scan_line` scan_line using(id_scan)
LEFT JOIN `noonltddwh.lms.user` user using(id_user)
WHERE scan_type.code like 'inventory') inventory on (inventory.scanned_code,inventory.created_at,inventory.username) = (ofd.awb_nr,ofd.ofd_date,ofd.username)
'''


[[authview]]
dag_id = 'Neeraj_EG'
task_id = 'Neeraj_ASN_EG_data'
destination_table = 'noonbilogmis.reporting.Neeraj_ASN_EG_data'
sql = '''
SELECT * FROM 
(
SELECT *
,case when code = 'closed' and date(updated_at_loc) <= target_date then 0 else 1 end as Breached_flag
FROM (
SELECT c.creq_nr AS ASN ,
c.client_ref AS ASN_nu,
case when cz.id_country =1 then "AE"
     when cz.id_country =2 then "SA"
     when cz.id_country =3 then "EG"
     else "Check"
     end as Country,
     cz.code as Country_zone,
     hs.hub_sector,
     hub.hub,
c.created_at creation,
ss.code as CL_status,
ss1.code as C_status,
case 
when cz.id_country = 1 then timestamp_add(c.created_at,interval 4 hour) 
when cz.id_country = 2 then timestamp_add(c.created_at,interval 3 hour) 
when cz.id_country = 3 then timestamp_add(c.created_at,interval 2 hour) END AS created_at_loc 
,ofp.updated_at
,case 
when cz.id_country = 1 then timestamp_add(ofp.updated_at,interval 4 hour) 
when cz.id_country = 2 then timestamp_add(ofp.updated_at,interval 3 hour) 
when cz.id_country = 3 then timestamp_add(ofp.updated_at,interval 2 hour) END AS updated_at_loc ,
case when cz.id_country = 2
  then IF
    (c.Created_at  <CAST(datetime(DATE(current_date-1),
          "10:00:00") AS timestamp),
      DATE(current_date -1),
      DATE(DATE_ADD(current_timestamp,INTERVAL 1 day)))
  when cz.id_country = 1
  then IF
    (c.Created_at<CAST(datetime(DATE(current_date -1),
          "12:00:00") AS timestamp),
      DATE(current_date-1),
      DATE(DATE_ADD(current_timestamp,INTERVAL 1 day)))
  when cz.id_country = 3
  then IF
    (c.Created_at<CAST(datetime(DATE(current_date -1),
          "10:00:00") AS timestamp),
      DATE(current_date-1),
      DATE(DATE_ADD(current_timestamp,INTERVAL 1 day))) else null end AS target_date
, case when ofp.code is null then 'opened' else ofp.code end as code
FROM `noonltddwh.lms.creq` c 
left join `noonltddwh.lms.status` ss1 on ss1.id_status = c.id_status
LEFT JOIN `noonltddwh.lms.creq_leg` cl using(id_creq)
left join `noonltddwh.lms.status` ss on ss.id_status = cl.id_status
LEFT JOIN `noonltddwh.lms.country_zone` cz using(id_country_zone)
left join (SELECT id_hub_sector,id_hub, code as Hub_sector FROM  `noonltddwh.lms.hub_sector`) hs using (id_hub_sector)
left join(SELECT id_hub, code as Hub FROM  `noonltddwh.lms.hub` ) hub using(id_hub)
LEFT JOIN(
SELECT * FROM (
(
SELECT id_creq_leg,sfi.updated_at ,s.code,row_number() over(partition by id_creq_leg order by sfi.updated_at desc) rw 
FROM (
SELECT id_creq_leg id, DATE(updated_At) dt,MIN(updated_at) l_dt
FROM `noonltddwh.lms.creq_leg_job` sfi
WHERE date(sfi.updated_at) <= current_date()-1
GROUP BY 1,2
) t
INNER JOIN `noonltddwh.lms.creq_leg_job` sfi on (t.id,t.l_dt)=(sfi.id_creq_leg,sfi.updated_At)
LEFT JOIN `noonltddwh.lms.status`  s using(id_status)
) 
)WHERE rw = 1
)ofp on ofp.id_creq_leg = cl.id_creq_leg
WHERE c.id_creq_type = 4
and id_creq_leg_type = 1
) 
WHERE 
SUBSTR(ASN_nu,0,1)= 'K'
and Country = 'EG'
and hub_sector  NOT LIKE '%DROPOFF%'
)WHERE SUBSTR(ASN_nu,0,1) <> 'P'
'''

[[authview]]
dag_id = 'Neeraj_EG'
task_id = 'Neeraj_EG_MTD_consolidation_adherence_rawdata'
destination_table = 'noonbilogmis.reporting.Neeraj_EG_MTD_consolidation_adherence_rawdata'
sql = '''
Select 
country_code, 
final.customer_code, 
Hub,
awb_nr,
delivered_date,
case when time_diff<=1 and b.awb_nr is not null then "delivered_together" else "not" end as Flag
From (Select country_code,customer_code,Hub,delivered_date,min_ts,max_ts, timestamp_diff(max_ts,min_ts,hour) as time_diff 
From ( select distinct hub.code as Hub, 
so.customer_code , so.country_code, date(min_delivered_at) as delivered_date, 
count(distinct awb_nr) as shipments, min(min_delivered_at) as min_ts, max(min_delivered_at) as max_ts, 
from `noonltddwh.sales.sales_order_item` soi 
left join `noonltddwh.sales.sales_order` so on so.id_sales_order =soi.id_sales_order 
left join `noonltddwh.sales.sales_order_item_shipment` sois on soi.id_sales_order_item_shipment =sois.id_sales_order_item_shipment 
left join `noonltddwh.lms.client_pkg_ref` client_pkg_ref using (awb_nr) 
left join (select * from `noonltddwh.lms.creq_leg` where id_creq_leg_type=3) as creq_leg using (id_creq) 
left join `noonltddwh.lms.hub_sector` hub_sector on hub_sector.id_hub_sector=creq_leg.id_hub_sector 
left join `noonltddwh.lms.hub` hub on hub_sector.id_hub=hub.id_hub 
left join (SELECT * FROM `noonltddwh.cache.sales_order_item_summary` 
WHERE sales_order_pdate >= date_trunc(current_date(), month)) soiss on soi.id_sales_order_item =soiss.id_sales_order_item 
where id_cart_type =1
and id_invoice_section =1 
and sois.id_carrier =9 
and soi.id_sales_order_item_status =6 
and extract(year from min_delivered_at)=extract(year from current_date())
group by 1,2,3,4 )) as final 
left join ( select distinct so.customer_code , awb_nr , min_delivered_at as delivered_ts 
from `noonltddwh.sales.sales_order_item` soi 
left join `noonltddwh.sales.sales_order` so on so.id_sales_order =soi.id_sales_order 
left join `noonltddwh.sales.sales_order_item_shipment` sois on soi.id_sales_order_item_shipment =sois.id_sales_order_item_shipment 
left join (SELECT * FROM `noonltddwh.cache.sales_order_item_summary` 
WHERE sales_order_pdate >= date_trunc(current_date(), month) ) soiss on soi.id_sales_order_item =soiss.id_sales_order_item 
where id_cart_type =1 
and id_invoice_section =1 
and sois.id_carrier =9 
and so.id_mp <>6
and soi.id_sales_order_item_status =6 ) b on (final.customer_code,delivered_date)=(b.customer_code,date(b.delivered_ts)) 
where extract(year from delivered_date)=extract(year from current_date()) 
and date(delivered_date)>= DATE_TRUNC(CURRENT_DATE() -1, MONTH) and  date(delivered_date)< current_date()
and country_code = 'EG'
group by 1,2,3,4,5,6
'''

[[authview]]
dag_id = 'Neeraj_EG'
task_id = 'Neeraj_xms_sourcing_ship_box'
destination_table = 'noonbilogmis.reporting.Neeraj_xms_sourcing_ship_box'
sql = '''
SELECT * FROM `noonltddwh.xms_sourcing.ship_box` 
'''

[[authview]]
dag_id = 'Neeraj_EG'
task_id = 'Neeraj_Noon_daily_RTO_pgroup_body'
destination_table = 'noonbilogmis.reporting.Neeraj_Noon_daily_RTO_pgroup_body'
sql = '''
Select Country,City,
Count ( Distinct case when Ageing ='EDD 1 -5 days' then awb_nr end) as Count_of_AWBs_EDD_1_to_5_Days,
Count (Distinct case when Ageing ='EDD 5- 10 days' then awb_nr end) as Count_of_AWBs_EDD_5_to_10_Days,
Count (Distinct case when Ageing ='greater than 10 days' then awb_nr end) as Count_of_AWBs_EDD_Greater_than_10_days
from
(
SELECT daily.*,
case
when timestamp_diff(current_date(),date(EDD),day) between 0 and 5 then "EDD 1 -5 days"
when timestamp_diff(current_date(),date(EDD),day) between 5 and 10 then "EDD 5- 10 days"
when timestamp_diff(current_date(),date(EDD),day)> 10 then "greater than 10 days"
end as Ageing,
ifs.creq creq,
ifs.creq_leg_type creq_leg_type,
loc_type loc_type,
held_reason held_reason,
Final_state,
Shipment_Status
FROM
(
SELECT Country, City, Hub, HubSector, CountryZone, order_nr, DA, BoxNr, awb_nr, shipment_nr, item_nr, OrderTS, EDD, CurrentLocation, LocationType, SlotTimeGST, SDDFlag, CLEG_ID_ADDRESS, ItemFinalStatus, First_held_reason, PaymentMethod, ProductName, barcodes, warehouseCode, OriginWH, ShipmentCreatedAt, ShipmentWHHandoverAt, ShipmentWHReceivedAt, ShipmentInTransitAt, ShipmentInToteAt, BoxCreatedAt, OFDAt, DeliveredAt, firstheld_TS, Instant_order, DSA_name
FROM `noonbilogoi.SA.ShipmentTracker`
WHERE awb_nr Is not null
UNION ALL
SELECT Country, City, Hub, HubSector, CountryZone, order_nr, DA, BoxNr, awb_nr, shipment_nr, item_nr, OrderTS, EDD, CurrentLocation, LocationType, SlotTimeGST, SDDFlag, CLEG_ID_ADDRESS, ItemFinalStatus, First_held_reason, PaymentMethod, ProductName, barcodes, warehouseCode, OriginWH, ShipmentCreatedAt, ShipmentWHHandoverAt, ShipmentWHReceivedAt, ShipmentInTransitAt, ShipmentInToteAt, BoxCreatedAt, OFDAt, DeliveredAt, firstheld_TS, Instant_order, DSA_name
FROM `noonbilogoi.SA.ST3`
left join(SeLECT item_nr as inr FROM `noonbilogoi.SA.ShipmentTracker` ) on item_nr=inr
WHERE inr is null
)daily
left join (SELECT awb_nr , creq ,creq_leg_type,loc_type loc_type,held_reason held_reason,current_location Final_state, shipment_status Shipment_Status FROM `noonbilogoi.battleplan_v1.item_final_state` )ifs on ifs.awb_nr = daily.awb_nr
WHERE date(EDD) < current_date() and loc_type= 'pgroup' and creq_leg_type='RTO'
)
group by Country,City
order by country
'''
[[authview]]
dag_id = 'Neeraj_EG'
task_id = 'Neeraj_Noon_daily_RTO_pgroup_rawdata'
destination_table = 'noonbilogmis.reporting.Neeraj_Noon_daily_RTO_pgroup_rawdata'
sql = '''
SELECT daily.*,
case
when timestamp_diff(current_date(),date(EDD),day) between 0 and 5 then "EDD 1 -5 days"
when timestamp_diff(current_date(),date(EDD),day) between 5 and 10 then "EDD 5- 10 days"
when timestamp_diff(current_date(),date(EDD),day)> 10 then "greater than 10 days"
end as Ageing,
ifs.creq creq,
ifs.creq_leg_type creq_leg_type,
loc_type loc_type,
held_reason held_reason,
Final_state,
Shipment_Status
FROM
(
SELECT Country, City, Hub, HubSector, CountryZone, order_nr, DA, BoxNr, awb_nr, shipment_nr, item_nr, OrderTS, EDD, CurrentLocation, LocationType, SlotTimeGST, SDDFlag, CLEG_ID_ADDRESS, ItemFinalStatus, First_held_reason, PaymentMethod, ProductName, barcodes, warehouseCode, OriginWH, ShipmentCreatedAt, ShipmentWHHandoverAt, ShipmentWHReceivedAt, ShipmentInTransitAt, ShipmentInToteAt, BoxCreatedAt, OFDAt, DeliveredAt, firstheld_TS, Instant_order, DSA_name
FROM `noonbilogoi.SA.ShipmentTracker`
WHERE awb_nr Is not null
UNION ALL
SELECT Country, City, Hub, HubSector, CountryZone, order_nr, DA, BoxNr, awb_nr, shipment_nr, item_nr, OrderTS, EDD, CurrentLocation, LocationType, SlotTimeGST, SDDFlag, CLEG_ID_ADDRESS, ItemFinalStatus, First_held_reason, PaymentMethod, ProductName, barcodes, warehouseCode, OriginWH, ShipmentCreatedAt, ShipmentWHHandoverAt, ShipmentWHReceivedAt, ShipmentInTransitAt, ShipmentInToteAt, BoxCreatedAt, OFDAt, DeliveredAt, firstheld_TS, Instant_order, DSA_name
FROM `noonbilogoi.SA.ST3`
left join(SeLECT item_nr as inr FROM `noonbilogoi.SA.ShipmentTracker` ) on item_nr=inr
WHERE inr is null
)daily
left join (SELECT awb_nr , creq ,creq_leg_type,loc_type loc_type,held_reason held_reason,current_location Final_state, shipment_status Shipment_Status FROM `noonbilogoi.battleplan_v1.item_final_state` )ifs on ifs.awb_nr = daily.awb_nr
WHERE date(EDD) < current_date() and loc_type= 'pgroup' and creq_leg_type='RTO'
'''


[[authview]]
dag_id = 'Neeraj_EG'
task_id = 'first_Mile_asn_pickup'
destination_table = 'noonbilogmis.reporting.first_Mile_asn_pickup'
sql = '''
SELECT * FROM `noonbilogmis.firstmile.asn_pickup`
'''

[[authview]]
dag_id = 'Neeraj_EG'
task_id = 'first_Mile_business_pickup'
destination_table = 'noonbilogmis.reporting.first_Mile_business_pickup'
sql = '''
SELECT * FROM `noonbilogmis.firstmile.business_pickup`
'''

[[authview]]
dag_id = 'Neeraj_EG'
task_id = 'instant_instant_order_sales_order'
destination_table = 'noonbilogmis.reporting.instant_instant_order_sales_order'
sql = '''
SELECT * FROM `noonltddwh.instant_instant_order.sales_order`
'''


[[authview]]
dag_id = 'Neeraj_EG'
task_id = 'instant_instant_order_sales_order_item'
destination_table = 'noonbilogmis.reporting.instant_instant_order_sales_order_item'
sql = '''
SELECT * FROM `noonltddwh.instant_instant_order.sales_order_item`  
'''


[[authview]]
dag_id = 'Neeraj_EG'
task_id = 'neeraj_cir_pending'
destination_table = 'noonbilogmis.reporting.pending_ofp'
sql = '''
SELECT DISTINCT * FROM `noonbilogoi.LOGKSA_DATA.cir_pending` a
LEFT JOIN `noonbilogoi.LOGKSA_DATA.location_by_country` b ON a.Hub =b.branch_code
where Final_Status <> 'Inscanned at CFC' and Hub is not null AND b.country LIKE 'KSA'
'''

[[authview]]
dag_id = 'Neeraj_EG'
task_id = 'LogKSA_DATA_pgroup'
destination_table = 'noonbilogmis.reporting.LogKSA_DATA_pgroup'
sql = '''
SELECT * FROM `noonbilogoi.LOGKSA_DATA.pgroup` 
'''

[[authview]]
dag_id = 'Neeraj_EG'
task_id = 'NP_Pending_Shipments_at_Hubs_v2'
destination_table = 'noonbilogmis.reporting.Pending_Shipments_at_Hubs_v2'
sql = '''
SELECT *
FROM (
SELECT hu.awb_nr  , DATE(promised_at) PDD,date_diff(current_date,DATE(hub_Receiving),day)aging_day,  CASE WHEN current_location IS NULL THEN   substr(Hub_Sector,0,6) ELSE current_location END AS   hub,Hub_Sector,CASE WHEN pkg_value > 2000 THEN 'high_value' ELSE 'low_value' END AS Value_Flag
,CASE WHEN OFD.awb IS NOT NULL THEN 'Out For Delivery' ELSE final_status END AS final_status, OFD.calls, omw,reason OFD_Status, 
CASE WHEN final_status = 'Held at Branch' THEN hu.held_reason END AS held_reason,Attempt_Count
,timestamp_diff(current_timestamp,LastAttempt_Date,Hour) Aging_from_Attempt
,timestamp_diff(current_timestamp,hub_Receiving,Hour) Aging_from_receiving
FROM `noonbilogmis.reporting.EG_Pending_Data`  hu 
left join
        noonltddwh.lms.item i using(awb_nr)
        LEFT JOIN
        (
select id_item, MIN(ish.created_at) as hub_Receiving 
from `noonltddwh.lms.item_state_history` ish
left join `noonltddwh.lms.hub_location`  hl  on (ish.id_loc_type,ish.loc_id) = (1,id_hub_location) 
where id_loc_type = 1 and hl.code Not like "%Z1%" and hl.code Not like "%W2%"  group by 1
)  LPC ON LPC.id_item = i.id_item
LEFT JOIN
(SELECT awb_nr awb,calls,omw,reason FROM 
noonbilogmis.reporting.OFD where OFD_Date = current_Date) OFD 
ON hu.awb_nr = OFD.awb 
and carrier_change = 'No') T
WHERE final_status NOT IN ('InTransit','Lost Manifest','RTO Pending for Handover','Shipped','Delivered','Returned_to_CFC')
'''


[[authview]]
dag_id = 'Neeraj_EG'
task_id = 'lms_daily_tote'
destination_table = 'noonbilogmis.reporting.lms_daily_tote'
sql = '''
SELECT * FROM `noonltddwh.lms.daily_tote` 
'''
[[authview]]
dag_id = 'Neeraj_EG'
task_id = 'lms_storage_type'
destination_table = 'noonbilogmis.reporting.lms_storage_type'
sql = '''
SELECT * FROM `noonltddwh.lms.storage_type` 
'''


[[matview]]
dag_id = 'logmis1'
task_id = 'POD_URL'
destination_table = 'noonbilogmis.reporting.POD_RTV'
sql = '''
select DISTINCT
item.awb_nr ,
mt.name AS Media_type,
concat("https://storage.cloud.google", substr(media_url, 27, 1000)) as POD  
from  `noonltddwh.lms.item` item
left join `noonltddwh.lms.item_pkg` using (id_item)
left join `noonltddwh.lms.creq_leg` creq_leg using (id_creq_leg)
left join `noonltddwh.lms.country_zone` c using(id_country_zone)
left join `noonltddwh.lms.creq` creq using (id_creq)
left join (SELECT DISTINCT manifest_item.id_item, manifest.created_at AS Delivery_date,manifest.id_manifest  
from noonltddwh.lms.manifest_item manifest_item 
left join `noonltddwh.lms.manifest` manifest using(id_manifest)
where manifest_item.is_outbound = 1  and manifest.id_manifest_type= 4) ma on ma.id_item = item.id_item
left join `noonltddwh.lms.manifest_media` mm using (id_manifest)
left join `noonltddwh.lms.media_type` mt using(id_media_type)
where id_creq_type = 3
and concat("https://storage.cloud.google", substr(media_url, 27, 1000)) is not null
'''


[[authview]]
dag_id = 'logmis'
task_id = 'Attempt_check_rtv'
destination_table = 'noonbilogmis.reporting.attempt_check_poa'
sql = '''
SELECT * FROM 
(
SELECT
rtv.Country,
rtv.Hub,
rtv.awb_nr awb_nr ,
rtv.client_ref,
T.POA_Cnt POA_Cnt,
L.OFD_Ct OFD_cnt
FROM `noonbilogmis.reporting.rtv_final` rtv
left join 
(
SELECT DISTINCT  AWB , COUNT(1) POA_Cnt FROM `noonbilogmis.reporting._old1`  x
GROUP BY 1
)T ON T.AWB = rtv.awb_nr 
left join 
(
SELECT distinct  awb_nr,COUNT(1) OFD_Ct
FROM (
SELECT id_item,DATE(ish.created_At) dt , MAX(created_at) ddt
FROM `noonltddwh.lms.item_state_history` ish
where ish.id_loc_type = 2
group by 1,2
) t INNER JOIN `noonltddwh.lms.item_state_history` ish on t.id_item = ish.id_item and t.ddt = ish.created_At
left join noonltddwh.lms.item i on i.id_item = ish.id_item
where ish.id_loc_type = 2 
group by 1
)L on L.awb_nr = rtv.awb_nr 
WHERE Address_Type <> 'Service Point'
and loc_type NOT IN ('Service Point Counter','Service Point Counter Location')
)
'''






[[authview]]
dag_id = 'logmis'
task_id = 'hub_sector_carrier'
destination_table = 'noonbilogmis.reporting.hub_sector_carrier'
sql = '''
SELECT * FROM `noonltddwh.lms.hub_sector_carrier` 
'''

[[authview]]
dag_id = 'logmis'
task_id = 'Pending_Shipments_at_LPC'
destination_table = 'noonbilogmis.reporting.Pending_Shipments_at_LPC'
sql = '''
SELECT DISTINCT *
FROM  `noonbilogoi.nefreelancer.ne_pending_shipments_mat` mt
LEFT JOIN 
(
SELECT distinct sois.awb_nr awb, substr(pw.display_name ,6,100) wh_type
FROM noonltddwh.sales.sales_order_item_shipment sois
LEFT JOIN `noonltddwh.partner.partner_warehouse` pw on sois.id_partner_warehouse_origin = pw.id_partner_warehouse 

) T ON T.awb = mt.awb_nr 
LEFT JOIN
(
SELECT awb_nr awbnr,Username 
FROM `noonbilogoi.LOGKSA_DATA.lms_v2` 
)us on mt.awb_nr = us.awbnr
LEFT JOIN
(
select awb_nr, max(ish.created_at) as Last_LPC_Receiving 
from `noonltddwh.lms.item_state_history` ish
left join `noonltddwh.lms.hub_location`  hl  on (ish.id_loc_type,ish.loc_id) = (1,id_hub_location) 
left join noonltddwh.lms.item i using(id_item)
where id_loc_type = 1 and (hl.code  like "%Z1%" or hl.code  like "%Z2%") group by 1)lpc using(awb_nr)
WHERE (current_location like '%Z1%' OR current_location like '%Z2%') AND country = 'EG'
AND final_status not in ('Returned_to_CFC','Pending for Dispatch','RTO Pending for Handover','Shipped','carrier_switch')
'''


[[authview]]
dag_id = 'AG'
task_id = 'ne_calling_outcome'
destination_table = 'noonbilogmis.reporting.ne_calling_outcome'
sql = '''
select * from `noonbilogoi.nefreelancer.ne_calling_outcom_history` 
'''


[[authview]]
dag_id = 'AG'
task_id = 'warranty_count_3pl'
destination_table = 'noonbilogmis.reporting.warranty_count_3pl'
sql = '''
SELECT 
creation_dt
,client_ref
,reference_nr
,pick_status
,dsp
,Hub
,country
,hub_sector
,CASE WHEN substr(client_ref,0,1) = 'W' 
then 'Warranty' 
else 'CIR' 
END AS Flag
FROM noonbilogmis.reporting.3PL_CIR_ALL_LS
WHERE pick_status = 'opened' 
and country <>'eg'
and DSP='aramex'
'''


[[authview]]
dag_id = 'AG'
task_id = 'warranty_count_3pl_summary'
destination_table = 'noonbilogmis.reporting.warranty_count_3pl_summary'
sql = '''
Select Age_flag
,count(distinct case when Flag='Warranty' then client_ref END) as Warranty_count, 
from 
(SELECT creation_dt,client_ref
,CASE WHEN substr(client_ref,0,1) = 'W' then 'Warranty' 
else 'CIR'
END AS Flag,
CASE WHEN date_diff(current_Date,creation_dt,day) < 3 then '0-2'
WHEN date_diff(current_Date,creation_dt,day) > 2 and date_diff(current_Date,creation_dt,day) < 5 then '2-4'
WHEN date_diff(current_Date,creation_dt,day) > 4 then 'More_then_4'
end as Age_flag
,COUNT(distinct client_ref) cnt
FROM noonbilogmis.reporting.3PL_CIR_ALL_LS
WHERE pick_status = 'opened' and country <>'eg' and DSP='aramex'
GROUP BY 1,2,3,4)
group by 1
order by 1
'''


[[authview]]
dag_id = 'AG'
task_id = 'ag_attempt_capacity_imile'
destination_table = 'noonbilogmis.reporting.ag_attempt_capacity_imile'
sql = '''
SELECT
  handover_d,
  COUNT(DISTINCT
    CASE
      WHEN MAX_attempt IS NULL AND code = 'Delivered' THEN awb_nr
  END
    ) No_Delivered,
  COUNT(DISTINCT
    CASE
      WHEN MAX_attempt IS NULL AND code = 'NDRR' THEN awb_nr
  END
    ) No_NDRR,
  COUNT(DISTINCT
    CASE
      WHEN MAX_attempt IS NULL AND code = 'Pending' THEN awb_nr
  END
    ) No_Pending,
  COUNT(DISTINCT
    CASE
      WHEN MAX_attempt IS NOT NULL AND code = 'Delivered' THEN awb_nr
  END
    ) Yes_Delivered,
  COUNT(DISTINCT
    CASE
      WHEN MAX_attempt IS NOT NULL AND code = 'NDRR' THEN awb_nr
  END
    ) Yes_NDRR,
  COUNT(DISTINCT
    CASE
      WHEN MAX_attempt IS NOT NULL AND code = 'Pending' THEN awb_nr
  END
    ) Yes_Pending,
  COUNT(DISTINCT awb_nr ) total,
  CONCAT(ROUND(COUNT(DISTINCT
        CASE
          WHEN code = 'Delivered' THEN awb_nr
      END
        )*100 / COUNT(DISTINCT awb_nr ),2),"%") Del_per,
        B.Capacity as Attempt_Capacity
FROM
  `noonbilogmis.reporting.3PL_ALL111_LS` A
  Left Join (Select attempt1,count(awb_nr) Capacity from 
(SELECT awb_nr, attempt1
FROM `noonbilogmis.reporting.3PL_ALL111_LS` where country_code = 'SA'
  AND DSP='iMile'
union all
SELECT awb_nr, attempt2
FROM `noonbilogmis.reporting.3PL_ALL111_LS` where country_code = 'SA'
  AND DSP='iMile'
union all
SELECT awb_nr, attempt3
FROM `noonbilogmis.reporting.3PL_ALL111_LS` where country_code = 'SA'
  AND DSP='iMile')
where   attempt1 >=current_date-30
  AND attempt1 < current_Date
group by 1
order by 1) B on A.handover_d=B.attempt1
WHERE
  handover_d >=current_date-30
  AND handover_d < current_Date
  AND country_code = 'SA'
  AND DSP='iMile'
GROUP BY
  1,
 B.capacity
ORDER BY
  1 desc
'''


[[authview]]
dag_id = 'AG'
task_id = 'ag_attempt_capacity_aramex'
destination_table = 'noonbilogmis.reporting.ag_attempt_capacity_aramex'
sql = '''
SELECT
  cast(handover_d as date) as handover_Date ,
  COUNT(DISTINCT
    CASE
      WHEN MAX_attempt IS NULL AND code = 'Delivered' THEN awb_nr
  END
    ) No_Delivered,
  COUNT(DISTINCT
    CASE
      WHEN MAX_attempt IS NULL AND code = 'NDRR' THEN awb_nr
  END
    ) No_NDRR,
  COUNT(DISTINCT
    CASE
      WHEN MAX_attempt IS NULL AND code = 'Pending' THEN awb_nr
  END
    ) No_Pending,
  COUNT(DISTINCT
    CASE
      WHEN MAX_attempt IS NOT NULL AND code = 'Delivered' THEN awb_nr
  END
    ) Yes_Delivered,
  COUNT(DISTINCT
    CASE
      WHEN MAX_attempt IS NOT NULL AND code = 'NDRR' THEN awb_nr
  END
    ) Yes_NDRR,
  COUNT(DISTINCT
    CASE
      WHEN MAX_attempt IS NOT NULL AND code = 'Pending' THEN awb_nr
  END
    ) Yes_Pending,
  COUNT(DISTINCT awb_nr ) total,
  CONCAT(ROUND(COUNT(DISTINCT
        CASE
          WHEN code = 'Delivered' THEN awb_nr
      END
        )*100 / COUNT(DISTINCT awb_nr ),2),"%") Del_per,
        B.Capacity as Attempt_Capacity
FROM
  `noonbilogmis.reporting.3PL_ALL111_LS` A
  Left Join (Select attempt1,count(awb_nr) Capacity from 
(SELECT awb_nr, attempt1
FROM `noonbilogmis.reporting.3PL_ALL111_LS` where country_code = 'SA'
  AND DSP='Aramex'
union all
SELECT awb_nr, attempt2
FROM `noonbilogmis.reporting.3PL_ALL111_LS` where country_code = 'SA'
  AND DSP='Aramex'
union all
SELECT awb_nr, attempt3
FROM `noonbilogmis.reporting.3PL_ALL111_LS` where country_code = 'SA'
  AND DSP='Aramex')
where   attempt1 >=current_date-30
  AND attempt1 < current_Date
group by 1
order by 1) B on A.handover_d=B.attempt1
WHERE
  handover_d >=current_date-30
  AND handover_d < current_Date
  AND country_code = 'SA'
  AND DSP='Aramex'
GROUP BY
  1,
 B.capacity
ORDER BY
  1 desc
'''


[[authview]]
dag_id = 'AG'
task_id = 'ag_attempt_capacity_sp'
destination_table = 'noonbilogmis.reporting.ag_attempt_capacity_sp'
sql = '''
SELECT
  handover_d,
  COUNT(DISTINCT
    CASE
      WHEN MAX_attempt IS NULL AND code = 'Delivered' THEN awb_nr
  END
    ) No_Delivered,
  COUNT(DISTINCT
    CASE
      WHEN MAX_attempt IS NULL AND code = 'NDRR' THEN awb_nr
  END
    ) No_NDRR,
  COUNT(DISTINCT
    CASE
      WHEN MAX_attempt IS NULL AND code = 'Pending' THEN awb_nr
  END
    ) No_Pending,
  COUNT(DISTINCT
    CASE
      WHEN MAX_attempt IS NOT NULL AND code = 'Delivered' THEN awb_nr
  END
    ) Yes_Delivered,
  COUNT(DISTINCT
    CASE
      WHEN MAX_attempt IS NOT NULL AND code = 'NDRR' THEN awb_nr
  END
    ) Yes_NDRR,
  COUNT(DISTINCT
    CASE
      WHEN MAX_attempt IS NOT NULL AND code = 'Pending' THEN awb_nr
  END
    ) Yes_Pending,
  COUNT(DISTINCT awb_nr ) total,
  CONCAT(ROUND(COUNT(DISTINCT
        CASE
          WHEN code = 'Delivered' THEN awb_nr
      END
        )*100 / COUNT(DISTINCT awb_nr ),2),"%") Del_per,
        B.Capacity as Attempt_Capacity
FROM
  `noonbilogmis.reporting.3PL_ALL111_LS` A
  Left Join (Select attempt1,count(awb_nr) Capacity from 
(SELECT awb_nr, attempt1
FROM `noonbilogmis.reporting.3PL_ALL111_LS` where country_code = 'SA'
  AND DSP='SP'
union all
SELECT awb_nr, attempt2
FROM `noonbilogmis.reporting.3PL_ALL111_LS` where country_code = 'SA'
  AND DSP='SP'
union all
SELECT awb_nr, attempt3
FROM `noonbilogmis.reporting.3PL_ALL111_LS` where country_code = 'SA'
  AND DSP='SP')
where   attempt1 >=current_date-30
  AND attempt1 < current_Date
group by 1
order by 1) B on A.handover_d=B.attempt1
WHERE
  handover_d >=current_date-30
  AND handover_d < current_Date
  AND country_code = 'SA'
  AND DSP='SP'
GROUP BY
  1,
 B.capacity
ORDER BY
  1 desc
'''

[[authview]]
dag_id = 'AG'
task_id = 'ag_cir_pending'
destination_table = 'noonbilogmis.reporting.ag_cir_pending'
sql = '''
Select Age_flag, count(distinct case when Flag='CIR' then client_ref END) as CIR_Pending,
from (SELECT creation_dt,client_ref
,CASE WHEN substr(client_ref,0,1) = 'W' then 'Warranty' else 'CIR' END AS Flag,
CASE WHEN date_diff(current_Date,creation_dt,day) < 3 then '0-2'
WHEN date_diff(current_Date,creation_dt,day) > 2 and date_diff(current_Date,creation_dt,day) < 5 then '2-4'
WHEN date_diff(current_Date,creation_dt,day) > 4 then 'More_then_4'
end as Age_flag
,COUNT(distinct client_ref) cnt
FROM noonbilogmis.reporting.3PL_CIR_ALL_LS
WHERE pick_status = 'opened' and country <>'eg'
and DSP = 'aramex'
GROUP BY 1,2,3,4)
group by 1
order by 1
'''


[[authview]]
dag_id = 'AG'
task_id = 'ag_sortation_summary'
destination_table = 'noonbilogmis.reporting.ag_sortation_summary'
sql = '''
Select city
,count(distinct case when code='Pending' then awb_nr END) Pending
, FROM `noonbilogmis.reporting.3PL_ALL111_LS`
where handover_d >= current_date-30
AND handover_d <= current_date  
and country_code='SA' 
and DSP = 'Aramex' and Returned_Flag='No'
group by 1
order by 2 desc
limit 15
'''

[[authview]]
dag_id = 'AG'
task_id = 'ag_sortation_summary_1'
destination_table = 'noonbilogmis.reporting.ag_sortation_summary_1'
sql = '''
Select city
,count(distinct case when code='Pending' then awb_nr END) Pending
, FROM `noonbilogmis.reporting.3PL_ALL111_LS`
where handover_d >= current_date-30
AND handover_d <= current_date  
and country_code='SA' 
and DSP = 'iMile' and Returned_Flag='No'
group by 1
order by 2 desc
limit 15
'''

[[authview]]
dag_id = 'AG'
task_id = 'ag_sortation_summary_2'
destination_table = 'noonbilogmis.reporting.ag_sortation_summary_2'
sql = '''
Select city
,count(distinct case when code='Pending' then awb_nr END) Pending
, FROM `noonbilogmis.reporting.3PL_ALL111_LS`
where handover_d >= current_date-30
AND handover_d <= current_date  
and country_code='SA' 
and DSP = 'SP' and Returned_Flag='No'
group by 1
order by 2 desc
limit 15
'''


[[authview]]
dag_id = 'AG'
task_id = 'ag_sp'
destination_table = 'noonbilogmis.reporting.ag_sp'
sql = '''
Select handover_d,
city,
awb_nr,
edd,
hub_sector ,
case when MAX_attempt is null then "No" Else "Yes" End as Attempted_Flag,
case when edd=current_date+1 and code='Pending' then "Yes" Else "No" End as EDD_tommorow,
case when current_date>edd and code='Pending' then "Yes" Else "No" End as EDD_breached,
case when code='Pending' then "Yes" Else "No" End as Backlog_pending
FROM `noonbilogmis.reporting.3PL_ALL111_LS`
where   handover_d>=current_date-30
  AND handover_d<=current_date
  and country_code = 'SA' and DSP='SP' and code='Pending' and Returned_Flag='No'
group by 1,2,3,4,5,6,7,8,9
order by 1 desc
'''

[[authview]]
dag_id = 'AG'
task_id = 'ag_sp_summ'
destination_table = 'noonbilogmis.reporting.ag_sp_summ'
sql = '''
Select edd, 
count(distinct case when code='Pending' then awb_nr END) Pending,
count(distinct case when code='Pending' and MAX_attempt is null then awb_nr END) Not_Attempted,
count(distinct case when code='Pending' and MAX_attempt=1 then awb_nr END) Second_Attempt_pending,
count(distinct case when code='Pending' and MAX_attempt=2 then awb_nr END) Third_Attempt_pending,
count(distinct case when code='Pending' and MAX_attempt>2 then awb_nr END) Greaterthan_Third

FROM `noonbilogmis.reporting.3PL_ALL111_LS`

where   edd>=current_date-10
  AND edd<=current_date+10 and DSP='SP' and country_code='SA' and Returned_Flag='No'
group by 1
order by 1 desc
'''


[[authview]]
dag_id = 'AG'
task_id = 'ag_imile'
destination_table = 'noonbilogmis.reporting.ag_imile'
sql = '''
Select handover_d,
city,
awb_nr,
edd,
hub_sector ,
case when MAX_attempt is null then "No" Else "Yes" End as Attempted_Flag,
case when edd=current_date+1 and code='Pending' then "Yes" Else "No" End as EDD_tommorow,
case when current_date>edd and code='Pending' then "Yes" Else "No" End as EDD_breached,
case when code='Pending' then "Yes" Else "No" End as Backlog_pending
FROM `noonbilogmis.reporting.3PL_ALL111_LS`
where   handover_d>=current_date-30
  AND handover_d<=current_date
  and country_code = 'SA' and DSP='iMile' and code='Pending' and Returned_Flag='No'
group by 1,2,3,4,5,6,7,8,9
order by 1 desc
'''


[[authview]]
dag_id = 'AG'
task_id = 'ag_imile_summ'
destination_table = 'noonbilogmis.reporting.ag_imile_summ'
sql = '''
Select edd, 
count(distinct case when code='Pending' then awb_nr END) Pending,
count(distinct case when code='Pending' and MAX_attempt is null then awb_nr END) Not_Attempted,
count(distinct case when code='Pending' and MAX_attempt=1 then awb_nr END) Second_Attempt_pending,
count(distinct case when code='Pending' and MAX_attempt=2 then awb_nr END) Third_Attempt_pending,
count(distinct case when code='Pending' and MAX_attempt>2 then awb_nr END) Greaterthan_Third
FROM `noonbilogmis.reporting.3PL_ALL111_LS`
where   edd>=current_date-10
  AND edd<=current_date+10 and DSP='iMile' and country_code='SA' and Returned_Flag='No'
group by 1
order by 1 desc
'''

[[authview]]
dag_id = 'AG'
task_id = 'ag_aramex'
destination_table = 'noonbilogmis.reporting.ag_aramex'
sql = '''
Select handover_d,
city,
awb_nr,
edd,
hub_sector ,
case when MAX_attempt is null then "No" Else "Yes" End as Attempted_Flag,
case when edd=current_date+1 and code='Pending' then "Yes" Else "No" End as EDD_tommorow,
case when current_date>edd and code='Pending' then "Yes" Else "No" End as EDD_breached,
case when code='Pending' then "Yes" Else "No" End as Backlog_pending
FROM `noonbilogmis.reporting.3PL_ALL111_LS`
where   handover_d>=current_date-30
  AND handover_d<=current_date
  and country_code = 'SA' and DSP='Aramex' and code='Pending' and Returned_Flag='No'
group by 1,2,3,4,5,6,7,8,9
order by 1 desc
'''

[[authview]]
dag_id = 'AG'
task_id = 'ag_aramex_summ'
destination_table = 'noonbilogmis.reporting.ag_aramex_summ'
sql = '''
Select edd, 
count(distinct case when code='Pending' then awb_nr END) Pending_Count,
count(distinct case when code='Pending' and MAX_attempt is null then awb_nr END) Not_Attempted,
count(distinct case when code='Pending' and MAX_attempt=1 then awb_nr END) Second_Attempt_pending,
count(distinct case when code='Pending' and MAX_attempt=2 then awb_nr END) Third_Attempt_pending,
count(distinct case when code='Pending' and MAX_attempt>2 then awb_nr END) Greaterthan_Third
FROM `noonbilogmis.reporting.3PL_ALL111_LS`
where   edd>=current_date-10
  AND edd<=current_date+10 and DSP='Aramex' and country_code='SA' and Returned_Flag='No'
group by 1
order by 1 desc
'''


[[matview]]
dag_id = 'AG_3PL'
task_id = 'tpl_handover'
destination_table = 'noonbilogmis.reporting.ag_3pl_handover1'
sql = '''
select distinct ifs.country, ifs.hub_sector, ifs.hub ,ifs.awb_nr, ifs.client_ref , ifs.promised_at as EDD, ifs.current_location ,ifs.carrier, ifs.shipment_creation_TS, ish.created_at as received_at_logistics_TS, h.code as recieved_hub, o.origin_city, 
case when F.created_at is NULL then 'No' else 'Yes' END as Address_Change_Flag,
case when substr(ifs.hub,0,3)=substr(h.code,0,3) then 1 else 0 END as Same_city_Flag,
timestamp_diff(current_date, date(ifs.shipment_creation_TS), DAY) Creation_Ageing,
timestamp_diff(current_date, date(ish.created_at),  Day ) Recieved_Ageing,
timestamp_diff(current_date, date(ish.created_at),  HOUR ) Recieved_Ageing_in_hrs
from
(select id_item, min(ish.created_at) as created_at
from `noonbilogmis.base_table.lms_item`
left join `noonbilogmis.base_table.lms_item_state_history` ish using (id_item)
left join `noonbilogmis.base_table.lms_hub_location` hl on (ish.id_loc_type, ish.loc_id) = (1, hl.id_hub_location)
where ish.id_loc_type = 1
group by 1
) hl1
left join `noonbilogmis.base_table.lms_item_state_history` ish using (id_item, created_at)
left join `noonbilogmis.base_table.lms_hub_location` hl on (ish.id_loc_type, ish.loc_id) = (1, hl.id_hub_location)
left join `noonbilogoi.battleplan_v1.item_final_state` ifs using (id_item)
left join `noonltddwh.lms.hub_location` hln on ish.loc_id  =hln.id_hub_location
left join `noonltddwh.lms.hub` h on h.id_hub =hln.id_hub
left join `noonltddwh.sales.sales_order_item_shipment` E on ifs.awb_nr =E.awb_nr 
left join `noonltddwh.sales.sales_shipment_address_change` F on E.id_sales_order_item_shipment =F.id_sales_order_item_shipment 
left join (select distinct id_city, name_en as origin_city  from `noonltddwh.ref.city` ) as o on id_origin_city=o.id_city 
left join
(select * from noonltddwh.lms.tpl_handover_item A
LEFT JOIN noonltddwh.lms.tpl_item_pgroup_map tpl_item_pgroup_map ON tpl_item_pgroup_map.id_item_pgroup = A.id_item
LEFT JOIN noonltddwh.lms.item tpl_item ON tpl_item_pgroup_map.id_item = tpl_item.id_item
where Date(A.created_at)>current_date-30 and tpl_item.id_item_type=1 and is_outbound=1) A
ON A.awb_nr=ifs.awb_nr
where ish.id_loc_type = 1
and hub like "%T1%"
and is_outbound is NULL
and creq_leg_type = "Delivery"
and shipment_status <> "Delivery"
and date(ish.created_at) > current_date -30
and current_location like '%TPL%'
'''

[[authview]]
dag_id = 'Instant1'
task_id = 'Doc_line'
destination_table = 'noonbilogmis.reporting.ag_doc_line_detail_2021'
sql = '''
Select * from noonbicfcxlb.opsreport.doc_line_detail_2021
'''

[[authview]]
dag_id = 'Instant1'
task_id = 'instant_sales_30'
destination_table = 'noonbilogmis.reporting.ag_instant_sales_30'
sql = '''
Select * from noonbifugc.sales.instant_sales_30
'''

[[authview]]
dag_id = 'Instant1'
task_id = 'xms_sales_ltd'
destination_table = 'noonbilogmis.reporting.ag_xms_sales_ltd'
sql = '''
Select * from noonbifugc.sales.xms_sales_ltd
'''

[[authview]]
dag_id = 'Instant1'
task_id = 'Stock_Detail'
destination_table = 'noonbilogmis.reporting.DS_Stock_Detail'
sql = '''
select * from noonbicfcxlb.opsreport.stock_detail
WHERE AREA IN ('STOCK') AND warehouse like '%DS%'
'''

[[authview]]
dag_id = 'Instant1'
task_id = 'BMR_IB'
destination_table = 'noonbilogmis.reporting.BMR_IB_Daily'
sql = '''
Select * from noonbicfcxlb.opsreport.BMR_IB_Daily
'''


[[matview]]
dag_id = 'Instant_AG'
task_id = 'Dash_Data'
destination_table = 'noonbilogmis.reporting.Instant_Dashboard_AG'
sql = '''
select distinct A.order_nr, Date(A.created_at) Order_Date, A.status_code_order, A.load_factor,D.Item_count,A.order_subtotal, A.da_name, Concat("'",Cast(A.da_phone As String)) DA_Phone,E.DA_Count, 
timestamp_add(CURRENT_TIMESTAMP,interval 4 hour) Refreshed_at
,Round(timestamp_diff(C.logistics_delivered_at , C.created_at, second)/60,2) Actual_Delivery_Time
,Round(timestamp_diff(A.estimated_delivery_at, C.created_at, second)/60,2) Estimate_Delivery_Time
,Round(Round(timestamp_diff(A.original_estimated_delivery_at, C.created_at, second)/60,2)+5,2) OrginalEstimate_Delivery_Time
,Round(timestamp_diff(C.logistics_assigned_at , C.created_at, second)/60,2) Assigned_Time
,Round(timestamp_diff(C.logistics_ready_to_pickup_at, C.created_at, second)/60,2) LogisticsReady_to_PickupTime
,Round(timestamp_diff(C.outlet_ready_for_pickup_at, C.created_at, second)/60,2) OutletReady_to_PickupTime
,Round(timestamp_diff(C.logistics_picked_up_at, C.created_at, second)/60,2) Picked_Up_Time
,Round(timestamp_diff(C.logistics_arrived_delivery_location_at, C.created_at, second)/60,2) Arrived_at_location_time
,Round(timestamp_diff(C.logistics_ready_to_pickup_at , C.logistics_assigned_at , second)/60,2) First_Mile
,Round(timestamp_diff(C.logistics_picked_up_at  , C.logistics_ready_to_pickup_at  , second)/60,2) DA_Wait_Time
,Round(timestamp_diff(C.logistics_arrived_delivery_location_at   , C.logistics_picked_up_at   , second)/60,2) Picked_to_Reached
,Round(timestamp_diff(C.logistics_delivered_at   , C.logistics_picked_up_at   , second)/60,2) Picked_to_Delivered
from noonltddwh.instant_instant_order.sales_order A
left join noonltddwh.scfood_foodlogistics.task B ON A.order_nr=B.mp_task_nr
left join noonltddwh.scfood_foodlogistics.task_timestamp C on B.id_task =C.id_task
left join (select distinct id_sales_order, count(item_nr) Item_count from noonltddwh.instant_instant_order.sales_order_item
group by 1) D on A.id_sales_order =D.id_sales_order 
left join (select distinct Date(created_at) created_at, count(distinct da_phone) DA_Count from noonltddwh.instant_instant_order.sales_order
group by 1) E on E.created_at =Date(A.created_at)
'''

[[matview]]
dag_id = 'AG_DS'
task_id = 'AG_DS_IB'
destination_table = 'noonbilogmis.reporting.AG_DS_IB'
sql = '''
select distinct coalesce(outbound_warehouse,inbound_warehouse,doc_warehouse,oms_warehouse_outbound) as warehouse,
 "Total_order_daily_instant" 
as metric, count(distinct order_nr) as order_count 
from `noonbilogmis.reporting.ag_instant_sales_30` 
left join noonltddwh.sales.sales_order_item soi using(item_nr)
where 
coalesce(outbound_warehouse,inbound_warehouse,doc_warehouse,oms_warehouse_outbound) like "%DS%"
and soi.id_sales_order_item_status not in (7,9)
and id_invoice_section =1
and id_mp=6
and soi.delivery_slot_group_ix =3
and date(order_at) = date_sub(current_date(),interval 1 day)
group by 1,2
 union all  
select distinct coalesce(outbound_warehouse,inbound_warehouse,doc_warehouse,oms_warehouse_outbound) as warehouse,
 "Total_items_daily_instant" 
as metric,
 count(distinct item_nr) as item_count 
from `noonbilogmis.reporting.ag_instant_sales_30` 
left join noonltddwh.sales.sales_order_item soi using(item_nr)
where 
coalesce(outbound_warehouse,inbound_warehouse,doc_warehouse,oms_warehouse_outbound) like "%DS%"
and soi.id_sales_order_item_status not in (7,9)
and id_invoice_section =1
and id_mp=6
and soi.delivery_slot_group_ix =3
and date(order_at) = date_sub(current_date(),interval 1 day)
group by 1,2
 union all 
select distinct warehouse_code,
 "Pure_instant_orders" as metric, 
count(distinct order_nr) as order_count from
(SELECT warehouse_code,
count(distinct warehouse_code) as wh_cnt,
extract(week from order_at)+1 as week,
order_nr from  (SELECT  order_nr,coalesce(outbound_warehouse,inbound_warehouse,doc_warehouse,oms_warehouse_outbound) as warehouse_code,order_at
 FROM noonbilogmis.reporting.ag_instant_sales_30 s
 left join noonltddwh.sales.sales_order_item soi using(item_nr)
where date(order_at) = date_sub(current_date(), interval 1 day)
and id_mp = 6
and soi.delivery_slot_group_ix =3
and id_invoice_section =1
) 
where warehouse_code like "%DS%"
group by 1,3,4
having wh_cnt=1) group by 1,2
 union all
 select  
distinct warehouse_code,
"Pure_instant_items" as metric, 
count(distinct item_nr) as item_count from
(SELECT warehouse_code,
count(distinct warehouse_code) as wh_cnt,
extract(week from order_at)+1 as week,
item_nr from  (SELECT  item_nr,coalesce(outbound_warehouse,inbound_warehouse,doc_warehouse,oms_warehouse_outbound) as warehouse_code,order_at
 FROM noonbilogmis.reporting.ag_instant_sales_30 s
 left join noonltddwh.sales.sales_order_item soi using(item_nr)
where date(order_at)  = date_sub(current_date(), interval 1 day)
and id_mp = 6
and soi.delivery_slot_group_ix =3
--and s.Id_sales_order_item_status not in (7,9)
and id_invoice_section =1
)
where warehouse_code like "%DS%"
group by 1,3,4
having wh_cnt=1) group by 1,2
--   union all
-- select
--  Distinct coalesce(outbound_warehouse,inbound_warehouse,doc_warehouse,oms_warehouse_outbound) as warehouse,
--  "Daily_items_not_instant" 
-- as metric,
--  count(distinct item_nr) as item_count 
-- from `noonbilogmis.reporting.ag_instant_sales_30` 
-- left join noonltddwh.sales.sales_order_item soi using(item_nr)
-- --left join noonltddwh.sales.sales_order using(id_sales_order)
-- where 
-- coalesce(outbound_warehouse,inbound_warehouse,doc_warehouse,oms_warehouse_outbound) like "%DS%"
-- and soi.id_sales_order_item_status not in (7,9)
-- and id_invoice_section =1
-- and id_mp=6
-- and is_instant <>1 
-- and date(order_at) = date_sub(current_date(),interval 1 day)
-- group by 1,2 */
 union all
select distinct warehouse,
"Order_to_export_TAT" as metric,
  percentile_disc(O2E_TAT,0.95) over (partition by warehouse) as P_99_O2E
from 
( select *, timestamp_diff(export_TS,order_at,minute) as O2E_TAT,
from( 
select distinct 
  coalesce(outbound_warehouse,inbound_warehouse,doc_warehouse,oms_warehouse_outbound) as warehouse,
  order_at,
  item_nr,
if(country in ("AE"),timestamp_add(min_exported_at, interval 4 hour),if(country in ("SA"),timestamp_add(min_exported_at, interval 3 hour),if(country in ("EG"),timestamp_add(min_exported_at, interval 2 hour),null))) as export_TS,
from `noonbilogmis.reporting.ag_instant_sales_30` s
left join noonltddwh.sales.sales_order_item soi using(item_nr)
left join   noonltddwh.cache.sales_order_item_summary oi on oi.sales_item_nr=s.item_nr
where 
coalesce(outbound_warehouse,inbound_warehouse,doc_warehouse,oms_warehouse_outbound) like "%DS%"
and s.id_sales_order_item_status not in (7,9)
and soi.delivery_slot_group_ix =3
and id_mp=6
and exported_at is not null
and date(exported_at) = date_sub(current_date(),interval 1 day)
group by 1,2,3,4,country,min_exported_at
))group by 1,2,O2E_TAT,export_TS
 union all
select
distinct  warehouse,
"export_breach" as metric,  
safe_divide(count(distinct If(timestamp_diff(export_TS,order_at,minute)>2,item_nr,null)),count(distinct item_nr)) as export_breach,
from( 
select distinct 
  coalesce(outbound_warehouse,inbound_warehouse,doc_warehouse,oms_warehouse_outbound) as warehouse,
  order_at,
  item_nr,
if(country in ("AE"),timestamp_add(min_exported_at, interval 4 hour),if(country in ("SA"),timestamp_add(min_exported_at, interval 3 hour),if(country in ("EG"),timestamp_add(min_exported_at, interval 2 hour),null))) as export_TS,
from `noonbilogmis.reporting.ag_instant_sales_30` s
left join noonltddwh.sales.sales_order_item soi using(item_nr)
left join   noonltddwh.cache.sales_order_item_summary oi on oi.sales_item_nr=s.item_nr
--left join noonltddwh.sales.sales_order using(id_sales_order)
where 
coalesce(outbound_warehouse,inbound_warehouse,doc_warehouse,oms_warehouse_outbound) like "%DS%"
and s.id_sales_order_item_status not in (7,9)
and id_mp=6
and soi.delivery_slot_group_ix =3
and exported_at is not null
and date(exported_at) = date_sub(current_date(),interval 1 day)
group by 1,2,3,4,country,min_exported_at
) group by 1,2
union all
select distinct  warehouse,
"exported_to_shipped_tat" as metric,  
  percentile_disc(E2S_TAT,0.95)  over(partition by  warehouse) as TAT_E2S_p95
from 
( select *, timestamp_diff(Shipped_at,export_TS,minute) as E2S_TAT,
from( 
select distinct 
  coalesce(outbound_warehouse,inbound_warehouse,doc_warehouse,oms_warehouse_outbound) as warehouse,
  shipped_at,
  item_nr,
if(country in ("AE"),timestamp_add(min_exported_at, interval 4 hour),
if(country in ("SA"),timestamp_add(min_exported_at, interval 3 hour),
if(country in ("EG"),timestamp_add(min_exported_at, interval 2 hour),null))) as export_TS,
from `noonbilogmis.reporting.ag_instant_sales_30` s
left join noonltddwh.sales.sales_order_item soi using(item_nr)
left join   noonltddwh.cache.sales_order_item_summary oi on oi.sales_item_nr=s.item_nr
where 
coalesce(outbound_warehouse,inbound_warehouse,doc_warehouse,oms_warehouse_outbound) like "%DS%"
and s.id_sales_order_item_status not in (7,9)
and id_mp=6
and exported_at is not null
and is_instant=1
and soi.delivery_slot_group_ix =3
and date(Shipped_at) = date_sub(current_date(),interval 1 day)
group by 1,2,3,4,country,min_exported_at
))group by 1,2,E2S_TAT,shipped_at
union all
select
distinct warehouse,
"shipped_breach" as metric,  
safe_divide(count(distinct If(timestamp_diff(shipped_at,export_TS,minute)>40,item_nr,null)),count(distinct item_nr)) as value,
from( 
select distinct 
  coalesce(outbound_warehouse,inbound_warehouse,doc_warehouse,oms_warehouse_outbound) as warehouse,
  shipped_at,
  item_nr,
if(country in ("AE"),timestamp_add(min_exported_at, interval 4 hour),
if(country in ("SA"),timestamp_add(min_exported_at, interval 3 hour),
if(country in ("EG"),timestamp_add(min_exported_at, interval 2 hour),null))) as export_TS,
from `noonbilogmis.reporting.ag_instant_sales_30` s
left join noonltddwh.sales.sales_order_item soi using(item_nr)
left join   noonltddwh.cache.sales_order_item_summary oi on oi.sales_item_nr=s.item_nr
--left join noonltddwh.sales.sales_order using(id_sales_order)
where 
coalesce(outbound_warehouse,inbound_warehouse,doc_warehouse,oms_warehouse_outbound) like "%DS%"
and s.id_sales_order_item_status not in (7,9)
and id_mp=6
and soi.delivery_slot_group_ix =3
and exported_at is not null
and date(shipped_at) = date_sub(current_date(),interval 1 day)
group by 1,2,3,4,country,min_exported_at
) group by 1,2
union all
select distinct warehouse,
"Shipped_to_Delivery_TAT" as metric,  
  percentile_disc(S2D_TAT,0.95)  over(partition by warehouse) as TAT_E2S_p95
from 
( select *, timestamp_diff(delivered_at,Shipped_at,minute) as S2D_TAT,
from( 
select distinct 
  coalesce(outbound_warehouse,inbound_warehouse,doc_warehouse,oms_warehouse_outbound) as warehouse,
  shipped_at,
  item_nr,
  delivered_at,
from `noonbilogmis.reporting.ag_instant_sales_30` s
left join noonltddwh.sales.sales_order_item soi using(item_nr)
left join   noonltddwh.cache.sales_order_item_summary oi on oi.sales_item_nr=s.item_nr
where 
coalesce(outbound_warehouse,inbound_warehouse,doc_warehouse,oms_warehouse_outbound) like "%DS%"
and s.id_sales_order_item_status not in (7,9)
and id_mp=6
and soi.delivery_slot_group_ix =3
and delivered_at is not null
and is_instant=1
and date(delivered_at) = date_sub(current_date(),interval 1 day)
group by 1,2,3,4,country,min_exported_at
))group by 1,2,S2D_TAT,delivered_at
union all
select distinct warehouse,
"Order_to_Delivery_TAT" as metric,  
  percentile_disc(O2D_TAT,0.95)  over(partition by  warehouse) as TAT_E2S_p95
from 
( select *, timestamp_diff(delivered_at,order_at,minute) as O2D_TAT,
from( 
select distinct 
  coalesce(outbound_warehouse,inbound_warehouse,doc_warehouse,oms_warehouse_outbound) as warehouse,
  order_at,
  item_nr,
  delivered_at,
from `noonbilogmis.reporting.ag_instant_sales_30` s
left join noonltddwh.sales.sales_order_item soi using(item_nr)
left join   noonltddwh.cache.sales_order_item_summary oi on oi.sales_item_nr=s.item_nr
where 
coalesce(outbound_warehouse,inbound_warehouse,doc_warehouse,oms_warehouse_outbound) like "%DS%"
and s.id_sales_order_item_status not in (7,9)
and id_mp=6
and delivered_at is not null
and is_instant=1
and soi.delivery_slot_group_ix =3
and date(delivered_at) = date_sub(current_date(),interval 1 day)
group by 1,2,3,4,country,min_exported_at
))group by 1,2,O2D_TAT,delivered_at
union all
select distinct warehouse,
 "Order_to_delivery_breach" as metric,  
safe_divide(count(distinct If(timestamp_diff(delivered_at,order_at,minute)>90,item_nr,null)),count(distinct item_nr)) as value,
from( 
select distinct 
  coalesce(outbound_warehouse,inbound_warehouse,doc_warehouse,oms_warehouse_outbound) as warehouse,
  order_at,
  item_nr,
delivered_at,
from `noonbilogmis.reporting.ag_instant_sales_30` s
left join noonltddwh.sales.sales_order_item soi using(item_nr)
left join   noonltddwh.cache.sales_order_item_summary oi on oi.sales_item_nr=s.item_nr
--left join noonltddwh.sales.sales_order using(id_sales_order)
where 
coalesce(outbound_warehouse,inbound_warehouse,doc_warehouse,oms_warehouse_outbound) like "%DS%"
and s.id_sales_order_item_status not in (7,9)
and id_mp=6
and soi.delivery_slot_group_ix =3
and delivered_at is not null
and date(delivered_at) = date_sub(current_date(),interval 1 day)
) group by 1,2
union all
select warehouse,
"perfect_order" as metrices,
count(distinct order_nr) as value
from 
 (
select distinct coalesce(outbound_warehouse,inbound_warehouse,doc_warehouse,oms_warehouse_outbound) as warehouse,
  order_nr,
  count(item_nr) as item_count,
  max(delivered_at) as delivered_at,
  max(s.estimated_delivery_at) as estimated_delivered,
  If(max(s.estimated_delivery_at) > max(delivered_at),1,0) as perfect_order,
from `noonbilogmis.reporting.ag_instant_sales_30` s
left join noonltddwh.sales.sales_order_item soi using(item_nr)
where 
coalesce(outbound_warehouse,inbound_warehouse,doc_warehouse,oms_warehouse_outbound) like "%DS%"
and s.id_sales_order_item_status not in (7,9)
and id_mp=6
and soi.delivery_slot_group_ix =3
and exported_at is not null
and date(delivered_at) = date_sub(current_date(),interval 1 day)
group by 1,2,s.estimated_delivery_at
)where perfect_order=1
group by 1,2
union all
select  warehouse,
"perfect_order_per" as metrices,
safe_divide(count(distinct if(perfect_order=1,order_nr,"")),count(distinct order_nr)) as value
from 
 (
select distinct coalesce(outbound_warehouse,inbound_warehouse,doc_warehouse,oms_warehouse_outbound) as warehouse,
  order_nr,
  count(item_nr) as item_count,
  max(delivered_at) as delivered_at,
  max(s.estimated_delivery_at) as estimated_delivered,
  If(max(s.estimated_delivery_at) > max(delivered_at),1,0) as perfect_order,
from `noonbilogmis.reporting.ag_instant_sales_30` s
left join noonltddwh.sales.sales_order_item soi using(item_nr)
where 
coalesce(outbound_warehouse,inbound_warehouse,doc_warehouse,oms_warehouse_outbound) like "%DS%"
and s.id_sales_order_item_status not in (7,9)
and soi.delivery_slot_group_ix =3
and id_mp=6
and exported_at is not null
and date(delivered_at)= date_sub(current_date(),interval 1 day)
group by 1,2,s.estimated_delivery_at
)
group by 1,2
UNION ALL 
select distinct coalesce(outbound_warehouse,inbound_warehouse,doc_warehouse,oms_warehouse_outbound) as warehouse,
 "OOS" as metric,
count(distinct if(si.id_sales_order_item_status = 7 and si.cancel_reason_code in ("C50","C44","C18"),item_nr,null)) as oos_count
from `noonbilogmis.reporting.ag_instant_sales_30` si
left join noonltddwh.sales.sales_order_item soi using(item_nr)
--left join noonltddwh.sales.sales_order using(id_sales_order)
where 
coalesce(outbound_warehouse,inbound_warehouse,doc_warehouse,oms_warehouse_outbound) like "%DS%"
and id_invoice_section =1
and id_mp=6
and soi.delivery_slot_group_ix =3
and date(order_at) = date_sub(current_date(),interval 1 day)
group by 1,2
union all
select distinct coalesce(outbound_warehouse,inbound_warehouse,doc_warehouse,oms_warehouse_outbound) as warehouse,
 "OOS_perc" as metric,
safe_divide(count(distinct if(si.id_sales_order_item_status = 7 and si.cancel_reason_code in ("C50","C44","C18"),item_nr,null)), count(distinct item_nr) )as oos_perc
from `noonbilogmis.reporting.ag_instant_sales_30` si
left join noonltddwh.sales.sales_order_item soi using(item_nr)
--left join noonltddwh.sales.sales_order using(id_sales_order)
where 
coalesce(outbound_warehouse,inbound_warehouse,doc_warehouse,oms_warehouse_outbound) like "%DS%"
and id_invoice_section =1
and id_mp=6
and soi.delivery_slot_group_ix =3
and date(order_at) = date_sub(current_date(),interval 1 day)
group by 1,2
union all
select distinct coalesce(outbound_warehouse,inbound_warehouse,doc_warehouse,oms_warehouse_outbound) as warehouse,
 "Docline_cancelled" as metric,
safe_divide(count(distinct if(si.dl_state="canceled",item_nr,null)), count(distinct item_nr))  as Docline_cancelled_perc
from `noonbilogmis.reporting.ag_instant_sales_30` si
left join noonltddwh.sales.sales_order_item soi using(item_nr)
where 
coalesce(outbound_warehouse,inbound_warehouse,doc_warehouse,oms_warehouse_outbound) like "%DS%"
and id_invoice_section =1
and id_mp=6
and soi.delivery_slot_group_ix =3
and date(order_at) = date_sub(current_date(),interval 1 day)
group by 1,2
union all
select distinct coalesce(outbound_warehouse,inbound_warehouse,doc_warehouse,oms_warehouse_outbound) as warehouse,
 "Docline_cancelled_other_than_oos" as metric,
safe_divide(count(distinct if(si.dl_state="canceled" and si.cancel_reason_code not in ("C50","C44","C18"),item_nr,null)), count(distinct item_nr))  as Docline_cancelled_perc
from `noonbilogmis.reporting.ag_instant_sales_30` si
left join noonltddwh.sales.sales_order_item soi using(item_nr)
where 
coalesce(outbound_warehouse,inbound_warehouse,doc_warehouse,oms_warehouse_outbound) like "%DS%"
and id_invoice_section =1
and si.id_sales_order_item_status != 9
and id_mp=6
and soi.delivery_slot_group_ix =3
and date(order_at) = date_sub(current_date(),interval 1 day)
group by 1,2
union all 
select distinct coalesce(outbound_warehouse,inbound_warehouse,doc_warehouse,oms_warehouse_outbound) as warehouse,
 "Docline_cancelled_other_than_oos_items" as metric,
count(distinct if(si.dl_state="canceled" and si.cancel_reason_code not in ("C50","C44","C18"),item_nr,null)) as Docline_cancelled_items
from `noonbilogmis.reporting.ag_instant_sales_30` si
left join noonltddwh.sales.sales_order_item soi using(item_nr)
where 
coalesce(outbound_warehouse,inbound_warehouse,doc_warehouse,oms_warehouse_outbound) like "%DS%"
and id_invoice_section =1
and soi.delivery_slot_group_ix =3
and si.id_sales_order_item_status != 9
and id_mp=6
and date(order_at) = date_sub(current_date(),interval 1 day)
group by 1,2
'''
[[authview]]
dag_id = 'logmis'
task_id = 'TPL_yesterday'
destination_table = 'noonbilogmis.reporting.TPL_yesterday_body'
sql = '''
SELECT 
PDD,
COUNT(DISTINCT awb_nr ) Total_shipment
,COUNT(DISTINCT case when B_flag = 1 then awb_nr end ) as Total_Breach
,Concat(ROUND(COUNT(DISTINCT case when B_flag = 1 then awb_nr end )*100/COUNT(DISTINCT awb_nr ) ,2),"%") AS Total_Breach_per
,COUNT(DISTINCT case when S_flag = 1 then awb_nr end )as Ship_Breach
,Concat(ROUND(COUNT(DISTINCT case when S_flag = 1 then awb_nr end )*100/COUNT(DISTINCT awb_nr ) ,2),"%") AS Total_Ship_per
,COUNT(DISTINCT case when handover_flag = 1 then awb_nr end )as Handover_Breach
,Concat(ROUND(COUNT(DISTINCT case when handover_flag = 1 then awb_nr end )*100/COUNT(DISTINCT awb_nr ) ,2),"%") AS Total_handover_per
,COUNT(DISTINCT case when handover_flag = 0 and B_flag = 1 and S_flag =0   then awb_nr end )as TPL_Breach
,Concat(ROUND(COUNT(DISTINCT case when handover_flag = 0 and B_flag = 1 and S_flag =0 then awb_nr end )*100/COUNT(DISTINCT awb_nr ) ,2),"%") AS Total_TPL_per
From
(
SELECT
destination_city,
ls.awb_nr,awb,PDD,ls.attempt1 ,dsp,ship_dt_noon, ship_cutoff, handover_cut, handover_dt, handover_d, ship_cut_flag, handover_cut_flag,
CASE WHEN handover_cut < handover_d then 1 else 0 end as handover_flag
,CASE WHEN PDD < attempt1 then 1 else 0 end as B_flag
,CASE WHEN ship_cutoff < ship_dt_noon then 1 else 0 end as S_flag

FROM `noonbilogmis.reporting.3PL_ALL111_LS` ls
LEFT JOIN
(
SELECT sois.awb_nr, DATE(soi.estimated_delivery_at) PDD,destination_city
FROM noonltddwh.sales.sales_order_item soi
left join `noonltddwh.sales.sales_order` so on so.id_sales_order =soi.id_sales_order
left join `noonltddwh.sales.sales_order_item_shipment` sois on soi.id_sales_order_item_shipment =sois.id_sales_order_item_shipment
left join (select address_code, id_city from `noonltddwh.bqsync.customer_address`) as ca on so.address_code = ca.address_code
left join (select id_city, name_en as destination_city from `noonltddwh.ref.city` ) as dc on ca.id_city=dc.id_city
WHERE soi.id_invoice_section = 1
and so.id_cart_type = 1
and soi.id_sales_order_item_status not in (1,2,7,9)
) dd on ls.awb = dd.awb_nr
WHERE PDD = current_date() - 1
and country_code = 'SA'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
)group by 1
'''

[[authview]]
dag_id = 'logmis'
task_id = 'NP_CIR_History'
destination_table = 'noonbilogmis.reporting.NP_CIR_History'
sql = '''
SELECT * FROM `noonbilogmis.NA_tables.cir_history`
'''

[[authview]]
dag_id = 'logmis'
task_id = 'TPL_yesterday1'
destination_table = 'noonbilogmis.reporting.TPL_yesterday_rawdata'
sql = '''
SELECT 
*
,case when handover_flag = 0 and B_flag = 1 and S_flag =0 then 1 else 0 end as Tpl_flag
FROM 
(
SELECT
destination_city,
ls.awb_nr,awb,PDD,ls.attempt1 ,dsp,ship_dt_noon, ship_cutoff, handover_cut, handover_dt, handover_d, ship_cut_flag, handover_cut_flag,
CASE WHEN handover_cut < handover_d then 1 else 0 end as handover_flag
,CASE WHEN PDD < attempt1 then 1 else 0 end as B_flag
,CASE WHEN ship_cutoff < ship_dt_noon then 1 else 0 end as S_flag

FROM `noonbilogmis.reporting.3PL_ALL111_LS` ls
LEFT JOIN
(
SELECT sois.awb_nr, DATE(soi.estimated_delivery_at) PDD,destination_city
FROM noonltddwh.sales.sales_order_item soi
left join `noonltddwh.sales.sales_order` so on so.id_sales_order =soi.id_sales_order
left join `noonltddwh.sales.sales_order_item_shipment` sois on soi.id_sales_order_item_shipment =sois.id_sales_order_item_shipment
left join (select address_code, id_city from `noonltddwh.bqsync.customer_address`) as ca on so.address_code = ca.address_code
left join (select id_city, name_en as destination_city from `noonltddwh.ref.city` ) as dc on ca.id_city=dc.id_city
WHERE soi.id_invoice_section = 1
and so.id_cart_type = 1
and soi.id_sales_order_item_status not in (1,2,7,9)
) dd on ls.awb = dd.awb_nr
WHERE PDD = current_date() - 1
and country_code = 'SA'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
)
'''


[[authview]]
dag_id = 'logmis'
task_id = 'partner_partner_warehouse'
destination_table = 'noonbilogmis.reporting.partner_partner_warehouse'
sql = '''
SELECT * from `noonltddwh.partner.partner_warehouse`
'''

[[authview]]
dag_id = 'logmis'
task_id = 'Base_table_LMS_Carrier'
destination_table = 'noonbilogmis.base_table.lms_carrier'
sql = '''
SELECT * FROM `noonltddwh.lms.carrier` 
'''


[[authview]]
dag_id = 'logmis'
task_id = 'asn_view_eg_NP'
destination_table = 'noonbilogmis.reporting.asn_view_eg_NP'
sql = '''
select
creq_nr , 
hub.code AS Hub,
client_ref as ASN,c.created_at as Created_at,
s.updated_at as Last_update_TS,num_attempts as Attempts_cnt,
display_name as Seller_name,
clt.code as Creq_leg_type,
ls.name as Status,
from `noonltddwh.lms.creq` c
left join (select * from `noonltddwh.lms.creq_leg` cl where id_creq_leg_type=1)s using (id_creq)
left join `noonltddwh.lms.address` a using (id_address)
left join `noonltddwh.partner.partner_warehouse` pw on code =address_uid
left join (Select * From noonltddwh.lms.status ls where id_status = 1)ls on (s.id_status=ls.id_status)
left join noonltddwh.lms.creq_leg_type clt using (id_creq_leg_type)
left join noonltddwh.lms.country lcc using (id_country)
left join `noonltddwh.lms.hub_sector` hs using(id_hub_sector)
left join `noonltddwh.lms.hub` hub using (id_hub)
Where id_country = 3
'''




[[authview]]
dag_id = 'logmis'
task_id = '3PL_CIR_Pick_return_body'
destination_table = 'noonbilogmis.reporting.3PL_CIR_Pick_return_body'
sql = '''
Select 
case 
     when timestamp_diff(current_date,pickup_date,day) between 0 and 2 then "0 - 2 Days"
     when timestamp_diff(current_date,pickup_date,day) between 2 and 4 then  "2 to 4 Days"
     when timestamp_diff(current_date,pickup_date,day)> 4 then  "Above 4 Days"
     end as Ageing,
     Count(Distinct lms_awb_nr) Total_lms_shipments,
from (SELECT
  item.awb_nr AS lms_awb_nr,
  client_ref AS cir_request,
  reference_nr AS tpl_awb_nr,
  DATE(item.created_at) AS pickup_date,
  current_date() as current_date,
  id_loc_type
FROM
  `noonltddwh.lms.tpl_pickup_shipment_carrier_reference` cr
LEFT JOIN
  `noonltddwh.lms.creq` creq
USING
  (id_creq)
LEFT JOIN
  `noonltddwh.lms.creq_leg` creq_leg
USING
  (id_creq)
LEFT JOIN
  `noonltddwh.lms.item_pkg` item_pkg
USING
  (id_creq_leg)
LEFT JOIN
  `noonltddwh.lms.item_state` item_state
USING
  (id_item)
LEFT JOIN
  `noonltddwh.lms.item` item
USING
 (id_item)
WHERE
  id_creq_leg_type=2
  AND id_loc_type=18
  )
  group by 1
  order by 1
'''


[[authview]]
dag_id = 'logmis'
task_id = '3PL_CIR_Pick_return_attachment'
destination_table = 'noonbilogmis.reporting.3PL_CIR_Pick_return_attachment'
sql = '''
SELECT
  item.awb_nr AS lms_awb_nr,
  client_ref AS cir_request,
  reference_nr AS tpl_awb_nr,
  DATE(item.created_at) AS pickup_date,
  current_date() as current_date,
  id_loc_type
FROM
  `noonltddwh.lms.tpl_pickup_shipment_carrier_reference` cr
LEFT JOIN
  `noonltddwh.lms.creq` creq
USING
  (id_creq)
LEFT JOIN
  `noonltddwh.lms.creq_leg` creq_leg
USING
  (id_creq)
LEFT JOIN
  `noonltddwh.lms.item_pkg` item_pkg
USING
  (id_creq_leg)
LEFT JOIN
  `noonltddwh.lms.item_state` item_state
USING
  (id_item)
LEFT JOIN
  `noonltddwh.lms.item` item
USING
 (id_item)
WHERE
  id_creq_leg_type=2
  AND id_loc_type=18
'''

[[authview]]
dag_id = 'logmis'
task_id = 'lms_hub_airport'
destination_table = 'noonbilogmis.reporting.lms_hub_airport'
sql = '''
SELECT * FROM  `noonltddwh.lms.hub_airport`
'''

[[authview]]
dag_id = 'logmis'
task_id = 'bqsync_customer_address'
destination_table = 'noonbilogmis.reporting.bqsync_customer_address'
sql = '''
SELECT * FROM  `noonltddwh.bqsync.customer_address`
'''


[[matview]]
dag_id = 'logmis1'
task_id = 'extra_rto'
destination_table = 'noonbilogmis.reporting.extra_rto'
sql = '''
WITH d as (
SELECT
ifs.awb_nr
,origin_warehouse
,DATE(ifs.shipment_creation_TS) Ship_dt
,shipment_status
,DATE(updated_at) updated_at
FROM `noonbilogoi.battleplan_v1.item_final_state` ifs
LEFT JOIN
(
SELECT awb_nr, if(id_partner_warehouse_from= id_partner_warehouse_outbound,"FBN","Non-FBN") as Business_Unit,
FROM `noonltddwh.sales.sales_order_item` soi
LEFT JOIN noonltddwh.sales.sales_order_item_shipment sois using(id_sales_order_item_shipment)
left join `noonltddwh.purchase_v2.purchase_order_item` poi on soi.id_sales_order_item=poi.id_sales_order_item
LEFT JOIN `noonltddwh.oms.purchase_item` pi on pi.purchase_item_nr = poi.purchase_item_nr
LEFT JOIN `noonltddwh.oms.stock_item` b USING (id_purchase_item)
) FBN on ifs.awb_nr = FBN.awb_nr
where display_name IN ('extra dammam',
'extra riyadh',
'extra Jeddah')
and Business_Unit = 'FBN'
)
SELECT d.*,del.Delivery_Manifest,NDR.Return_Manifest
FROM (
SELECT origin_warehouse,Ship_dt,COUNT(distinct awb_nr) Inbound_Manifest
FROM d
GROUP BY 1,2
) d
LEFT JOIN
(
SELECT origin_warehouse, DATE(updated_at) updated_at ,
COUNT(distinct awb_nr) Delivery_Manifest
FROM d
WHERE shipment_status = 'Delivery'
GROUP BY 1,2
) del on d.origin_warehouse = del.origin_warehouse and del.updated_at = d.Ship_dt
LEFT JOIN
(
SELECT origin_warehouse, DATE(updated_at) updated_at ,
COUNT(distinct awb_nr) Return_Manifest
FROM d
WHERE shipment_status = 'NDR'
GROUP BY 1,2
) NDR on d.origin_warehouse = NDR.origin_warehouse and NDR.updated_at = d.Ship_dt
'''




[[matview]]
dag_id = 'NP_RTV_ll'
task_id = 'RTV_longitude_latitude'
destination_table = 'noonbilogmis.reporting.RTV_longitude_latitude'
sql = '''
SELECT DISTINCT * FROM (SELECT
  *,
  ROUND(ST_DISTANCE( ST_GEOGPOINT(user_lat/10000000,
        user_lng/10000000),
      ST_GEOGPOINT(partner_lat/10000000,
        partner_longitude/10000000)),2) AS distance_m
FROM (
  SELECT
    a.attempt_date,
    a.username,
    a.client_ref,
    a.awb_nr,
    a.user_lat,
    a.user_lng,
    a.address_uid,
    a.id_country,
    b.id_partner_owner,
    b.lat AS partner_lat,
    b.lng AS partner_longitude
  FROM (
    SELECT
      DATE(a.created_at) AS attempt_date,
      username,
      c.client_ref,
      f.awb_nr,
      user_lat,
      user_lng,
      e.id_country AS id_country,
      address_uid
    FROM
      `noonltddwh.lms.creq_leg_attempt_log` a
    LEFT JOIN
      `noonltddwh.lms.creq_leg` b
    USING
      (id_creq_leg)
    LEFT JOIN
      `noonltddwh.lms.creq` c
    USING
      (id_creq)
    LEFT JOIN (
      SELECT
        a.address_uid,
        a.id_address,
        a.id_country,
        b.code,
        b.id_hub
      FROM
        `noonltddwh.lms.address` a
      LEFT JOIN
        `noonltddwh.lms.hub` b
      USING
        (id_address)) e
    USING
      (id_address)
    LEFT JOIN
      `noonltddwh.lms.user` d
    USING
      (id_user)
    LEFT JOIN (
      SELECT
        a.id_creq,
        a.client_pkg_ref,
        a.awb_nr,
        b.id_creq_leg
      FROM
        `noonltddwh.lms.client_pkg_ref` a
      LEFT JOIN
        `noonltddwh.lms.creq_leg` b
      ON
        a.id_creq = b.id_creq ) f
    ON
      a.id_creq_leg = f.id_creq_leg
    WHERE
      user_lat IS NOT NULL) a
  LEFT JOIN
    `noonltddwh.partner.partner_warehouse` b
  ON
    a.address_uid = b.code)
    )
'''


[[authview]]
dag_id = 'logmis'
task_id = 'reporting_mart_DB_RTC_warranty'
destination_table = 'noonbilogmis.reporting.reporting_mart_DB_RTC_warranty'
sql = '''
SELECT * FROM `noonbilogmis.reporting.DB_RTC_warranty` 
'''

[[authview]]
dag_id = 'logmis'
task_id = 'reporting_mart_DB_warranty1'
destination_table = 'noonbilogmis.reporting.reporting_mart_DB_warranty1'
sql = '''
SELECT * FROM `noonbilogmis.reporting.DB_warranty1` 
'''

[[authview]]
dag_id = 'logmis'
task_id = 'reporting_mart_DB_warranty2'
destination_table = 'noonbilogmis.reporting.reporting_mart_DB_warranty2'
sql = '''
SELECT * FROM `noonbilogmis.reporting.DB_warranty2` 
'''


[[authview]]
dag_id = 'logmis'
task_id = 'reporting_mart_DB_warranty3_1'
destination_table = 'noonbilogmis.reporting.reporting_mart_DB_warranty3_1'
sql = '''
SELECT * FROM `noonbilogmis.reporting.DB_warranty3_1` 
'''



[[matview]]
dag_id = 'NSSH8'
task_id = 'Held_Move'
destination_table = 'noonbilogmis.reporting.G3_Held_Move'
sql = '''
select ist1.*
, ish1.id_creq_leg
, stts.code as item_status, rs.code as reason_held 
, ish1.created_at as cl_created_at, ish1.updated_at as cl_updated_at
from 
(select it.id_item, it.awb_nr, hl.code as Hub_Location
from `noonbilogmis.base_table.lms_item` it 
left join `noonbilogmis.base_table.lms_item_state` ist using (id_item)
left join `noonbilogmis.reporting.lms_item_type` itt using (id_item_type)
left join `noonbilogmis.base_table.lms_hub_location` hl on ist.loc_id = hl.id_hub_location
where (hl.code like "DXB-G3%HELD" or hl.code like "RUH-G1%HELD")
and ist.id_loc_type = 1 
and itt.code = "pkg"
) ist1
left join (select distinct ipk.id_item,  cl.id_creq_leg, cl.id_status, cl.id_reason_held, cl.created_at , cl.updated_at
from `noonbilogmis.base_table.creq_leg` cl
left join `noonbilogmis.base_table.lms_item_pkg`  ipk using (id_creq_leg)
) ish1 using (id_item)
left join `noonbilogmis.base_table.lms_reason` rs on ish1.id_reason_held = rs.id_reason
left join `noonbilogmis.base_table.lms_status` stts on ish1.id_status = stts.id_status
order by ist1.id_item

'''

[[matview]]
dag_id = 'NSSH8'
task_id = 'No_Movements'
destination_table = 'noonbilogmis.reporting.G3_No_Movements'
sql = '''
select i.id_item, i.id_item_type, sih.awb_nr, sih.Status, sih.Loc_type,  sih.location, sih.Location_Type, sih.ScanDate as last_scan_TS, sih.username,
sih.Scan_Type, s.code,
TIMESTAMP_DIFF(current_timestamp(),sih.ScanDate, hour) as hours_from_last_scan
from (select awb_nr, max(ScanDate) as scan_date from `noonbilogoi.LOGKSA_DATA.scan_item_history` group by awb_nr) sih1
left join `noonbilogoi.LOGKSA_DATA.scan_item_history` sih on (sih1.awb_nr, sih1.scan_date) = (sih.awb_nr, sih.ScanDate)
left join `noonbilogmis.base_table.lms_item` i on (i.awb_nr) = (sih.awb_nr)
left join `noonbilogmis.base_table.lms_item_state` its using (id_item)
left join `noonbilogmis.base_table.lms_status` s using (id_status)
where (sih.location like "DXB-G3%" or sih.location like "RUH-G1%")
--and i.id_item_type = 1
order by TIMESTAMP_DIFF(current_timestamp(),sih.ScanDate, hour) desc
'''

[[matview]]
dag_id = 'NSSH8'
task_id = 'Incorrect_Putaway'
destination_table = 'noonbilogmis.reporting.G3_Incorrect_Putaway'
sql = '''
select B.*, A.SG_PDD
from
(select  hl.code as SG_location, min(date(promised_at)) as SG_PDD
from `noonbilogmis.base_table.lms_item_state` ist
left join `noonbilogmis.base_table.lms_item` it using (id_item)
left join `noonbilogmis.base_table.lms_hub_location` hl on ist.loc_id = hl.id_hub_location  
left join `noonbilogmis.base_table.lms_hub` hb using (id_hub)
left join `noonbilogmis.base_table.lms_item_pkg` ipkg on ist.id_item = ipkg.id_item
where (hl.code like "DXB-G3-SG%-X%" or hl.code like "RUH-G1-SG%-X%") and It.id_item_type =1
group by SG_location
) A 
left join
(select  hl.code as SG_location, ist.id_item, it.awb_nr, date(promised_at) as PDD, 
from `noonbilogmis.base_table.lms_item_state` ist
left join `noonbilogmis.base_table.lms_item` it using (id_item)
left join `noonbilogmis.base_table.lms_hub_location` hl on ist.loc_id = hl.id_hub_location  
left join `noonbilogmis.base_table.lms_hub` hb using (id_hub)
left join `noonbilogmis.base_table.lms_item_pkg` ipkg on ist.id_item = ipkg.id_item
where (hl.code like "DXB-G3-SG%-X%" or hl.code like "RUH-G1-SG%-X%") and It.id_item_type =1
) B on (A.SG_location) = (B.SG_location) 

'''

[[matview]]
dag_id = 'NSSH8'
task_id = 'G3_Shipments'
destination_table = 'noonbilogmis.reporting.G3_Shipments'
sql = '''
select distinct 
country.code as country
,origin_warehouse
,partner_code
,substr(hs.code,0,6) as hub
,substr(hsx.code,0,6) as forward_hub
,hsx.code as forward_hub_sector
,hs.code as hub_sector
,hss.code as hub_subsector
,cz.code as country_zone
,ct.name as C_type
,ct.short_code as creq
,clt.name as creq_leg_type
,client_ref
,json_extract(c.misc, "$.mp_code") as MP
,car.code as carrier
,display_name
,BU
,A.* 
,cl.id_reason_held as held_check
,reason.code as held_reason
,scan_type
,scan_date
,User_Last_Scan
,Location_Last_Scan
,scan_line_created_at
,TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), scan_line_created_at, hour ) AS Ageing
,date_DIFF(date(CURRENT_TIMESTAMP()), date(shipment_creation_TS), Day ) AS Shipment_Ageing
,case when clt.name = "RTO" and loc_type = "manifested" and status = "completed" then "NDR"
when clt.name = "Delivery" and loc_type = "manifested" and status = "completed" then "Delivery" 
else "Pending_closure" end as shipment_status
, mf1.manifest_nr as manifest_nr_1st
, mf1.created_at as manifest_TS_1st
, triage.code as G3_triage_location
, triage.Recieved_at_G3_triage_TS  
, date(triage.Recieved_at_G3_triage_TS) as received_at_G3_triage_Date
, SG_loc.SG_Location
, SG_loc.SG_Location_TS
, tot.tote_nr
, tot.tote_hub
, tot.Tote_Creation_TS
, tot.Item_tote_Putaway_TS
, xsort.XSort_location
, xsort.Xsort_TS

, pgroup.pgroup_awb_nr as driver_bag_nr
, pgroup.Pgroup_TS as driver_bag_TS           ------------------check this again

, ht.cline_awb_nr as HT_bag_at_G3
, ht.hub_transfer_nr
, ht.HT_Created_TS
, ht.source_location
, ht.destination_location
, ht.unpacked_at
from 
(
select distinct
ish.id_item
, id_creq_leg
, i.awb_nr
, s.code as status
, lt.code as loc_type
, REPLACE(coalesce(hl.code,
u.username,
pgroup,
ht.hub_transfer_nr,
m.manifest_nr,
mx.manifest_nr,
spc.code,
spl.code,
spll.code,
carrier.code,
tote.awb_nr,
pgroup_com,
spcl.code,
adx.address_uid),"@ops.idp.noon.team","") 
as current_location
, promised_at
, ish.updated_at as  updated_at
,i.created_at as shipment_creation_TS
,round(if (cod_value  > 0 , (cod_value /100),0) , 2 ) as cod_value
,round(if (pkg_value  > 0 , (pkg_value  /100),0) , 2 ) as pkg_value
from 
`noonbilogmis.base_table.lms_item_state` ish 
left join `noonbilogmis.base_table.lms_item` i using (id_item)
left join `noonbilogmis.base_table.lms_item_pkg` using (id_item)
left join `noonbilogmis.base_table.lms_hub_location` hl  on (ish.id_loc_type,ish.loc_id) = (1,id_hub_location)
left join `noonbilogmis.base_table.lms_user` u on (ish.id_loc_type,ish.loc_id) = (2,id_user)
left join (select id_item, awb_nr as pgroup from `noonbilogmis.base_table.lms_item_pgroup` left join `noonbilogmis.base_table.lms_item` using (id_item) ) ip on  (ish.id_loc_type,ish.loc_id) = (3,ip.id_item)
left join `noonbilogmis.base_table.lms_hub_transfer` ht on  (ish.id_loc_type,ish.loc_id) = (4,id_hub_transfer)
left join `noonbilogmis.base_table.lms_manifest` m on  (ish.id_loc_type,ish.loc_id) = (5,m.id_manifest)
left join `noonbilogmis.base_table.lms_manifest` mx on (ish.id_loc_type,ish.loc_id) = (6,mx.id_manifest)
left join `noonbilogmis.base_table.lms_service_point_counter` spc on  (ish.id_loc_type,ish.loc_id) = (8,id_service_point_counter)
left join `noonbilogmis.base_table.address` adx on  (ish.id_loc_type,ish.loc_id) = (9,adx.id_address)
left join (select id_item, awb_nr as pgroup_com from `noonbilogmis.base_table.lms_item_pgroup` left join `noonbilogmis.base_table.lms_item` using (id_item) ) ipc on (ish.id_loc_type,ish.loc_id) = (11,ipc.id_item)
left join `noonbilogmis.base_table.lms_service_point_locker` spl on (ish.id_loc_type,ish.loc_id) = (12,id_service_point_locker)
left join `noonbilogmis.base_table.lms_service_point_locker_location` spll on (ish.id_loc_type,ish.loc_id) = (17,spll.id_service_point_locker_location)
LEFT JOIN `noonbilogmis.base_table.service_point_counter_location` spcl ON (ish.id_loc_type,ish.loc_id)=(13,spcl.id_service_point_counter_location)
LEFT JOIN `noonbilogmis.base_table.LMS_Carrier` carrier ON (ish.id_loc_type,ish.loc_id)=(18,carrier.id_carrier)
LEFT JOIN `noonbilogmis.base_table.lms_item` tote ON (ish.id_loc_type,ish.loc_id)=(16,tote.id_item)
left join `noonbilogmis.base_table.lms_status` s on s.id_status = ish.id_status
left join  `noonbilogmis.reporting.lms_loc_type` lt using (id_loc_type)  
) A
left join `noonbilogmis.base_table.creq_leg` cl using (id_creq_leg)
left join `noonbilogmis.base_table.LMS_Carrier` car using (id_carrier)
left join `noonbilogmis.base_table.lms_reason` reason on id_reason_held = id_reason
left join `noonbilogmis.reporting.lms_creq_leg_type` clt using (id_creq_leg_type)
left join `noonbilogmis.base_table.creq` c on cl.id_creq = c.id_creq
left join `noonbilogmis.base_table.lms_hub_sector` hs on cl.id_hub_sector = hs.id_hub_sector
left join `noonbilogmis.base_table.lms_hub_subsector` hss on cl.id_hub_subsector = hss.id_hub_subsector
left join  `noonbilogmis.reporting.lms_creq_type` ct using (id_creq_type)
left join `noonbilogmis.base_table.lms_hub` h on hs.id_hub = h.id_hub
left join `noonbilogmis.reporting.lms_hub_airport` ha using (id_hub_airport)
left join  `noonbilogmis.base_table.lms_country` country using (id_country)
left join `noonbilogmis.base_table.country_zone`  cz using (id_country_zone)  
left join (select distinct id_item , a.address_uid as origin_warehouse , pw.partner_code as partner_code, ish.created_at , if(pw.id_partner_owner=9000,"Noon-owned","Seller") as BU , pw.display_name
from `noonbilogmis.base_table.lms_item_state_history` ish
left join `noonbilogmis.base_table.address`  a on (id_loc_type, loc_id) = (9, id_address)
left join `noonbilogmis.reporting.partner_partner_warehouse`  pw on pw.code = a.address_uid 
where id_loc_type = 9
) using (id_item)
left join ( select * from  `noonbilogmis.base_table.creq_leg` where id_creq_leg_type in (1,3)) clx on clx.id_creq = c.id_creq
left join `noonbilogmis.base_table.lms_hub_sector` hsx on clx.id_hub_sector = hsx.id_hub_sector
LEFT JOIN (SELECT id_scan_status,
scanned_code,
scan_type.name AS scan_type,
scan_date,
username AS User_Last_Scan,
hub_location.code AS Location_Last_Scan
,scan_line.created_at AS scan_line_created_at
FROM ( SELECT *  FROM ( SELECT scanned_code ,MAX(created_at) AS created_at FROM `noonbilogmis.base_table.lms_scan_line` GROUP BY 1)
LEFT JOIN  `noonbilogmis.base_table.lms_scan_line`    USING (scanned_code,created_at)) scan_line
LEFT JOIN  `noonbilogmis.base_table.lms_scan` scan    USING (id_scan)
LEFT JOIN   `noonltddwh.lms.scan_type` scan_type USING (id_scan_type)
LEFT JOIN  `noonbilogmis.base_table.lms_hub_location` hub_location  ON (scan.loc_id , scan.id_loc_type)=(hub_location.id_hub_location,1)
LEFT JOIN  `noonbilogmis.base_table.lms_user` USING (id_user)) last_scan  ON    A.awb_nr=last_scan.scanned_code
----------------1st_Manifest--------------------
left join (select distinct ish.id_item, mi.id_manifest, mi.is_outbound, mi.created_at, m.manifest_nr
from `noonbilogmis.base_table.lms_item_state_history` ish 
left join `noonbilogmis.base_table.lms_manifest_item` mi using (id_item)
left join `noonbilogmis.base_table.lms_manifest`m using (id_manifest)
where ish.id_loc_type = 5 
and mi.is_outbound = 0
)mf1 using(id_item)
----------------------Recieved_at_G3-------------------------
left join 
(select ish.id_item, hl.code,  min(ish.created_at) as Recieved_at_G3_triage_TS
from `noonbilogmis.base_table.lms_item_state_history` ish
left join `noonbilogmis.base_table.lms_hub_location` hl on (ish.id_loc_type, ish.loc_id) = (1,hl.id_hub_location)
where ish.id_loc_type = 1 and (hl.code like "DXB-G3-TRIAGE" or hl.code like "RUH-G1-TRIAGE")
group by 1,2
)triage using (id_item)
----------------------At_SG_Locations--------------
left join 
(select ish1.id_item, hl1.code as SG_Location, ish1.created_at as SG_Location_TS
from 
(select ish.id_item, max(ish.created_at) as created_at from `noonbilogmis.base_table.lms_item_state_history` ish
left join `noonbilogmis.base_table.lms_hub_location` hl on (ish.id_loc_type, ish.loc_id) = (1,hl.id_hub_location)
where ish.id_loc_type = 1 and (hl.code like "DXB-G3-SG%" or hl.code like "RUH-G1-SG%")
group by ish.id_item)
left join ( select * from `noonbilogmis.base_table.lms_item_state_history` where id_loc_type =1) ish1 using (id_item, created_at)
left join `noonbilogmis.base_table.lms_hub_location` hl1 on (ish1.id_loc_type, ish1.loc_id) = (1,hl1.id_hub_location)
)SG_loc using (id_item)
-------------------------Tote_Details ----------------------
left join
(select ish.id_item, it.awb_nr, i.awb_nr as tote_nr, h.code as tote_hub, tote.created_at as Tote_Creation_TS,
ish.created_at as Item_tote_Putaway_TS
from `noonbilogmis.base_table.lms_item_state_history` ish
left join `noonbilogmis.base_table.lms_item` it using (id_item)
left join `noonbilogmis.base_table.lms_item_tote` tote on (ish.id_loc_type,ish.loc_id) = (16,tote.id_item)
left join `noonbilogmis.base_table.lms_item` i ON tote.id_item = i.id_item and i.id_item_type = 3
left join `noonbilogmis.base_table.lms_hub` h on tote.id_hub = h.id_hub
where ish.id_loc_type = 16
and (h.code like "DXB-G3" or h.code like "RUH-G1")
) tot using (id_item)
------------------At_G3_XSORT_Locations---------------
left join 
(select ish.id_item, hl.code as XSort_location, ish.created_at as Xsort_TS
from `noonbilogmis.base_table.lms_item_state_history` ish
left join `noonbilogmis.base_table.lms_hub_location` hl on (ish.id_loc_type, ish.loc_id) = (1,hl.id_hub_location)
where ish.id_loc_type = 1 and (hl.code like "DXB-G3-XSORT%" or hl.code like "RUH-G1-XSORT%")
)xsort using (id_item)
---------------------Driver Bag- P_group_Details---------------
left join (select ip3.*, hl.code
from
(select B.*
from
(select ish.id_item, max(ish.created_at) as Pgroup_TS
from (select id_item, awb_nr as pgroup_awb_nr, id_item_pgroup_type from `noonbilogmis.base_table.lms_item_pgroup` 
     left join `noonbilogmis.base_table.lms_item` using (id_item) ) ip
left join `noonbilogmis.base_table.lms_item_pgroup_type` ipt using (id_item_pgroup_type)  
left join `noonbilogmis.base_table.lms_item_state_history` ish on (ish.id_loc_type,ish.loc_id) = (3,ip.id_item)
left join `noonbilogmis.base_table.lms_item` i on (ish.id_item) = i.id_item
where ipt.awb_suffix = "S"
and ish.id_loc_type = 3
group by 1
)A
left join 
(select ish.id_item, i.awb_nr, ish.created_at as Pgroup_TS,  ip.pgroup_awb_nr, ip.id_item as pgroup_id_item
from (select id_item, awb_nr as pgroup_awb_nr, id_item_pgroup_type from `noonbilogmis.base_table.lms_item_pgroup` 
     left join `noonbilogmis.base_table.lms_item` using (id_item) ) ip
left join `noonbilogmis.base_table.lms_item_pgroup_type` ipt using (id_item_pgroup_type)  
left join `noonbilogmis.base_table.lms_item_state_history` ish on (ish.id_loc_type,ish.loc_id) = (3,ip.id_item)
left join `noonbilogmis.base_table.lms_item` i on (ish.id_item) = i.id_item
where ipt.awb_suffix = "S"
and ish.id_loc_type = 3
) B on (A.id_item, A.Pgroup_TS) = (B.id_item, B.Pgroup_TS)
)
ip3
left join `noonbilogmis.base_table.lms_item_state_history` ish1 on (ish1.id_item, ish1.id_loc_type) = (ip3.pgroup_id_item, 1)
left join `noonbilogmis.base_table.lms_hub_location` hl on (ish1.loc_id) = (hl.id_hub_location)
where (hl.code like "DXB-G3%" or hl.code like "RUH-G1%")
)pgroup using (id_item)
----------------------------Hub_Transfer-> DXB-G3 to LM-------------------------------
left join (select i1.awb_nr as cline_awb_nr, cline.created_at, cline.unpacked_at, 
ht.hub_transfer_nr, ht.created_at as HT_Created_TS, ish.id_item, i.awb_nr, 
src.code as source_location, dst.code as destination_location
from `noonbilogmis.base_table.container_line` cline
left join `noonbilogmis.base_table.lms_item` i1 on cline.id_item = i1.id_item
left join `noonbilogmis.base_table.lms_hub_transfer` ht on (cline.id_loc_type, cline.loc_id) = (4, ht.id_hub_transfer)
left join `noonbilogmis.base_table.lms_item_state_history` ish  on (ish.loc_id) = (cline.id_item)
left join `noonbilogmis.base_table.lms_item` i on ish.id_item = i.id_item
left join `noonbilogmis.base_table.lms_hub` src on src.id_hub = id_hub_src
left join `noonbilogmis.base_table.lms_hub` dst on dst.id_hub = id_hub_dst
where cline.id_loc_type = 4
and (src.code like "DXB-G3" or src.code like "RUH-G1")
)ht using (id_item)

----------------Manifested--------------------

left join (select ish1.id_item, ish1.created_at as Manifested_TS, m1.manifest_nr as Manifested
from `noonbilogmis.base_table.lms_item_state_history` ish1 
left join `noonbilogmis.base_table.lms_manifest` m1 on  (ish1.id_loc_type,ish1.loc_id) = (6,m1.id_manifest)
where ish1.id_loc_type = 6
)MFx using(id_item)
---------------------------------------------Filters-----------------------------------
where partner_code in (select partner_code from  `noonbilogmis.reporting.partner_partner_warehouse`) 
--and date(triage.Recieved_at_G3_triage_TS) = current_date -1  
'''

#[[matview]]
#dag_id = '3PL_DAG'
#task_id = '3PL_neeraj'
#destination_table = 'noonbilogmis.reporting.3PL_neeraj'
#sql = '''
#SELECT  DISTINCT
#timestamp_add(current_timestamp() , INTERVAL 3 hour) as current_KSA_time,
#so.order_nr 
#,so.created_at as Order_dt
#,soi.estimated_delivery_at as EDD
#,case when so.payment_method_code = 'cod' then 'COD' else 'Prepaid' end as Payment_method
#,hubr.hub
#,hubr.hub_sector
#,base.*
#,check1.flag12 as Received_flag
#FROM `noonltddwh.sales.sales_order_item` soi 
#left join `noonltddwh.sales.sales_order` so using (id_sales_order)
#left join `noonltddwh.sales.sales_order_item_shipment` sois using (id_sales_order_item_shipment)
#left join 
#(SELECT DISTINCT DATE(tpl_carrier_reference.created_at) dispatch, 
#tpl_item.awb_nr awb,tpl_carrier_reference.reference_nr 
#,al.MAX_attempt as Attempt_cnt
#,c.name
#,hsc.city sp_new_city
#FROM `noonltddwh.lms.item` item
#LEFT JOIN noonltddwh.lms.tpl_item_pgroup_map tpl_item_pgroup_map ON tpl_item_pgroup_map.id_item_pgroup = item.id_item
#LEFT JOIN noonltddwh.lms.item tpl_item ON tpl_item_pgroup_map.id_item = tpl_item.id_item
#left join `noonltddwh.lms.tpl_carrier_reference` as tpl_carrier_reference on tpl_carrier_reference.id_item =tpl_item_pgroup_map.id_item_pgroup
#left join noonltddwh.lms.item_pkg ip on ip.id_item = tpl_item.id_item
#left join noonltddwh.lms.carrier c using(id_carrier)
#left join `noonltddwh.lms.creq_leg` cl using(id_creq_leg)
#left join `noonltddwh.lms.hub_sector_carrier` hsc on hsc.id_hub_sector = cl.id_hub_sector and hsc.id_carrier = c.id_carrier 
#left join(SELECT * FROM `noonbilogmis.reporting.AlmSxlmAtt` where attempt1 > current_date()-20 )al on al.WaybillNumber = tpl_carrier_reference.reference_nr 
#where date(tpl_carrier_reference.created_at) >= current_date() - 10 
#) base on base.awb = sois.awb_nr 
#LEFT JOIN
#(
#SELECT i.awb_nr awb19 ,CASE WHEN hi.id_tpl_handover_item is null then 'Not-Received' ELSE 'Received' end as flag12
#FROM noonltddwh.lms.item i
#left join  `noonltddwh.lms.item_state_history`  ish on ish.id_item = i.id_item 
#left join  noonltddwh.lms.item i1 on ish.loc_id = i1.id_item
#left join `noonltddwh.lms.tpl_handover_item` hi on hi.id_item = i1.id_item
#where  id_loc_type = 3 and hi.is_outbound = 1
#order by ish.created_at 
#) check1 on check1.awb19 = sois.awb_nr 
#left join (
#SELECT awb_nr , hub.code as Hub , hub_sector.code as hub_sector
#from `noonltddwh.lms.client_pkg_ref` client_pkg_ref
#LEFT JOIN(select * from `noonltddwh.lms.creq_leg` where id_creq_leg_type=3) as creq_leg using (id_creq)
#LEFT JOIN `noonltddwh.lms.hub_sector` hub_sector on hub_sector.id_hub_sector=creq_leg.id_hub_sector
#LEFT JOIN `noonltddwh.lms.hub` hub on hub_sector.id_hub=hub.id_hub
#) Hubr ON Hubr.awb_nr = sois.awb_nr
#WHERE base.awb IS not null
#and so.id_mp <>6
#and id_cart_type =1 
#and id_invoice_section =1 
#'''


[[matview]]
dag_id = 'LS_DS'
task_id = 'ls_darkstore'
destination_table = 'noonbilogmis.reporting.ls_darkstore'
sql = '''
SELECT t.*,Darkstore,sl.capacity,ff.system_capacity 
FROM (
SELECT 'Rest' as dd, Country,city, date(EDD) EDD,'Instant' AS Instant_order,'' as slot,extract(hour from orderts) hour,OriginWH,
CASE 
WHEN extract(hour from orderts) = 0 THEN 1.0 
WHEN extract(hour from orderts) = 1 THEN 0.7 
WHEN extract(hour from orderts) = 2 THEN 0.3 
WHEN extract(hour from orderts) = 3 THEN 0.3 
WHEN extract(hour from orderts) = 4 THEN 0.2 
WHEN extract(hour from orderts) = 5 THEN 0.3 
WHEN extract(hour from orderts) = 6 THEN 1.4 
WHEN extract(hour from orderts) = 7 THEN 3.2 
WHEN extract(hour from orderts) = 8 THEN 4.9 
WHEN extract(hour from orderts) = 9 THEN 6.3 
WHEN extract(hour from orderts) = 10 THEN 7.2 
WHEN extract(hour from orderts) = 11 THEN 6.9 
WHEN extract(hour from orderts) = 12 THEN 6.6 
WHEN extract(hour from orderts) = 13 THEN 6.2 
WHEN extract(hour from orderts) = 14 THEN 6.2 
WHEN extract(hour from orderts) = 15 THEN 5.9 
WHEN extract(hour from orderts) = 16 THEN 5.9 
WHEN extract(hour from orderts) = 17 THEN 5.7 
WHEN extract(hour from orderts) = 18 THEN 6.4 
WHEN extract(hour from orderts) = 19 THEN 7.1 
WHEN extract(hour from orderts) = 20 THEN 6.1 
WHEN extract(hour from orderts) = 21 THEN 5.2 
WHEN extract(hour from orderts) = 22 THEN 3.7 
WHEN extract(hour from orderts) = 23 THEN 2.2 
END AS uti,
COUNT(distinct CASE WHEN  Breach_Check = 'Logistic_Breach' and timestamp_add(orderts,interval 90 minute) <= case when Country = 'AE' then timestamp_add(current_timestamp,interval 4 hour) when Country = 'SA' then timestamp_add(current_timestamp,interval 3 hour) end  THEN order_nr end) order_Logistic_Breach
,COUNT(distinct CASE WHEN Breach_Check = 'Logistic_Breach' THEN item_nr end) item_nr_Logistic_Breach
,count(distinct case when timestamp_add(orderts,interval 90 minute) <= case when Country = 'AE' then timestamp_add(current_timestamp,interval 4 hour) when Country = 'SA' then timestamp_add(current_timestamp,interval 3 hour) end then order_nr end) com_order_nr
,count(distinct order_nr) order_nr
,count(distinct item_nr) item_nr
,count(distinct CASE WHEN ItemFinalStatus = 'delivered' THEN order_nr END) Del_order_nr
,count(distinct CASE WHEN ItemFinalStatus = 'delivered' THEN item_nr END) Del_item_nr
FROM (
select * , 
case when DeliveryAdherenceCheck in( "DeliveredInSlot" ,"AttemptedInslot" ) then "Compliance"
     when DeliveryAdherenceCheck = "shippingBreach" then "Non_Logistic_Breach"
     when DeliveryAdherenceCheck = "DelBreach"  then "Logistic_Breach"
     when DeliveryAdherenceCheck = "DeliveryPending" and ShippingCutoffCheck in ("ShippedAfterCutOff", "ShippingPending") then "Non_Logistic_Breach"
     when DeliveryAdherenceCheck = "DeliveryPending" and ShippingCutoffCheck in ("ShippedBeforeCutOff") then "Logistic_Breach"
     when DeliveryAdherenceCheck in ( "ShippingPending") then "Non_Logistic_Breach"
     when DeliveryAdherenceCheck = "NonShippable" then "NonShippable"
end as Breach_Check 
from (select *,
case
    when ShipmentWHHandoverAt is null and ItemFinalStatus in ('new','cancelled','invalid') then 'NonShippable'
    when ShipmentWHHandoverAt is null and ItemFinalStatus<>'cancelled' and itemFinalStatus<> 'invalid' and itemFinalStatus<> 'new' then 'ShippingPending'
    when ShipmentWHHandoverAt is not null and OutBound_Manifest_Number is null then 'DeliveryPending'
    when OutBound_Manifest_Number is null and adherance_check = "Attemptedwithin_TAT" then 'AttemptedInslot'
    when OutBound_Manifest_Number is not null and adherance_check = "within_TAT" then 'DeliveredInSlot'
    when OutBound_Manifest_Number is not null and ShippingCutoffCheck = 'ShippedBeforeCutOff' and  adherance_check = "Over_TAT" then 'DelBreach'
    when OutBound_Manifest_Number is not null and ShippingCutoffCheck = 'ShippedAfterCutOff' and  adherance_check = "Over_TAT" then 'shippingBreach'
end as DeliveryAdherenceCheck,
from((
select *,
Case when DeliveredAt is not null and timestamp_diff(DeliveredAt, OrderTS ,minute) <= 90 then "within_TAT"
     when DeliveredAt is not null and timestamp_diff(DeliveredAt, OrderTS ,minute) > 90 then "Over_TAT"
     when DeliveredAt is null and timestamp_diff(firstheld_TS, OrderTS ,minute) <= 90 then "Attemptedwithin_TAT"
     when BoxCreatedAt is null and itemfinalCategory<>0 and itemFinalCategory<>2 then 'ShippingPending'
     when BoxCreatedAt is null and itemfinalCategory in (0,2) then 'NonShippable'
     end as adherance_check,
case
    when BoxCreatedAt is null and itemfinalCategory in (0,2) then 'NonShippable'
    when BoxCreatedAt is null and itemfinalCategory<>0 and itemFinalCategory<>2 then 'ShippingPending'
    when BoxCreatedAt<cut_off then 'ShippedBeforeCutOff'
     when BoxCreatedAt>=cut_off  then 'ShippedAfterCutOff'
end as ShippingCutoffCheck,
from
(select *,
 timestamp_add(OrderTS, interval 30 MINUTE) as cut_off,
case when CancellationReason like '%Out of Stock%' then 2
      when itemFinalStatus in ('new', 'invalid','cancelled') then 0
      else 1 end ItemFinalCategory
from(
select *, timestamp_add(EDD,interval SlotStartHourGST hour) EDDStart, 
timestamp_add(EDD,interval SlotEndHourGST hour) EDDEnd from `noonbilogoi.SA.ShipmentTracker` 
where date(EDD) = date(timestamp_add(current_timestamp(),interval 1 hour))
and Instant_order = 1
))))))
WHERE ItemFinalStatus IN ('delivered','shipped','undeliverable')
GROUP BY 1,2,3,4,5,6,7,8,9
UNION ALL
SELECT 'Rest' as dd, Country,city, date(EDD) EDD, 'Daily' AS Instant_order, SlotTimeGST,0 as hour,'' OriginWH,0.0 as d,
COUNT(distinct CASE WHEN Breach_Check = 'Logistic_Breach' and EDDEnd <= case when Country = 'AE' then timestamp_add(current_timestamp,interval 4 hour) when Country = 'SA' then timestamp_add(current_timestamp,interval 3 hour) end THEN order_nr end) order_Logistic_Breach
,COUNT(distinct CASE WHEN Breach_Check = 'Logistic_Breach' THEN item_nr end) item_nr_Logistic_Breach
,count(distinct case when EDDEnd <= case when Country = 'AE' then timestamp_add(current_timestamp,interval 4 hour) when Country = 'SA' then timestamp_add(current_timestamp,interval 3 hour) end then order_nr end) order_nr
,count(distinct order_nr) order_nr
,count(distinct item_nr) item_nr
,count(distinct CASE WHEN ItemFinalStatus = 'delivered' THEN order_nr END) Del_order_nr
,count(distinct CASE WHEN ItemFinalStatus = 'delivered' THEN item_nr END) Del_item_nr
FROM(
select *,
case when DeliveryAdherenceCheck in ("DeliveredInSlot", "DeliveredBeforeSlot","AttemptedWithInslot","DeliveryAttempted")  then "Compliance"
     when DeliveryAdherenceCheck = "DelBreach" and ShippingCutoffCheck in ("ShippedAfterCutOff", "ShippingPending") then "Non_Logistic_Breach"
     when DeliveryAdherenceCheck = "DelBreach" and ShippingCutoffCheck in ("ShippedBeforeCutOff") then "Logistic_Breach"
     when DeliveryAdherenceCheck = "DeliveryPending" and ShippingCutoffCheck in ("ShippedAfterCutOff", "ShippingPending") then "Non_Logistic_Breach"
     when DeliveryAdherenceCheck = "DeliveryPending" and ShippingCutoffCheck in ("ShippedBeforeCutOff") then "Logistic_Breach"
     when DeliveryAdherenceCheck in ( "ShippingPending","PackagingPending") then "Non_Logistic_Breach"
     when DeliveryAdherenceCheck = "NonShippable" then "NonShippable"
end as Breach_Check,
case when itemFinalStatus='shipped' and First_held_reason='address_changed' then 'address_changed'
when itemFinalStatus='shipped' and First_held_reason='rescheduled' then 'rescheduled'
when itemFinalStatus='shipped' and First_held_reason='address_misrouted' then 'address_misrouted'
when itemFinalStatus='shipped' and First_held_reason='held_consolidation' then 'held_consolidation'
when itemFinalStatus='shipped' and First_held_reason='customer_canceled' then 'Shipped_CustomerCanceled'
when itemFinalStatus='shipped' and First_held_reason is null and DeliveryAdherenceCheck='PackagingPending' then 'PackagingPending'
when itemFinalStatus='shipped' and First_held_reason is null and DeliveryAdherenceCheck='DeliveryPending' then 'DeliveryPending' end
as Shipped_Breakdown,
case when itemFinalStatus='cancelled' and CancellationReason='Auto Cancellation -- Out of Stock' then 'Auto_Cancellation_OOS'
when itemFinalStatus='cancelled' and CancellationReason<>'Auto Cancellation -- Out of Stock' then 'CustomerCancelled' end
as Cancelled_Breakdown from((
select *,
case
    when BoxCreatedAt is null and itemfinalCategory in (0,2) then 'NonShippable'
    when BoxCreatedAt is null and itemfinalCategory<>0 and itemFinalCategory<>2 then 'ShippingPending'
    when BoxCreatedAt<cut_off then 'ShippedBeforeCutOff'
     when BoxCreatedAt>=cut_off  then 'ShippedAfterCutOff'
end as ShippingCutoffCheck,
case
    when ShipmentWHHandoverAt is null and ItemFinalStatus in ('new','cancelled','invalid') then 'NonShippable'
    when ShipmentWHHandoverAt is null and ItemFinalStatus<>'cancelled' and itemFinalStatus<> 'invalid' and itemFinalStatus<> 'new' then 'ShippingPending'
    when ShipmentWHHandoverAt is not null and HTCreatedAt is null and OutBound_Manifest_Number is null then 'PackagingPending'
    when ShipmentWHHandoverAt is not null and HTCreatedAt is not null and OutBound_Manifest_Number is null and firstheld_TS is not null then 'DeliveryAttempted'
    when ShipmentWHHandoverAt is not null and HTCreatedAt is not null and OutBound_Manifest_Number is null and UnreachableTS is not null then 'DeliveryAttempted'
    when ShipmentWHHandoverAt is not null and HTCreatedAt is not null and OutBound_Manifest_Number is null and firstheld_TS is null then 'DeliveryPending'
    when OutBound_Manifest_Number is not null and DeliveredAt < EddStart then 'DeliveredBeforeSlot'
    when OutBound_Manifest_Number is not null and DeliveredAt >=EDDStart and DeliveredAt <=EddEnd then 'DeliveredInSlot'
    when OutBound_Manifest_Number is not null and date(firstheld_TS) <= date(EDD) then 'AttemptedWithInslot'
    when OutBound_Manifest_Number is not null and DeliveredAt > EddEnd  then 'DelBreach'
end as DeliveryAdherenceCheck
from(select *,
case 
     when country = 'AE' and warehouseCode <> "W00017252A" and (SlotTimeGST = '5am - 9am' or SlotTimeGST = '7am - 11am') then timestamp_add(EDDStart, interval -90 MINUTE)---GMM
     when country = 'AE' and warehouseCode = "W00017252A" and (SlotTimeGST = '5am - 9am' or SlotTimeGST = '7am - 11am') then timestamp_add(EDDStart, interval -60 MINUTE)---GMM
     when country = 'AE' and warehouseCode <> "W00017252A" and (SlotTimeGST = '10am - 2pm' or SlotTimeGST = '10am - 1pm') then timestamp_add(EDDStart, interval -240 MINUTE)
     when country = 'AE' and warehouseCode = "W00017252A" and (SlotTimeGST = '10am - 2pm' or SlotTimeGST = '10am - 1pm') then timestamp_add(EDDStart, interval -180 MINUTE)
     when country = 'AE' and warehouseCode <> "W00017252A" and city <> "Dubai" and  SlotTimeGST = '11am - 7pm' then timestamp_add(EDDStart, interval -270 MINUTE)
     when country = 'AE' and warehouseCode = "W00017252A" and city <> "Dubai" and  SlotTimeGST = '11am - 7pm' then timestamp_add(EDDStart, interval -210 MINUTE)  
     when country = 'AE' and warehouseCode <> "W00017252A" and (SlotTimeGST = '12pm - 8pm' or SlotTimeGST = '2pm - 5pm') then timestamp_add(EDDStart, interval -210 MINUTE)
     when country = 'AE' and warehouseCode = "W00017252A" and (SlotTimeGST = '12pm - 8pm' or SlotTimeGST = '2pm - 5pm') then timestamp_add(EDDStart, interval -150 MINUTE)
     when country = 'AE' and warehouseCode <> "W00017252A" and city <> "Dubai" and  SlotTimeGST = '3pm - 9pm' then timestamp_add(EDDStart, interval -300 MINUTE)
     when country = 'AE' and warehouseCode = "W00017252A" and city <> "Dubai" and  SlotTimeGST = '3pm - 9pm' then timestamp_add(EDDStart, interval -240 MINUTE)
     when country = 'AE' and warehouseCode <> "W00017252A" and city <> "Dubai" and  SlotTimeGST = '9am - 3pm' then timestamp_add(EDDStart, interval -150 MINUTE)
     when country = 'AE' and warehouseCode = "W00017252A" and city <> "Dubai" and  SlotTimeGST = '9am - 3pm' then timestamp_add(EDDStart, interval -270 MINUTE)
     when country = 'AE' and warehouseCode <> "W00017252A" and (SlotTimeGST = '4pm - 9pm' or SlotTimeGST = '6pm - 9pm') then timestamp_add(EDDStart, interval -210 MINUTE)
     when country = 'AE' and warehouseCode = "W00017252A" and (SlotTimeGST = '4pm - 9pm' or SlotTimeGST = '6pm - 9pm') then timestamp_add(EDDStart, interval -150 MINUTE)
     when country = 'AE' then timestamp_add(EDDStart, interval -4 HOUR)
     when country = 'SA' and SlotTimeGST = '5pm - 9pm' then timestamp_add(EDDStart, interval -2 HOUR) --- KSA SDD Cutoff
     when country = 'SA' then timestamp_add(EDDStart, interval -4 HOUR) --- KSA REST Cutoff
end as cut_off,
case when CancellationReason like '%Out of Stock%' then 2
      when itemFinalStatus in ('new', 'invalid','cancelled', 'undeliverable') then 0
      else 1 end ItemFinalCategory
,case
when itemFinalStatus in ('new', 'invalid','cancelled', 'undeliverable') then "UD"
when FMpgroup is not null then "GB_Created"
when ShipmentWHReceivedAt is null then "Handover_Pending"
when ( FM_HTcreationTS is null and FMPgroupcreationTS is null) and Tote_nr is null then "HTPending"
when ( FMHT_unpack_TS is null and FMPgroupunpack_TS is null) and Tote_nr is null  then "HTUnpack_Pending"
when Tote_nr is null then "Putaway_Pending"
when BoxCreatedAt is null then "GBCreationPending"
when DeliveredAt is not null then "Delivered"
else "Check" end as FMRemark      
from
(select *, timestamp_add(EDD,interval SlotStartHourGST hour) EDDStart, 
timestamp_add(EDD,interval SlotEndHourGST hour) EDDEnd from `noonbilogoi.SA.ShipmentTracker` 
where date(EDD) = date(timestamp_add(current_timestamp(),interval 1 hour))
and  instant_order=0
)))))
WHERE ItemFinalStatus IN ('delivered','shipped','undeliverable')

GROUP BY 1,2,3,4,5,6,7,8
#----------------------------------------
UNION ALL
SELECT 'Overall' as dd, Country,'' as city, date(EDD) EDD,'Instant' AS Instant_order,'' as slot,0 as hour,'' as OriginWH,0.0 as d,
COUNT(distinct CASE WHEN  Breach_Check = 'Logistic_Breach' and timestamp_add(orderts,interval 90 minute) <= case when Country = 'AE' then timestamp_add(current_timestamp,interval 4 hour) when Country = 'SA' then timestamp_add(current_timestamp,interval 3 hour) end  THEN order_nr end) order_Logistic_Breach
,COUNT(distinct CASE WHEN Breach_Check = 'Logistic_Breach' THEN item_nr end) item_nr_Logistic_Breach

,count(distinct case when timestamp_add(orderts,interval 90 minute) <= case when Country = 'AE' then timestamp_add(current_timestamp,interval 4 hour) when Country = 'SA' then timestamp_add(current_timestamp,interval 3 hour) end then order_nr end) com_order_nr
,count(distinct order_nr) order_nr
,count(distinct item_nr) item_nr
,count(distinct CASE WHEN ItemFinalStatus = 'delivered' THEN order_nr END) Del_order_nr
,count(distinct CASE WHEN ItemFinalStatus = 'delivered' THEN item_nr END) Del_item_nr
FROM (
select * , 
case when DeliveryAdherenceCheck in( "DeliveredInSlot" ,"AttemptedInslot" ) then "Compliance"
     when DeliveryAdherenceCheck = "shippingBreach" then "Non_Logistic_Breach"
     when DeliveryAdherenceCheck = "DelBreach"  then "Logistic_Breach"
     when DeliveryAdherenceCheck = "DeliveryPending" and ShippingCutoffCheck in ("ShippedAfterCutOff", "ShippingPending") then "Non_Logistic_Breach"
     when DeliveryAdherenceCheck = "DeliveryPending" and ShippingCutoffCheck in ("ShippedBeforeCutOff") then "Logistic_Breach"
     when DeliveryAdherenceCheck in ( "ShippingPending") then "Non_Logistic_Breach"
     when DeliveryAdherenceCheck = "NonShippable" then "NonShippable"
end as Breach_Check 
from (
select *,
case
    when ShipmentWHHandoverAt is null and ItemFinalStatus in ('new','cancelled','invalid') then 'NonShippable'
    when ShipmentWHHandoverAt is null and ItemFinalStatus<>'cancelled' and itemFinalStatus<> 'invalid' and itemFinalStatus<> 'new' then 'ShippingPending'
    when ShipmentWHHandoverAt is not null and OutBound_Manifest_Number is null then 'DeliveryPending'
    when OutBound_Manifest_Number is null and adherance_check = "Attemptedwithin_TAT" then 'AttemptedInslot'
    when OutBound_Manifest_Number is not null and adherance_check = "within_TAT" then 'DeliveredInSlot'
    when OutBound_Manifest_Number is not null and ShippingCutoffCheck = 'ShippedBeforeCutOff' and  adherance_check = "Over_TAT" then 'DelBreach'
    when OutBound_Manifest_Number is not null and ShippingCutoffCheck = 'ShippedAfterCutOff' and  adherance_check = "Over_TAT" then 'shippingBreach'
end as DeliveryAdherenceCheck,
from((
select *,
Case when DeliveredAt is not null and timestamp_diff(DeliveredAt, OrderTS ,minute) <= 90 then "within_TAT"
     when DeliveredAt is not null and timestamp_diff(DeliveredAt, OrderTS ,minute) > 90 then "Over_TAT"
     when DeliveredAt is null and timestamp_diff(firstheld_TS, OrderTS ,minute) <= 90 then "Attemptedwithin_TAT"
     when BoxCreatedAt is null and itemfinalCategory<>0 and itemFinalCategory<>2 then 'ShippingPending'
     when BoxCreatedAt is null and itemfinalCategory in (0,2) then 'NonShippable'
     end as adherance_check,
case
    when BoxCreatedAt is null and itemfinalCategory in (0,2) then 'NonShippable'
    when BoxCreatedAt is null and itemfinalCategory<>0 and itemFinalCategory<>2 then 'ShippingPending'
    when BoxCreatedAt<cut_off then 'ShippedBeforeCutOff'
     when BoxCreatedAt>=cut_off  then 'ShippedAfterCutOff'
end as ShippingCutoffCheck,
from(select *,
 timestamp_add(OrderTS, interval 30 MINUTE) as cut_off,
case when CancellationReason like '%Out of Stock%' then 2
      when itemFinalStatus in ('new', 'invalid','cancelled') then 0
      else 1 end ItemFinalCategory
from(
select *, timestamp_add(EDD,interval SlotStartHourGST hour) EDDStart, 
timestamp_add(EDD,interval SlotEndHourGST hour) EDDEnd from `noonbilogoi.SA.ShipmentTracker` 
where date(EDD) = date(timestamp_add(current_timestamp(),interval 1 hour))
and  Instant_order = 1
))))))
WHERE ItemFinalStatus IN ('delivered','shipped','undeliverable')
GROUP BY 1,2,3,4,5,6,7,8,9
UNION ALL
SELECT 'Overall' as dd, Country,'' as city, date(EDD) EDD, 'Daily' AS Instant_order,'' as SlotTimeGST,0 as hour,'' OriginWH,0.0 as d,
COUNT(distinct CASE WHEN Breach_Check = 'Logistic_Breach' and EDDEnd <= case when Country = 'AE' then timestamp_add(current_timestamp,interval 4 hour) when Country = 'SA' then timestamp_add(current_timestamp,interval 3 hour) end THEN order_nr end) order_Logistic_Breach
,COUNT(distinct CASE WHEN Breach_Check = 'Logistic_Breach' THEN item_nr end) item_nr_Logistic_Breach
,count(distinct case when EDDEnd <= case when Country = 'AE' then timestamp_add(current_timestamp,interval 4 hour) when Country = 'SA' then timestamp_add(current_timestamp,interval 3 hour) end then order_nr end) order_nr
,count(distinct order_nr) order_nr
,count(distinct item_nr) item_nr
,count(distinct CASE WHEN ItemFinalStatus = 'delivered' THEN order_nr END) Del_order_nr
,count(distinct CASE WHEN ItemFinalStatus = 'delivered' THEN item_nr END) Del_item_nr
FROM(
select *,
case when DeliveryAdherenceCheck in ("DeliveredInSlot", "DeliveredBeforeSlot","AttemptedWithInslot","DeliveryAttempted")  then "Compliance"
     when DeliveryAdherenceCheck = "DelBreach" and ShippingCutoffCheck in ("ShippedAfterCutOff", "ShippingPending") then "Non_Logistic_Breach"
     when DeliveryAdherenceCheck = "DelBreach" and ShippingCutoffCheck in ("ShippedBeforeCutOff") then "Logistic_Breach"
     when DeliveryAdherenceCheck = "DeliveryPending" and ShippingCutoffCheck in ("ShippedAfterCutOff", "ShippingPending") then "Non_Logistic_Breach"
     when DeliveryAdherenceCheck = "DeliveryPending" and ShippingCutoffCheck in ("ShippedBeforeCutOff") then "Logistic_Breach"
     when DeliveryAdherenceCheck in ( "ShippingPending","PackagingPending") then "Non_Logistic_Breach"
     when DeliveryAdherenceCheck = "NonShippable" then "NonShippable"
end as Breach_Check,
case when itemFinalStatus='shipped' and First_held_reason='address_changed' then 'address_changed'
when itemFinalStatus='shipped' and First_held_reason='rescheduled' then 'rescheduled'
when itemFinalStatus='shipped' and First_held_reason='address_misrouted' then 'address_misrouted'
when itemFinalStatus='shipped' and First_held_reason='held_consolidation' then 'held_consolidation'
when itemFinalStatus='shipped' and First_held_reason='customer_canceled' then 'Shipped_CustomerCanceled'
when itemFinalStatus='shipped' and First_held_reason is null and DeliveryAdherenceCheck='PackagingPending' then 'PackagingPending'
when itemFinalStatus='shipped' and First_held_reason is null and DeliveryAdherenceCheck='DeliveryPending' then 'DeliveryPending' end
as Shipped_Breakdown,
case when itemFinalStatus='cancelled' and CancellationReason='Auto Cancellation -- Out of Stock' then 'Auto_Cancellation_OOS'
when itemFinalStatus='cancelled' and CancellationReason<>'Auto Cancellation -- Out of Stock' then 'CustomerCancelled' end
as Cancelled_Breakdown from((
select *,
case
    when BoxCreatedAt is null and itemfinalCategory in (0,2) then 'NonShippable'
    when BoxCreatedAt is null and itemfinalCategory<>0 and itemFinalCategory<>2 then 'ShippingPending'
    when BoxCreatedAt<cut_off then 'ShippedBeforeCutOff'
     when BoxCreatedAt>=cut_off  then 'ShippedAfterCutOff'
end as ShippingCutoffCheck,
case
    when ShipmentWHHandoverAt is null and ItemFinalStatus in ('new','cancelled','invalid') then 'NonShippable'
    when ShipmentWHHandoverAt is null and ItemFinalStatus<>'cancelled' and itemFinalStatus<> 'invalid' and itemFinalStatus<> 'new' then 'ShippingPending'
    when ShipmentWHHandoverAt is not null and HTCreatedAt is null and OutBound_Manifest_Number is null then 'PackagingPending'
    when ShipmentWHHandoverAt is not null and HTCreatedAt is not null and OutBound_Manifest_Number is null and firstheld_TS is not null then 'DeliveryAttempted'
    when ShipmentWHHandoverAt is not null and HTCreatedAt is not null and OutBound_Manifest_Number is null and UnreachableTS is not null then 'DeliveryAttempted'
    when ShipmentWHHandoverAt is not null and HTCreatedAt is not null and OutBound_Manifest_Number is null and firstheld_TS is null then 'DeliveryPending'
    when OutBound_Manifest_Number is not null and DeliveredAt < EddStart then 'DeliveredBeforeSlot'
    when OutBound_Manifest_Number is not null and DeliveredAt >=EDDStart and DeliveredAt <=EddEnd then 'DeliveredInSlot'
    when OutBound_Manifest_Number is not null and date(firstheld_TS) <= date(EDD) then 'AttemptedWithInslot'
    when OutBound_Manifest_Number is not null and DeliveredAt > EddEnd  then 'DelBreach'
end as DeliveryAdherenceCheck from(
select *,
case 
     when country = 'AE' and warehouseCode <> "W00017252A" and (SlotTimeGST = '5am - 9am' or SlotTimeGST = '7am - 11am') then timestamp_add(EDDStart, interval -90 MINUTE)---GMM
     when country = 'AE' and warehouseCode = "W00017252A" and (SlotTimeGST = '5am - 9am' or SlotTimeGST = '7am - 11am') then timestamp_add(EDDStart, interval -60 MINUTE)---GMM
     when country = 'AE' and warehouseCode <> "W00017252A" and (SlotTimeGST = '10am - 2pm' or SlotTimeGST = '10am - 1pm') then timestamp_add(EDDStart, interval -240 MINUTE)
     when country = 'AE' and warehouseCode = "W00017252A" and (SlotTimeGST = '10am - 2pm' or SlotTimeGST = '10am - 1pm') then timestamp_add(EDDStart, interval -180 MINUTE)
     when country = 'AE' and warehouseCode <> "W00017252A" and city <> "Dubai" and  SlotTimeGST = '11am - 7pm' then timestamp_add(EDDStart, interval -270 MINUTE)
     when country = 'AE' and warehouseCode = "W00017252A" and city <> "Dubai" and  SlotTimeGST = '11am - 7pm' then timestamp_add(EDDStart, interval -210 MINUTE)   
     when country = 'AE' and warehouseCode <> "W00017252A" and (SlotTimeGST = '12pm - 8pm' or SlotTimeGST = '2pm - 5pm') then timestamp_add(EDDStart, interval -210 MINUTE)
     when country = 'AE' and warehouseCode = "W00017252A" and (SlotTimeGST = '12pm - 8pm' or SlotTimeGST = '2pm - 5pm') then timestamp_add(EDDStart, interval -150 MINUTE) 
     when country = 'AE' and warehouseCode <> "W00017252A" and city <> "Dubai" and  SlotTimeGST = '3pm - 9pm' then timestamp_add(EDDStart, interval -300 MINUTE)
     when country = 'AE' and warehouseCode = "W00017252A" and city <> "Dubai" and  SlotTimeGST = '3pm - 9pm' then timestamp_add(EDDStart, interval -240 MINUTE)
     when country = 'AE' and warehouseCode <> "W00017252A" and city <> "Dubai" and  SlotTimeGST = '9am - 3pm' then timestamp_add(EDDStart, interval -150 MINUTE)
     when country = 'AE' and warehouseCode = "W00017252A" and city <> "Dubai" and  SlotTimeGST = '9am - 3pm' then timestamp_add(EDDStart, interval -270 MINUTE)    
     when country = 'AE' and warehouseCode <> "W00017252A" and (SlotTimeGST = '4pm - 9pm' or SlotTimeGST = '6pm - 9pm') then timestamp_add(EDDStart, interval -210 MINUTE)
     when country = 'AE' and warehouseCode = "W00017252A" and (SlotTimeGST = '4pm - 9pm' or SlotTimeGST = '6pm - 9pm') then timestamp_add(EDDStart, interval -150 MINUTE)   
     when country = 'AE' then timestamp_add(EDDStart, interval -4 HOUR)
     when country = 'SA' and SlotTimeGST = '5pm - 9pm' then timestamp_add(EDDStart, interval -2 HOUR) --- KSA SDD Cutoff
     when country = 'SA' then timestamp_add(EDDStart, interval -4 HOUR) --- KSA REST Cutoff
end as cut_off,
case when CancellationReason like '%Out of Stock%' then 2
      when itemFinalStatus in ('new', 'invalid','cancelled', 'undeliverable') then 0
      else 1 end ItemFinalCategory
,case
when itemFinalStatus in ('new', 'invalid','cancelled', 'undeliverable') then "UD"
when FMpgroup is not null then "GB_Created"
when ShipmentWHReceivedAt is null then "Handover_Pending"
when ( FM_HTcreationTS is null and FMPgroupcreationTS is null) and Tote_nr is null then "HTPending"
when ( FMHT_unpack_TS is null and FMPgroupunpack_TS is null) and Tote_nr is null  then "HTUnpack_Pending"
when Tote_nr is null then "Putaway_Pending"
when BoxCreatedAt is null then "GBCreationPending"
when DeliveredAt is not null then "Delivered"
else "Check" end as FMRemark      
from
(select *, timestamp_add(EDD,interval SlotStartHourGST hour) EDDStart, 
timestamp_add(EDD,interval SlotEndHourGST hour) EDDEnd from `noonbilogoi.SA.ShipmentTracker` 
where date(EDD) = date(timestamp_add(current_timestamp(),interval 1 hour))
and  instant_order=0
)))))
WHERE ItemFinalStatus IN ('delivered','shipped','undeliverable')
GROUP BY 1,2,3,4,5,6,7,8
) T
LEFT JOIN `noonbilogmis.reporting.ls_ds_gs` ff on t.EDD = ff.Date
LEFT JOIN
(
select a.delivery_date, c.code, c.name slot1, ct.country_code country,ct.name_en city, sum(a.Capacity) capacity
from `noonltddwh.dsa.city_slot_capacity_date` as a
left join (select distinct id_delivery_time_slot, is_active from noonltddwh.dsa.daily_service_area_slot_new) as b on b.id_delivery_time_slot=a.id_delivery_time_slot
left join noonltddwh.ref.delivery_time_slot as c on a.id_delivery_time_slot=c.id_delivery_time_slot
left join noonltddwh.ref.city ct using (id_city)
where b.is_active=1
and c.is_sdd =0
and concat(c.code, "-", ct.name_en) not in ("UL-Abu Dhabi", "UH-Dubai")
and delivery_date >= current_date -3 and delivery_date <= current_date 
group by 1,2,3,4,5
union all
SELECT
delivery_date,'GMM' as code,'5am - 9am' as slot, 'AE' as country, 'Dubai' as city, 600 as capacity
FROM UNNEST(GENERATE_DATE_ARRAY(current_date-3, current_date)) AS delivery_date
ORDER BY delivery_date
) sl on sl.slot1 = t.slot and t.EDD = sl.delivery_date and sl.city = T.city 
'''


[[matview]]
dag_id = 'NP_DAILY'
task_id = 'rtv_poa'
destination_table = 'noonbilogmis.reporting.rtv_POA'
sql = '''
SELECT 
T.*,
ll.distance_m,
substring(POA ,71,10) AS POA_Uploaded_date
FROM (select distinct 
case when cz.id_country =1 then "AE"
     when cz.id_country =2 then "SA"
     when cz.id_country =3 then "EG"
     else "Check"
     end as Country, 
      hub.code as Hub,
awb_nr ,
REPLACE (concat("https://storage.cloud.google.",substr(json_extract(data,'$.media_urls'), 30,1000)), '"]', "") as POA
from `noonltddwh.lms.item` item
left join `noonltddwh.lms.item_pkg` using (id_item)
left join `noonltddwh.lms.creq_leg` cll using (id_creq_leg)
left join `noonltddwh.lms.country_zone` cz using(id_country_zone)
left join `noonltddwh.lms.creq` c using (id_creq)
left join `noonltddwh.lms.hub_sector` hs using (id_hub_sector)
left join `noonltddwh.lms.hub` hub using(id_hub)
left join `noonltddwh.lms.address`  dst_address on dst_address.id_address = cll.id_address 
left join `noonltddwh.lms.address_type` add_type using (id_address_type)
left join (select * from `noonltddwh.lms.creq_leg` where id_creq_leg_type =3) cl on c.id_creq=cl.id_creq
left join `noonltddwh.lms.creq_leg_history` clh on clh.id_creq_leg =cl.id_creq_leg 
where json_EXTRACT(clh.data,'$.media_urls') is not null
and id_creq_type =3
and add_type.id_address_type = 7
)T left join `noonbilogmis.reporting.RTV_longitude_latitude` ll on (ll.awb_nr,date(ll.attempt_date))   = (T.awb_nr,date(substring(POA ,71,10)) )
'''

[[matview]]
dag_id = 'NP_mtd'
task_id = 'rtv_poa_old1'
destination_table = 'noonbilogmis.reporting.rtv_poa_old1'
sql = '''
SELECT 
*,
substring(POA ,71,10) AS POA_Uploaded_date
FROM (select distinct 
case when cz.id_country =1 then "AE"
     when cz.id_country =2 then "SA"
     when cz.id_country =3 then "EG"
     else "Check"
     end as Country, 
      hub.code as Hub,
awb_nr AS AWB ,
REPLACE (concat("https://storage.cloud.google.",substr(json_extract(data,'$.media_urls'), 30,1000)), '"]', "") as POA
from `noonltddwh.lms.item` item
left join `noonltddwh.lms.item_pkg` using (id_item)
left join `noonltddwh.lms.creq_leg` using (id_creq_leg)
left join `noonltddwh.lms.country_zone` cz using(id_country_zone)
left join `noonltddwh.lms.creq` c using (id_creq)
left join `noonltddwh.lms.hub_sector` hs using (id_hub_sector)
left join `noonltddwh.lms.hub` hub using(id_hub)
left join (select * from `noonltddwh.lms.creq_leg` where id_creq_leg_type =3) cl on c.id_creq=cl.id_creq
left join `noonltddwh.lms.creq_leg_history` clh on clh.id_creq_leg =cl.id_creq_leg 
where json_EXTRACT(clh.data,'$.media_urls') is not null
and id_creq_type =3
)
'''


[[authview]]
dag_id = 'logmis'
task_id = 'NE_branchcode_updates_cir'
destination_table = 'noonbilogmis.reporting.NE_branchcode_updates_cir'
sql = '''
SELECT rc.* , ifs.status , ifs.current_location , ifs.loc_type
FROM `noonbilogoi.UAE_data.return_check` rc
left join `noonbilogoi.battleplan_v1.item_final_state` ifs on rc.return_nr = client_ref
'''

[[authview]]
dag_id = 'logmis'
task_id = 'NE_branchcode_updates_all'
destination_table = 'noonbilogmis.reporting.NE_branchcode_updates_all'
sql = '''
select * from `noonbilogoi.RG.hview`
'''

[[authview]]
dag_id = 'logmis'
task_id = 'Shipped_not_delivered_ss'
destination_table = 'noonbilogmis.reporting.Shipped_not_delivered_ss'
sql = '''
SELECT * FROM `noonbilogoi.RG.RGDeliveryValidationV2`
'''







[[authview]]
dag_id = 'logmis'
task_id = 'warranty_branchcode_updates'
destination_table = 'noonbilogmis.reporting.warranty_branchcode_updates'
sql = '''
select * from `noonbilogoi.nefreelancer.warranty_base`
'''









[[authview]]
dag_id = 'logmis'
task_id = 'Neeraj_3PL_attempt'
destination_table = 'noonbilogmis.reporting.AlmSxlmAtt'
sql = '''
WITH d AS(
SELECT  WaybillNumber,UpdateDateTime,ROW_NUMBER() OVER (PARTITION BY WaybillNumber ORDER BY UpdateDateTime) RW 
FROM 
(
SELECT distinct WaybillNumber,UpdateDateTime
FROM (
SELECT WaybillNumber,UpdateDateTime
FROM (
SELECT WaybillNumber,DATE(UpdateDateTime) UpdateDateTime,MAX(UpdateDateTime) MAX_UpdateDateTime FROM `noonbilogoi.VJEXP.ALM` where  UpdateDescription IN  ('Out for Delivery - Partial','Out for Delivery') GROUP BY 1,2)
UNION ALL
SELECT WaybillNumber,UpdateDateTime
FROM (
SELECT AWB WaybillNumber,DATE(updated_at) UpdateDateTime,MAX(updated_at) MAX_UpdateDateTime FROM `noonbilogoi.VJEXP.SXLM` where  status IN  ('Out for Delivery ','OUT FOR DELIVERY ') GROUP BY 1,2) 
UNION ALL
SELECT WaybillNumber,UpdateDateTime
FROM 
(
SELECT reference_nr WaybillNumber,DATE(timestamp_at) UpdateDateTime,MAX(timestamp_at)MAX_UpdateDateTime
FROM noonbilogmis.reporting.3PL_LMS_API 
WHERE EventNameEN IN ('Out for Delivery ','OUT FOR DELIVERY ','Out for Delivery - Partial','Out for Delivery',' Unsuccessful (physical) delivery','delivered','Unsuccessful (physical) delivery','600',' out for physical delivery' ) 
OR (carreir = 'smsa' AND EventNameEN IN ('AWAITING CONSIGNEE FOR COLLECTION','PROOF OF DELIVERY CAPTURED','Call Attempt - Consignee No Response','OUT FOR DELIVERY ','DELIVERED','CONSIGNEE ADDRESS CHANGED','CONSIGNEE MOBILE OFF','CUSTOMER REQUESTED FUTURE DELIVERY','SHIPMENT REFUSE BY RECIPIENT','COLLECTED FROM RETAIL ','CONTACT NO. NOT BELONGS TO CONSIGNEE','OUT FOR DELIVERY','COLLECTED FROM RETAIL','FUTURE DELIVERY REQUEST','CONSIGNEE NO RESPONSE','INCORRECT DELIVERY ADDRESS','INCORRECT ADDRESS'))
OR (carreir = 'saudi_post' AND EventNameEN IN ('Your shipment is out for delivery',' Your shipment is out for delivery','Your address is incomplete','Attempted to delivery','Your shipment is ready for pickup (Office name)','Your shipment is delivered, Thank you for choosing us',' out for physical delivery','delivery Attempted','delivered') and EventNameEN not in ('Departed to','SMS Sent'))
OR (carreir = 'saudi_post_lc' AND EventNameEN IN ('Your shipment is out for delivery',' Your shipment is out for delivery','Your address is incomplete','Attempted to delivery','Your shipment is ready for pickup (Office name)','Your shipment is delivered, Thank you for choosing us',' out for physical delivery','delivery Attempted','delivered') and EventNameEN not in ('Departed to','SMS Sent'))
OR (carreir = 'aramex' AND EventNameEN IN ('Out for Delivery - Partial','Customer Contact Attempts Completed - Pending Return to Shipper','Out for Delivery','Customer contact Attempts Completed',"Credit Card Payment - Completed","Customs' Documents Out for Delivery",'SMS Sent to Consignee for the shipper updates description','Redirected to New Delivery Address','SMS Sent to Consignee','Collected by Consignee','Delivery Address Corrected','Delivered'))
OR (carreir = 'imile' AND EventNameEN IN ('PR','600'))
OR (carreir = 'jt_express' and EventCategoryName_En in ('Delivery scan','Sign scan','Signing scan'))
GROUP BY 1,2
)
UNION ALL
SELECT *
FROM (
SELECT DISTINCT ItemBarcode AS WaybillNumber,
DATE(CAST(LEFT(CreationDate,4) AS INT64),CAST(substr(CreationDate,6,2) AS INT64),CAST(substr(CreationDate,9,2)AS INT64))
UpdateDateTime
FROM `noonbilogmis.reporting.saudiPost` 
where EventNameEN IN (' Unsuccessful (physical) delivery','delivered')
)
UNION ALL
SELECT *
FROM(
SELECT tpl_carrier_reference.reference_nr awb_nr ,MIN(DATE(soi.updated_at)) FA_date
FROM `noondwh.lms.item` item
LEFT JOIN noondwh.lms.tpl_item_pgroup_map tpl_item_pgroup_map ON tpl_item_pgroup_map.id_item_pgroup = item.id_item
LEFT JOIN noondwh.lms.item tpl_item ON tpl_item_pgroup_map.id_item = tpl_item.id_item
left join `noondwh.lms.tpl_carrier_reference` as tpl_carrier_reference on tpl_carrier_reference.id_item =tpl_item_pgroup_map.id_item_pgroup
left join noondwh.lms.item_pkg ip on ip.id_item = tpl_item.id_item
left join noondwh.lms.carrier c using(id_carrier)
left join noondwh.sales.sales_order_item_shipment sois on sois.awb_nr = tpl_item.awb_nr
left join noondwh.sales.sales_order_item soi using(id_sales_order_item_shipment)
left join noondwh.sales.sales_order so on so.id_sales_order = sois.id_sales_order
WHERE tpl_carrier_reference.reference_nr  IS NOT NULL
AND id_sales_order_item_status IN (6)
GROUP BY 1
)
)
)
)
SELECT T1.*,T2.attempt2, T3.attempt3,T4.MAX_attempt,t5.last_attempt
FROM (SELECT WaybillNumber,UpdateDateTime attempt1 FROM d WHERE RW = 1
) T1
LEFT JOIN
(SELECT WaybillNumber,UpdateDateTime attempt2 FROM d WHERE RW = 2) T2 ON T1.WaybillNumber = T2.WaybillNumber
LEFT JOIN
(SELECT WaybillNumber,UpdateDateTime attempt3 FROM d WHERE RW = 3) T3 ON T1.WaybillNumber = T3.WaybillNumber
LEFT JOIN
(SELECT WaybillNumber,MAX(RW) MAX_attempt FROM d GROUP BY 1) T4 ON T1.WaybillNumber = T4.WaybillNumber
LEFT JOIN
(SELECT d.WaybillNumber,d.UpdateDateTime last_attempt FROM (SELECT WaybillNumber,MAX(RW) MAX_attempt FROM d  GROUP BY 1)t1 INNER JOIN d  on d.WaybillNumber = t1.WaybillNumber and t1.MAX_attempt = d.rw) T5 ON T1.WaybillNumber = T5.WaybillNumber
'''


[[authview]]
dag_id = 'logmis'
task_id = 'FA_TO_RTO_RAW_WEEKLY_TEMP'
destination_table = 'noonbilogmis.reporting.FA_To_RTO_Raw_Weekly'
sql = '''
SELECT ofd.hub , RTO_LegDate, T.created_at , ie.awb_nr ,
case 
when display_name = 'NOON/CAI01' then display_name 
when display_name = 'NOON/CAI02' then display_name 
when display_name = 'NOON/CAI03' then display_name 
when display_name = 'NOON/CAI04' then display_name 
when display_name = 'NOON/CAIBK01' then display_name 
when display_name = 'NOON/CAIXDOCK03' then display_name 
when display_name IN ('Ahmed Teilab',	'Bakier- Maadi Store',	'Brand House.',	'Dawaa Pharmacy',	'Decathlon',	'Dokkan Tech',	'el captain store',	'EL MANAR',	'El Mesery-1st Warehouse Drop Off...',	'ELSERAG',	'Fast Print',	'Heaven Warehouse',	'Hedeya Office',	'hero.4',	'Joxy Brand',	'M.Esmat',	'null',	'Omega',	'Pretty-woman',	'sanmark',	'Tal2aaStore',	'Techno Trade Group',	'USA Outlet')  then 'DS' end AS WH_type
FROM (SELECT id_item, MIN(d.created_at) created_at 
FROM `noonltddwh.lms.session_field_item` d GROUP BY 1) T 
INNER JOIN `noonltddwh.lms.session_field_item` sfi on T.id_item = sfi.id_item and T.created_at = sfi.created_at 
INNER JOIN noonltddwh.lms.item ie ON sfi.id_item = ie.id_item 
INNER JOIN `noonbilogmis.reporting.OFD_Forward` ofd on ie.awb_nr = ofd.awb_nr and DATE(sfi.created_at ) = ofd.ofd_date 
INNER JOIN `noonbilogmis.reporting.EG_Pending_Data` pd on ofd.awb_nr = pd.awb_nr 
INNER JOIN `noonbilogoi.battleplan_v1.item_final_state` ifs on ifs.awb_nr = pd.awb_nr 
WHERE DATE(ofd.ofd_date ) >= current_Date -8 and DATE(ofd.ofd_date) <= current_Date -1 and pd.RTO_LegDate IS NOT NULL
'''


[[authview]]
dag_id = 'logmis'
task_id = 'Billing_Report_Z3'
destination_table = 'noonbilogmis.reporting.Billing_Report_Z3'
sql = '''
SELECT distinct
-- epd.Shipping_TS Del_Date,
case when soish.id_sales_order_item_status = 6 then soish.created_at end as Del_date_time,
case when soish.id_sales_order_item_status = 8 then epd.location_entity end as Return_MF,
sois.awb_nr as AWB_Nr,
ofd.username as Username,
Hub_Name
,soish.id_sales_order_item_status
,ofd.hub As Destination_hub
,case when soish.id_sales_order_item_status = 6 then "Delivered"
when soish.id_sales_order_item_status = 8 then "RTO"
when soish.id_sales_order_item_status = 5 then "RTO Converted" end as Status
,MAX(soi.estimated_delivery_at ) PDD
,epd.RTO_LegDate AS RTO_Convert_TS
,Case WHEN soish.id_sales_order_item_status = 5 then null else soish.created_at end as Returned_TS
,Manifested_date
,case when hub_name ='ALY-A1' then 'Zone_2' else 'Zone_1' end as Zone
FROM noonltddwh.sales.sales_order_item soi
LEFT JOIN noonltddwh.sales.sales_order soii using(id_sales_order)
LEFT JOIN `noonltddwh.sales.sales_order_item_shipment` sois using(id_sales_order_item_shipment)
left join `noonbilogmis.reporting.EG_Pending_Data` epd on epd.awb_nr = sois.awb_nr
LEFT JOIN `noonltddwh.lms.client_pkg_ref` so on client_pkg_ref = sois.shipment_nr
LEFT JOIN `noonltddwh.sales.sales_order_item_status_history` soish using(id_sales_order_item)
left join `noonltddwh.ref.sales_order_item_status` status ON status.id_sales_order_item_status = soish.id_sales_order_item_status
left join (
SELECT awb_nr , hub.code as Hub_Name
from `noonltddwh.lms.client_pkg_ref` client_pkg_ref
LEFT JOIN(select * from `noonltddwh.lms.creq_leg` where id_creq_leg_type=3) as creq_leg using (id_creq)
LEFT JOIN `noonltddwh.lms.hub_sector` hub_sector on hub_sector.id_hub_sector=creq_leg.id_hub_sector
LEFT JOIN `noonltddwh.lms.hub` hub on hub_sector.id_hub=hub.id_hub
) Hubr ON Hubr.awb_nr = sois.awb_nr
left join
(SELECT d.*
From
(SELECT awb_nr , MAX(OFD_Date) ofd FROM `noonbilogmis.reporting.OFD` group by 1) b
INNER JOIN `noonbilogmis.reporting.OFD` d on (d.awb_nr,d.OFD_Date)= (b.awb_nr ,b.ofd)
)
ofd on ofd.awb_nr = sois.awb_nr
left join (Select i.awb_nr , MAX(timestamp_add(ish.created_at,interval 2 hour) ) Manifested_date from `noonltddwh.lms.item_state_history` ish 
left join `noonltddwh.lms.item` i using(id_item)
where ish.id_loc_type = 6
group by 1
) hhh on hhh.awb_nr = sois.awb_nr 
WHERE ( soish.id_sales_order_item_status IN (6,8) OR (soish.id_sales_order_item_status IN (5) and epd.RTO_LegDate is not null))
and id_cart_type =1
and id_invoice_section =1
and soii.country_code = 'EG'
and ofd.hub like '%-Z3%'
and ofd.ofd_date >= current_date()-60
GROUP BY 1,2,3,4,5,6,7,8,10,11,12
'''







[[authview]]
dag_id = 'logmis'
task_id = 'FA_TO_RTO_RAW_WEEKLY_TEMP12'
destination_table = 'noonbilogmis.reporting.HT_To_FA_Raw_Weekly'
sql = '''
SELECT *
,timestamp_diff(FA_created_at,hta_start_ts,hour) HT_FA
,timestamp_diff(FA_created_at,Received_by_hub,hour) Hub_FA
,timestamp_diff(Received_by_hub,hta_start_ts,hour) HT_Hub
FROM (
select  i.awb_nr , 
case 
when display_name = 'NOON/CAI01' then display_name 
when display_name = 'NOON/CAI02' then display_name 
when display_name = 'NOON/CAI03' then display_name 
when display_name = 'NOON/CAI04' then display_name 
when display_name = 'NOON/CAIBK01' then display_name 
when display_name = 'NOON/CAIXDOCK03' then display_name 
when display_name IN ('Ahmed Teilab',	'Bakier- Maadi Store',	'Brand House.',	'Dawaa Pharmacy',	'Decathlon',	'Dokkan Tech',	'el captain store',	'EL MANAR',	'El Mesery-1st Warehouse Drop Off...',	'ELSERAG',	'Fast Print',	'Heaven Warehouse',	'Hedeya Office',	'hero.4',	'Joxy Brand',	'M.Esmat',	'null',	'Omega',	'Pretty-woman',	'sanmark',	'Tal2aaS
