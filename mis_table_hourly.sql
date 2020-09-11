use common;

CREATE OR REPLACE TABLE mis_table_hr_tmp_new using delta AS WITH 

wt_tmp AS (
-- used to find order weight details -data generally available only for Food orders
SELECT temp.sku, temp.unit_weight_in_gm 
FROM 
(SELECT substring(lc.item_id,33,28) sku, lc.unit_weight_in_gm, row_number() OVER (PARTITION BY substring(lc.item_id,33,28) ORDER BY lc.updated_at DESC) row_num 
  FROM default.ds_sql_ud_logistics_catalogue lc
  ) temp 
WHERE temp.row_num=1 ), 

high_gmv_cancellations as 
---- removing fake gmv
( select 
  order_id, 
  order_status, 
  sum(total_order_sp_paise)/100 as gmv 
  FROM 
  default.ds_sql_seller_order  
  where order_status in ('SELLER_ORDER_BUYER_CANCEL')
  group by 1,2 
  ),


st as
(
-- used to find important order state times like rts etc

select st.order_id, 
min(case when st.new_state = 'SELLER_ORDER_RTS' then st.created_at end) as rts,
max(case when st.new_state = 'SELLER_ORDER_HOLD' then st.created_at end) as hold_date,
max(case when st.new_state = 'SELLER_ORDER_EXPIRED' then st.created_at end) as expired_date,
max(case when (st.new_state = "SELLER_ORDER_BUYER_CANCEL" OR st.new_state = "SELLER_ORDER_SELLER_CANCEL") then st.created_at end) as time

from default.ds_sql_seller_order_state_transition as st
group by st.order_id
),

sgt as 
(
-- used to find important shipment logistics times
select sg.shipment_group_id, 
min(case when sg.new_state = 'SHIPMENT_IN_TRANSIT' then sg.created_at end) as first_shipped,
max(case when sg.new_state = 'SHIPMENT_IN_TRANSIT' then sg.created_at end) as last_shipped,
min(case when sg.new_state = 'SHIPMENT_ATTEMPTED_UNDELIVERED' then sg.created_at end) as first_attempt_undelivered,
max(case when sg.new_state = 'SHIPMENT_ATTEMPTED_UNDELIVERED' then sg.created_at end) as last_attempt_undelivered,
min(case when sg.new_state = 'SHIPMENT_RTO' then sg.created_at end) as first_rto,
max(case when sg.new_state = 'SHIPMENT_RTO' then sg.created_at end) as last_rto,
min(case when sg.new_state = 'SHIPMENT_RTO_DELIVERED' then sg.created_at end) as first_rto_delivered,
max(case when sg.new_state = 'SHIPMENT_RTO_DELIVERED' then sg.created_at end) as last_rto_delivered,
max(case when sg.new_state = 'SHIPMENT_RTO_ABSORBED' then sg.created_at end) as rto_absorbed,
max(case when sg.new_state = 'SHIPMENT_RTO_TO_SELLER' then sg.created_at end) as rto_to_seller,
max(case when sg.new_state = 'SHIPMENT_DELIVERED' then sg.created_at end) as delivered

from default.ds_sql_shipment_group_state_transition as sg
group by sg.shipment_group_id

),


tl AS (
-- Used to find tax inmformation of order
SELECT tax_line_id, taxable_entity_id, tax_amount_paise, current_active 
FROM default.ds_sql_tax_line WHERE current_active = 1), 


fli1 AS (
-- used for fulfillment data - used to find order location details
SELECT fulfillment_line_items_id, order_line_id, fulfillment_id, shipment_group_id, status, created_at,
row_number() OVER (PARTITION BY order_line_id ORDER BY fulfillment_line_items_id desc) AS row_num 
FROM default.ds_sql_fulfillment_line_items 
WHERE current_active = 1), 

fli AS (SELECT * FROM fli1 WHERE row_num=1), 


sg1 AS (
-- used for fulfillment data - used to find order shipment/invoice data
SELECT Shg.Shipment_group_id AS shipment_group_id, shg.shipment_status AS shipment_status, shg.awb_number AS awb_number, shg.shipment_type AS shipment_type, inv.created_at as invoice_date,
invs.invoice_id AS invoice_id, inv.invoice_ref_id AS invoice_ref_id, inv.amount_in_paisa AS amount, inv.num_of_items AS num_of_items, inv.invoice_type AS invoice_type, 
row_number() OVER (PARTITION BY shg.shipment_group_id ORDER BY inv.amount_in_paisa desc) AS row_num 

FROM default.ds_sql_shipment_group AS shg 
LEFT JOIN default.ds_sql_invoice_shipment_group_association AS invs ON shg.shipment_group_id = invs.shipment_group_id 
LEFT JOIN default.ds_sql_invoice AS inv ON invs.invoice_id = inv.invoice_id 
WHERE shg.current_active = 1 AND invs.current_active = 1 AND inv.current_active = 1 AND inv.invoice_type = "GOODS"), 

sg AS (SELECT * FROM sg1 WHERE row_num = 1), 


log AS (
-- used to find order logistics charges/amount details
select tb2.shipment_group_id as shipment_group_id, tb2.created_at as created_at, tb2.logistics_amount as logistics_amount, tb2.buyer_inv as buyer_inv 
from
(select tb.*, row_number() over(partition by tb.shipment_group_id order by tb.created_at desc) as rno
 from
 (SELECT sg.shipment_group_id AS shipment_group_id, sg.created_at, sum(inv.amount_in_paisa)/100 AS logistics_amount, inv.buyer_org_id as buyer_inv

  FROM default.ds_sql_shipment_group sg 
  LEFT JOIN default.ds_sql_invoice_shipment_group_association isa ON sg.shipment_group_id=isa.shipment_group_id 
  LEFT JOIN  default.ds_sql_invoice inv ON inv.invoice_id=isa.invoice_id 
  WHERE inv.invoice_type='LOGISTIC_SERVICE' AND sg.current_active=1 
  GROUP BY sg.shipment_group_id, sg.created_at, inv.buyer_org_id) as tb) as tb2
where tb2.rno = 1
), 

temp_org_unit AS (
-- used to find order location/address details
SELECT sou.org_unit_id unit_id, sou.unit_name  org_unit_name, concat(get_json_object(sou.unit_address,'$.address_line1'),",",get_json_object(sou.unit_address,'$.address_line2'),",",
  get_json_object(sou.unit_address,'$.address_line3'),",",get_json_object(sou.unit_address,'$.city'),",",get_json_object(sou.unit_address,'$.state'),",",
  get_json_object(sou.unit_address,'$.pincode')) AS address, get_json_object(sou.unit_address,'$.city') city, get_json_object(sou.unit_address,'$.state') state, 
get_json_object(sou.unit_address,'$.pincode') pincode 

FROM default.ds_sql_org_units sou), 

fulfillment_table AS ( SELECT temp.fulfillment_center, temp.seller_order_id,temp.shipfrom_address_id,temp.shipto_address_id 
  FROM 
  (SELECT fulfillment_center, seller_order_id, updated_at,shipfrom_address_id,shipto_address_id,
    row_number() OVER (PARTITION BY seller_order_id ORDER BY updated_at desc) AS row_num 
    FROM default.ds_sql_fulfillment_line
    WHERE current_active = 1) as temp 
  WHERE temp.row_num=1 ),

tempToken AS ( SELECT DISTINCT token_ref_id, "YES" AS token_taken1 FROM default.ds_sql_token_amount_expectation),
tpc as (
-- used to filter fake orders and find prepaid orders
SELECT st.order_id  FROM default.ds_sql_seller_order_state_transition as st where st.new_state = 'SELLER_ORDER_PAYMENT_OPEN' group by st.order_id )


SELECT 
distinct sol.seller_order_id AS Order_ID,
sol.order_line_id AS Order_line_ID, 
so.order_status AS Order_Status,  
cast(from_unixtime(cast(unix_timestamp(sol.created_at)+19800 as bigint),'yyyy-MM-dd HH:mm:ss') as timestamp) as order_line_created_at,
cast(from_unixtime(cast(unix_timestamp(sol.created_at)+19800 as bigint),'yyyy-MM-dd') as timestamp) as order_line_created_date,                         
cast(from_unixtime(cast(unix_timestamp(so.created_at)+19800 as bigint),'yyyy-MM-dd') as timestamp)AS Order_date, 

CASE WHEN from_unixtime(cast(unix_timestamp(so.created_at)+19800 as bigint),'yyyy-MM-dd') = '2018-12-31' THEN '2019/1'
WHEN from_unixtime(cast(unix_timestamp(so.created_at)+19800 as bigint),'yyyy-MM-dd') = '2019-12-30' THEN '2019/53'
WHEN from_unixtime(cast(unix_timestamp(so.created_at)+19800 as bigint),'yyyy-MM-dd') = '2019-12-31' THEN '2019/53'
ELSE CONCAT(YEAR(from_unixtime(cast(unix_timestamp(so.created_at)+19800 as bigint),'yyyy-MM-dd')),"/",
  WEEKOFYEAR(from_unixtime(cast(unix_timestamp(so.created_at)+19800 as bigint),'yyyy-MM-dd'))) END AS order_wk, 


from_unixtime(cast(unix_timestamp(so.created_at)+19800 as bigint),'yyyy/MM') AS order_month, 

sol.order_line_status AS order_line_Status, 

-- indicates if it is a forward order or exchange order
so.order_type AS Order_type, 

-- buyer details are the buyer head office location details and need not be order location details
so.buyer_id AS Buyer_org_id, og1.org_name AS Buyer_org_name, og1.city AS buyer_city, 
og1.state AS buyer_state, og1.pincode AS buyer_pincode, og1.owner_phone AS buyer_phone, 

--seller details are the seller head office location details and need not be order location details
so.seller_id AS Seller_org_id, og2.org_name AS Seller_org_name, og2.city AS Seller_city, 
og2.state AS Seller_state, og2.pincode AS Seller_pincode, og2.owner_phone AS seller_phone, 

sol.listing_id AS Listing_ID, list.title AS listing_title, sol.sales_unit_id AS SKU_ID, 

sol.units AS unit_qty, sol.per_unit_sp_paise/100 AS per_unit_price, list.tax_details AS tax,

sol.order_line_sp_paise/100 AS order_line_amount, tl.tax_amount_paise/100 AS tax_line_amount, 
CASE WHEN tl.tax_amount_paise is null THEN sol.order_line_sp_paise/100 ELSE (sol.order_line_sp_paise/100 + tl.tax_amount_paise/100) END AS total_line_amount, 

-- category of order mapped using listing id which has been mapped to a category
list.vertical AS vertical, list.sub_category AS sub_category, 
CASE WHEN list.category is null THEN scmp.category ELSE list.category END AS category,
CASE WHEN list.category is null THEN scmp.business_unit ELSE list.business_unit END AS business_unit, 
-- listing details
list.brand AS brand, list.model_name as model_name,
list.vertical_spec AS vertical_spec,
list.ideal_for ideal_for, 
get_json_object(so.extra_data,'$.selected_payment_method') AS payment_mode, 
cast(from_unixtime(cast(unix_timestamp(so.created_at)+19800 as bigint),'yyyy-MM-dd HH:mm:ss') as timestamp) AS order_date_time,

-- listing created_at in bigint so not changed
list.created_at AS listing_created_at, 

-- include removes fake orders, terminated orders
CASE WHEN so.order_type = "EXCHANGE" THEN "NO" 
WHEN og1.org_name LIKE '%HIVELOOP TECHNOLOGY%' THEN "NO" 
WHEN og1.org_name LIKE '%Udaan%' THEN "NO" 
WHEN og2.org_name LIKE '%Udaan%' THEN "NO" 
WHEN og1.org_name LIKE '%Test-VG%' THEN "NO" 
WHEN og2.org_name LIKE '%Test-VG%' THEN "NO" 
WHEN do.type LIKE '%Dummy%' THEN "NO" 
WHEN so.seller_id in ('ORG0XY5NJ9W1FCVCF2LERL6V5444E','ORG10RL5ZNSV5DCTFD9TK3TS3VVQW','ORGGMLRXELRE5C3FFEDD2XEL436SF','ORGFQEFKNZ3RV8LW43459MMDEVGRM') THEN 'NO'
WHEN so.buyer_id in ('ORG1BM4G86NG2Q3E4BPVZHEHBM8C8') THEN 'NO'
WHEN sol.seller_order_id in ("ODDBY464180PCW","ODDFQ154953R5B","OD1KX1549532DY","ODN0D155038PCM","ODP345513733DG","ODPZY20939811R","OD1FK155592XWN","ODQ5C155145VFX","ODZ2V1555685F4","ODDP1155559VK3","OD9X4552460XTK","ODD5M578616XDT","OD7N4556887XJ7","ODVEJ155809HQC","ODYMY801805MG1","ODQDK1558711Q6","ODLEE1558711Q6","ODE4B716902XNC") THEN 'NO'
WHEN so.order_status = 'SELLER_ORDER_TERMINATED' AND from_unixtime(cast(unix_timestamp(so.created_at)+19800 as bigint),'yyyy-MM-dd') >= '2019-02-01' THEN 'NO'
WHEN tpc.order_id IS NOT NULL AND so.order_status in ('SELLER_ORDER_BUYER_CANCEL') THEN 'NO'
WHEN fpo.org_id is not null and from_unixtime(cast(unix_timestamp(so.created_at)+19800 as bigint),'yyyy-MM-dd') >= '2019-05-01'  
AND sol.seller_order_id not in ('ODF6M5671501LN','OD6MR672370KLG','ODD1P672364ES5','OD90Z723680854','ODVB2723748KWT','ODZCH156723GBX','OD7XK7237202ES','ODF6M5672366B4','ODXZC567237B4T') then 'NO'
WHEN so.order_status in ('SELLER_ORDER_PAYMENT_OPEN') THEN 'NO'
WHEN list.category = 'Fullfilment Material' then 'NO'
WHEN hgc.order_id is not null and hgc.gmv > 1000000 then 'NO'
END AS include, 

-- GMV new Defination
CASE 
WHEN sol.order_line_status not in ("SELLER_ORDER_LINE_DRAFT","SELLER_ORDER_LINE_RESERVED") THEN 
CASE WHEN tl.tax_amount_paise is null THEN sol.order_line_sp_paise/100 
ELSE (sol.order_line_sp_paise/100 + tl.tax_amount_paise/100) END
ELSE 0
END AS new_total_line_amount, 

CASE WHEN tpc.order_id IS NOT NULL THEN 'PREPAYMENT' else NULL end as prepayment_tag,

-- Tagging buyer as HORECA, Supermart etc, tag sellers as distribution sellers 
bm.type as buyer_tag, CASE WHEN cmp.type LIKE '%cohort%' THEN cmp.type ELSE do.type END as order_tag, 
sm.type as seller_tag, CASE WHEN sm.distribution="Distribution" THEN "Distribution" END as distribution, 

-- fulfillment/shipment information
fli.fulfillment_line_items_id AS fulfillment_line_items_id, 
fli.fulfillment_id AS fulfillment_id, fli.shipment_group_id AS shipment_group_id, sg.shipment_status AS shipment_status, 
sg.awb_number AS awb_number, sg.invoice_ref_id AS seller_invoice_id, 

sg.amount/100 as sg_amount, sg.num_of_items as num_of_items,

-- invoice date
from_unixtime(cast(unix_timestamp(sg.invoice_date)+19800 as bigint),'yyyy-MM-dd HH:mm:ss') as invoice_date ,

-- Below lines contain shipment logistics information
-- st.rts AS rts,
from_unixtime(cast(unix_timestamp(st.rts)+19800 as bigint),'yyyy-MM-dd HH:mm:ss') AS rts, 

-- sgt.first_shipped AS first_shipped, 
from_unixtime(cast(unix_timestamp(sgt.first_shipped)+19800 as bigint),'yyyy-MM-dd HH:mm:ss') AS first_shipped, 


-- sgt.last_shipped AS last_shipped, 
from_unixtime(cast(unix_timestamp(sgt.last_shipped)+19800 as bigint),'yyyy-MM-dd HH:mm:ss') AS last_shipped, 

-- sgt.first_attempt_undelivered AS first_attempt_undelivered,
from_unixtime(cast(unix_timestamp(sgt.first_attempt_undelivered)+19800 as bigint),'yyyy-MM-dd HH:mm:ss') AS first_attempt_undelivered, 


-- sgt.last_attempt_undelivered AS last_attempt_undelivered, 
from_unixtime(cast(unix_timestamp(sgt.last_attempt_undelivered)+19800 as bigint),'yyyy-MM-dd HH:mm:ss') AS last_attempt_undelivered, 


-- sgt.first_rto AS first_rto, 
from_unixtime(cast(unix_timestamp(sgt.first_rto)+19800 as bigint),'yyyy-MM-dd HH:mm:ss') AS first_rto, 


-- sgt.last_rto AS last_rto, 
from_unixtime(cast(unix_timestamp(sgt.last_rto)+19800 as bigint),'yyyy-MM-dd HH:mm:ss') AS last_rto, 



-- sgt.first_rto_delivered AS first_rto_delivered, 
from_unixtime(cast(unix_timestamp(sgt.first_rto_delivered)+19800 as bigint),'yyyy-MM-dd HH:mm:ss') AS first_rto_delivered, 



-- sgt.last_rto_delivered AS last_rto_delivered,
from_unixtime(cast(unix_timestamp(sgt.last_rto_delivered)+19800 as bigint),'yyyy-MM-dd HH:mm:ss') AS last_rto_delivered, 

-- sgt.rto_absorbed AS rto_absorbed, 
from_unixtime(cast(unix_timestamp(sgt.rto_absorbed)+19800 as bigint),'yyyy-MM-dd HH:mm:ss') AS rto_absorbed, 


-- sgt.rto_to_seller AS rto_to_seller, 
from_unixtime(cast(unix_timestamp(sgt.rto_to_seller)+19800 as bigint),'yyyy-MM-dd HH:mm:ss') AS rto_to_seller, 


case when so.order_status = 'SELLER_ORDER_DELIVERED' then from_unixtime(cast(unix_timestamp(sgt.delivered)+19800 as bigint),'yyyy-MM-dd HH:mm:ss') else NULL end AS delivered, 


-- order cancellation data
REGEXP_REPLACE(GET_JSON_OBJECT(so.extra_data,'$.cancel_reason'), '\\r|\\t|\\n', '') AS cancel_reason, 

-- st.time AS cancellation_time, 
from_unixtime(cast(unix_timestamp(st.time)+19800 as bigint),'yyyy-MM-dd HH:mm:ss') AS cancellation_time, 

REGEXP_REPLACE(GET_JSON_OBJECT(so.extra_data,'$.buyer_remarks'), '\\r|\\t|\\n', '') AS buyer_remarks, 

-- log.logistics_amount/shpct.ct AS logistics_charge, 
log.logistics_amount as logistics_amount,

from_unixtime(cast(unix_timestamp(st.time)+19800 as bigint),'yyyy-MM-dd')  AS cancelation_day,  

-- fos information - not used anymore

zm.zone AS zone, 

-- listing price and details as of now
list.unit_price listing_unit_price, list.unit_price_wth_tax listing_unit_price_tax,
wt_tmp.unit_weight_in_gm unit_weight_gms,


dtm.type AS dts_tag, 

-- order placed and delivered location details
tou1.unit_id AS buyer_org_unit_id, tou1.address AS shipto_address, tou1.city AS shipto_city, tou1.state AS shipto_state, tou1.pincode AS shipto_pincode, tou1.org_unit_name AS shipto_org_unit_name, ctit.district shipto_district, 
tou2.unit_id AS seller_org_unit_id, tou2.address AS shipfrom_address, tou2.city AS shipfrom_city, tou2.state AS shipfrom_state, tou2.pincode AS shipfrom_pincode, tou2.org_unit_name AS shipfrom_org_unit_name, 

list.business_group business_group, list.business_type business_type,

CASE WHEN min(so.created_at) over (partition by so.buyer_id order by so.created_at asc) = min(so.created_at) over (partition by so.buyer_id, list.category order by so.created_at asc)
then 'Yes' else 'NO' end as true_category_buyer,


t6c.mapped_city top60_mapped_city, t6c.tag top60_tag, 

-- st.hold_date AS order_hold_date, 
from_unixtime(cast(unix_timestamp(st.hold_date)+19800 as bigint),'yyyy-MM-dd HH:mm:ss') AS order_hold_date, 


-- st.expired_date AS order_expired_date, 
from_unixtime(cast(unix_timestamp(st.expired_date)+19800 as bigint),'yyyy-MM-dd HH:mm:ss') AS order_expired_date, 


CASE WHEN tempToken.token_taken1="YES" THEN "YES" ELSE "NO" END AS token_taken, 
sfl.fulfillment_center AS fulfillment_center,


(CASE WHEN get_json_object(sol.properties, '$.promotion_info.ad_group_id') > 0 THEN 1 ELSE 0 END) AS is_promoted,
get_json_object(sol.properties, '$.promotion_info.ad_group_id') AS ad_group_id,

CASE WHEN so.buyer_id <> log.buyer_inv then 'Yes' else 'No' end as free_shipping,

case when datediff(current_date(),cast(so.created_at as date)) > 44 then 1 else 0 end as partition_date

FROM default.ds_sql_seller_order_line AS sol 
LEFT JOIN default.ds_sql_seller_order as so ON sol.seller_order_id = so.order_id 
LEFT JOIN common.stream_listing AS list ON sol.listing_id = list.listing_id  /* update tmp table to complete table*/
LEFT JOIN tl ON sol.order_line_id = tl.taxable_entity_id 
LEFT JOIN common.org_table_base AS og1 ON so.buyer_id=og1.org_id 
LEFT JOIN common.org_table_base AS og2 ON so.seller_id=og2.org_id 
LEFT JOIN default.ds_csv_order_mapping as do ON sol.seller_order_id = do.order_id 
LEFT JOIN default.ds_csv_buyer_mapping as bm ON so.buyer_id = bm.org_id 
LEFT JOIN default.ds_csv_seller_mapping as sm ON so.seller_id = sm.org_id 
LEFT JOIN fli ON sol.order_line_id = fli.order_line_id 
LEFT JOIN sgt ON fli.shipment_group_id = sgt.shipment_group_id 
LEFT JOIN st on so.order_id = st.order_id
LEFT JOIN sg on fli.shipment_group_id = sg.shipment_group_id
-- LEFT JOIN shpct ON fli.shipment_group_id = shpct.shipment_group_id 
LEFT JOIN common.seller_category_mapping scmp ON so.seller_id=scmp.org_id 
-- LEFT JOIN bulkrep ON bulkrep.order_id=so.order_id 
LEFT JOIN log ON fli.shipment_group_id = log.shipment_group_id 
LEFT JOIN default.ds_csv_cohort_mapping as cmp ON so.buyer_id = cmp.org_id 
-- LEFT JOIN ds_csv_clothing_fos_mapping fsm ON concat(fsm.category,fsm.pincode)=concat(list.category,og1.pincode) 
LEFT JOIN default.ds_csv_zone_mapping zm ON concat(zm.category,zm.state)=concat(list.category,og1.state)  
LEFT JOIN default.ds_csv_dts_mapping dtm ON sol.seller_order_id=dtm.order_id 
LEFT JOIN fulfillment_table sfl ON sfl.seller_order_id=so.order_id 
LEFT JOIN temp_org_unit tou1 ON tou1.unit_id=sfl.shipto_address_id 
LEFT JOIN temp_org_unit tou2 ON tou2.unit_id=sfl.shipfrom_address_id 
LEFT JOIN wt_tmp ON wt_tmp.sku=sol.sales_unit_id 
LEFT JOIN default.ds_csv_top60city t6c ON concat(tou1.state,tou1.city)=concat(t6c.state,t6c.city) 
LEFT JOIN default.ds_csv_city_district_fos_it ctit ON tou1.pincode=ctit.pincode 
LEFT JOIN tempToken ON sol.seller_order_id=tempToken.token_ref_id 
LEFT JOIN tpc ON tpc.order_id = so.order_id
LEFT JOIN default.ds_csv_first_party_orgs as fpo on so.buyer_id = fpo.org_id
LEFT JOIN high_gmv_cancellations as hgc on so.order_id = hgc.order_id 

WHERE so.current_active = 1 AND sol.current_active = 1 AND so.order_status != "SELLER_ORDER_DRAFT" AND so.order_status != "SELLER_ORDER_RESERVED" 
and CAST(so.created_at as DATE) >= date_sub(current_date(), 180)
; 

DELETE FROM common.mis_table1 mis1
WHERE CAST(mis1.order_date_time as DATE) >= date_sub(current_date(), 180);

INSERT INTO common.mis_table1 select * from mis_table_hr_tmp_new;

CREATE OR REPLACE TABLE mis_table_hr_final_temp using delta AS WITH

bulkrep AS (SELECT order_id, sum(total_line_amount) gmv FROM common.mis_table1 GROUP BY order_id), 

overall AS (SELECT buyer_org_id, CONCAT(YEAR(min(order_date_time)),"/",WEEKOFYEAR(min(order_date_time))) AS wk, max(order_date_time) AS dt, min(order_date_time) AS dt_min 
  FROM common.mis_table1 WHERE include is null GROUP BY buyer_org_id), 

buyerseller AS (SELECT concat(buyer_org_id,seller_org_id) AS bs, CONCAT(YEAR(min(order_date_time)),"/",WEEKOFYEAR(min(order_date_time))) AS wk, max(order_date_time) AS dt, min(order_date_time) AS dt_min 
  FROM common.mis_table1 WHERE include is null GROUP BY buyer_org_id,seller_org_id), 

buyercat AS (SELECT concat(buyer_org_id,category) AS bc, CONCAT(YEAR(min(order_date_time)),"/",WEEKOFYEAR(min(order_date_time))) AS wk, max(order_date_time) AS dt, min(order_date_time) AS dt_min 
  FROM common.mis_table1 WHERE include is null  GROUP BY buyer_org_id,category), 

buyerbu AS (SELECT concat(buyer_org_id,business_unit) AS bc, CONCAT(YEAR(min(order_date_time)),"/",WEEKOFYEAR(min(order_date_time))) AS wk, max(order_date_time) AS dt, min(order_date_time) AS dt_min 
  FROM common.mis_table1 WHERE include is null  GROUP BY buyer_org_id, business_unit), 

slroverall AS (SELECT seller_org_id, CONCAT(YEAR(min(order_date_time)),"/",WEEKOFYEAR(min(order_date_time))) AS wk, max(order_date_time) AS dt, min(order_date_time) AS dt_min 
  FROM common.mis_table1 WHERE include is null GROUP BY seller_org_id), 

slrcat AS (SELECT concat(seller_org_id,category) AS bc, CONCAT(YEAR(min(order_date_time)),"/",WEEKOFYEAR(min(order_date_time))) AS wk, max(order_date_time) AS dt, min(order_date_time) AS dt_min 
  FROM common.mis_table1 WHERE include is null  GROUP BY seller_org_id,category), 

slrbu AS (SELECT concat(seller_org_id,business_unit) AS bc, CONCAT(YEAR(min(order_date_time)),"/",WEEKOFYEAR(min(order_date_time))) AS wk, max(order_date_time) AS dt, min(order_date_time) AS dt_min 
  FROM common.mis_table1 WHERE include is null  GROUP BY seller_org_id,business_unit), 

shryj AS (
  select COUNT(inv_amt.order_line_id) OVER (PARTITION BY mp.order_id) AS count ,
  mp.order_id as order_id,
  inv_amt.amount as amount,
  mp.order_line_id as order_line_id,
  inv_amt.order_quantity_unit as order_quantity_unit
  from common.mis_table1 mp
  left join 
  (select 
    sum(il.total_line_price_in_paise)/100 as amount,
    get_json_object(get_json_object(il.metadata,'$.metadata'),'$.orderLineId') as order_line_id,
    sum(get_json_object(il.qty_details,'$.qty')) as order_quantity_unit
    from default.ds_sql_invoice_line as il 
    where 
    get_json_object(get_json_object(il.metadata,'$.metadata'),'$.orderLineId') is not null
    and il.type IN ('TAX_INVOICE','BILL_OF_SUPPLY') and il.current_active = 1 and il.state = "ACTIVE"
    group by get_json_object(get_json_object(il.metadata,'$.metadata'),'$.orderLineId'))  inv_amt 
  on inv_amt.order_line_id = mp.order_line_id),

shpct AS (SELECT shipment_group_id, count(order_line_id) AS ct FROM common.mis_table1 GROUP BY shipment_group_id)



select mp.Order_ID, mp.Order_line_ID, mp.Order_Status,  mp.order_line_created_at, mp.order_line_created_date, mp.Order_date,  mp.order_wk,  
mp.order_month, mp.order_line_Status, mp.Order_type,  mp.Buyer_org_id,  mp.Buyer_org_name,  mp.buyer_city,  mp.buyer_state, mp.buyer_pincode, 
mp.buyer_phone, mp.Seller_org_id, mp.Seller_org_name, mp.Seller_city, mp.Seller_state,  mp.Seller_pincode,  mp.seller_phone,  mp.Listing_ID,  
mp.listing_title, mp.SKU_ID,  mp.unit_qty,mp.per_unit_price,  mp.tax, mp.order_line_amount, mp.tax_line_amount, mp.total_line_amount, 
mp.vertical,  mp.sub_category,mp.category,  mp.business_unit, mp.brand, mp.model_name,  mp.vertical_spec, mp.ideal_for, mp.payment_mode,  
mp.order_date_time,mp.listing_created_at, mp.include, mp.prepayment_tag,  mp.buyer_tag, mp.order_tag, mp.seller_tag,  mp.distribution,  
mp.fulfillment_line_items_id, mp.fulfillment_id,  mp.shipment_group_id, mp.shipment_status, mp.awb_number,  mp.seller_invoice_id, 
mp.sg_amount, mp.num_of_items,  mp.rts, mp.first_shipped, mp.last_shipped,  mp.first_attempt_undelivered, mp.last_attempt_undelivered,
mp.first_rto, mp.last_rto,  mp.first_rto_delivered, mp.last_rto_delivered,  mp.rto_absorbed,  mp.rto_to_seller, mp.delivered, 
mp.cancel_reason, mp.cancellation_time, mp.buyer_remarks, mp.logistics_amount,  mp.cancelation_day, mp.zone,  mp.listing_unit_price,
mp.listing_unit_price_tax,  mp.unit_weight_gms,   mp.dts_tag, mp.buyer_org_unit_id, mp.shipto_address,  mp.shipto_city, mp.shipto_state,
mp.shipto_pincode,  mp.shipto_org_unit_name,  mp.shipto_district, mp.seller_org_unit_id,  mp.shipfrom_address,  mp.shipfrom_city, 
mp.shipfrom_state,  mp.shipfrom_pincode,  mp.shipfrom_org_unit_name,  mp.business_group,  mp.business_type, mp.true_category_buyer, 
mp.top60_mapped_city, mp.top60_tag, mp.order_hold_date, mp.order_expired_date,  mp.token_taken, mp.fulfillment_center,  mp.is_promoted, 
mp.ad_group_id, mp.free_shipping, mp.partition_date,mp.invoice_date,mp.new_total_line_amount,


slroverall.dt_min AS seller_first_order_date, slrcat.dt_min AS seller_first_cat_order_date, overall.dt_min AS buyer_first_order_date, 
buyercat.dt_min AS buyer_first_cat_order_date, buyerbu.dt_min AS buyer_first_BU_order_date, buyerseller.dt_min AS buyerseller_first_order_date, 

overall.dt AS buyer_last_order_date, buyercat.dt AS buyer_last_cat_order_date, buyerbu.dt AS buyer_last_BU_order_date, buyerseller.dt AS buyerseller_last_order_date,
slroverall.dt AS seller_last_order_date, slrcat.dt AS seller_last_cat_order_date, slrbu.dt AS seller_last_bu_order_date, 

mp.sg_amount/shpct.ct AS invoiced_value, 

CASE WHEN shryj.count > 0 THEN
CASE WHEN shryj.amount is null THEN 0 
ELSE shryj.amount END
ELSE mp.sg_amount/shpct.ct end as invoice_amount,

CASE WHEN shryj.count > 0 THEN
CASE WHEN shryj.order_quantity_unit is null THEN 0 
ELSE shryj.order_quantity_unit END
ELSE mp.num_of_items/shpct.ct END AS invoiced_qty,

mp.logistics_amount/shpct.ct AS logistics_charge, 

CASE WHEN overall.wk IS NULL OR overall.wk=CONCAT(YEAR(mp.order_date_time),"/",WEEKOFYEAR(mp.order_date_time)) THEN "New" ELSE "Repeat" END AS buyer_overall_NewRep, 

CASE WHEN buyercat.wk IS NULL OR buyercat.wk=CONCAT(YEAR(mp.order_date_time),"/",WEEKOFYEAR(mp.order_date_time)) THEN "New" ELSE "Repeat" END AS buyer_cat_NewRep, 

CASE WHEN buyerbu.wk IS NULL OR buyerbu.wk=CONCAT(YEAR(mp.order_date_time),"/",WEEKOFYEAR(mp.order_date_time)) THEN "New" ELSE "Repeat" END AS buyer_BU_NewRep, 

CASE WHEN buyerseller.wk IS NULL OR buyerseller.wk=CONCAT(YEAR(mp.order_date_time),"/",WEEKOFYEAR(mp.order_date_time)) THEN "New" ELSE "Repeat" END AS buyer_seller_NewRep, 

CASE WHEN bulkrep.gmv>200000 THEN "Bulk" ELSE "Retail" END AS Bulk_Retail, 

CASE WHEN slroverall.wk=CONCAT(YEAR(mp.order_date_time),"/",WEEKOFYEAR(mp.order_date_time)) THEN "New" ELSE "Repeat" END AS seller_overall_NewRep,

CASE WHEN slrcat.wk=CONCAT(YEAR(mp.order_date_time),"/",WEEKOFYEAR(mp.order_date_time)) THEN "New" ELSE "Repeat" END AS seller_cat_NewRep, 

CASE WHEN slrbu.wk=CONCAT(YEAR(mp.order_date_time),"/",WEEKOFYEAR(mp.order_date_time)) THEN "New" ELSE "Repeat" END AS seller_BU_NewRep,

CASE WHEN overall.dt_min IS NULL OR 
concat(year(overall.dt_min),'/',month(overall.dt_min))  =  concat(year(mp.order_date_time),'/',month(mp.order_date_time)) 

THEN "New" ELSE "Repeat" END AS buyer_month_NewRep,


CASE WHEN buyerseller.dt_min IS NULL OR  concat(year( buyerseller.dt_min),'/',month( buyerseller.dt_min))  =  concat(year(mp.order_date_time),'/',month(mp.order_date_time))  THEN "New" 
ELSE "Repeat" END AS buyer_seller_month_NewRep, 
CASE WHEN buyercat.dt_min IS NULL OR concat(year(buyercat.dt_min),'/',month(buyercat.dt_min))  =  concat(year(mp.order_date_time),'/',month(mp.order_date_time)) THEN "New"
ELSE "Repeat" END AS buyer_cat_month_NewRep,
CASE WHEN buyerbu.dt_min IS NULL OR concat(year(buyerbu.dt_min),'/',month(buyerbu.dt_min))  =  concat(year(mp.order_date_time),'/',month(mp.order_date_time)) THEN "New"
ELSE "Repeat" END AS buyer_BU_month_NewRep 

from common.mis_table1 as mp
LEFT JOIN overall ON overall.buyer_org_id=mp.buyer_org_id
LEFT JOIN buyercat ON buyercat.bc=concat(mp.buyer_org_id,mp.category) 
LEFT JOIN buyerbu ON buyerbu.bc=concat(mp.buyer_org_id,mp.business_unit) 
LEFT JOIN bulkrep ON bulkrep.order_id=mp.order_id 
LEFT JOIN slroverall ON slroverall.seller_org_id=mp.seller_org_id 
LEFT JOIN slrcat ON slrcat.bc=concat(mp.seller_org_id,mp.category) 
LEFT JOIN slrbu ON slrbu.bc=concat(mp.seller_org_id,mp.business_unit) 
LEFT JOIN shpct ON mp.shipment_group_id = shpct.shipment_group_id 
LEFT JOIN buyerseller ON buyerseller.bs=concat(mp.buyer_org_id,mp.seller_org_id) 
LEFT JOIN shryj ON shryj.order_line_id = mp.order_line_id

WHERE CAST(mp.order_date_time as DATE) >= date_sub(current_date(), 180)
;

DELETE FROM common.mis_table mis

WHERE CAST(mis.order_date_time as DATE) >= date_sub(current_date(), 180);
INSERT INTO common.mis_table select * from mis_table_hr_final_temp;