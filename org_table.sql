DROP TABLE org_table_tmp;

CREATE TABLE IF NOT EXISTS org_table_tmp as 
SELECT 
og.org_id AS org_id, 
og.org_name AS org_name, 
og.owner_user_id AS owner_user_id, 
og.owner_user_name AS owner_user_name, 
og.owner_phone AS owner_phone, 
og.pincode AS pincode, 
og.city AS city, 
og.state AS state, 
og.created_at AS created_at, 
og.registration_week AS registration_week, 
og.pref_category AS pref_category, 
og.org_type AS org_type, 
og.is_selling_enabled  AS is_selling_enabled, 
og.selling_enabled_time AS selling_enabled_time, 
to_date(from_utc_timestamp(og.selling_enabled_time, "IST")) as selling_enabled_date, 
buyer.first AS first_buyer_order, 
buyer.last AS last_buyer_order, 
buyer.orders AS buyer_orders_till_date, 
buyer.gmv AS buyer_GMV_till_date, 
buyer30.orders AS buyer_orders_last_30days, 
buyer30.gmv AS buyer_GMV_last_30days, 
seller.first AS first_seller_order, 
seller.last AS last_seller_order, 
seller.orders AS seller_orders_till_date, 
seller.gmv AS seller_GMV_till_date, 
seller30.orders AS seller_orders_last_30days, 
seller30.gmv AS seller_GMV_last_30days,
bm.type AS buyer_tag, 
lst.count AS active_listing_count, 
seller.rts AS first_seller_rts, 
og.status as status, 
top60.mapped_city AS top60_mapped_city, 
top60.tag AS top60_tag, 
og.gstin as gstin, 
case when selgst.org_id = og.org_id then selgst.gstin_value else og.gstin_value end as gstin_value, 
og.selling_category as selling_category,
og.product_category as product_category, 
og.pref_selling_categories, 
lst.last_listing_created_at_ts



FROM temp_org_table AS og 


LEFT JOIN 
	(SELECT	buyer_org_id,
			min(order_date_time) AS first, 
			max(order_date_time) AS last, 
			count(distinct order_id) AS orders, 
			sum(total_line_amount) AS gmv 
		FROM mis_table WHERE include is NULL GROUP BY buyer_org_id
	) 
	
AS buyer ON buyer.buyer_org_id=og.org_id 

LEFT JOIN 
	(SELECT	buyer_org_id, 
			count(distinct order_id) AS orders, 
			sum(total_line_amount) AS gmv 
		FROM mis_table 
		WHERE include is NULL AND order_date_time>(unix_timestamp()-2592000)*1000 GROUP BY buyer_org_id
	) 
AS buyer30 ON buyer30.buyer_org_id=og.org_id 

LEFT JOIN 
	(SELECT	seller_org_id, 
			min(order_date_time) AS first, 
			max(order_date_time) AS last, 
			count(distinct order_id) AS orders, 
			sum(total_line_amount) AS gmv, 
			min(rts) AS rts 
		FROM mis_table
		WHERE include is NULL 
		GROUP BY seller_org_id
	)
AS seller ON seller.seller_org_id=og.org_id 

LEFT JOIN 
	(SELECT	seller_org_id, 
			count(distinct order_id) AS orders, 
			sum(total_line_amount) AS gmv 
		FROM mis_table 
		WHERE include is NULL AND order_date_time>(unix_timestamp()-2592000)*1000 
		GROUP BY seller_org_id
	) 
AS seller30 ON seller30.seller_org_id=og.org_id 

LEFT JOIN ds_csv_buyer_mapping AS bm ON bm.org_id=og.org_id 

LEFT JOIN
	(SELECT	org_id, 
			count(listing_id) AS count, 
			max(created_at) as last_listing_created_at_ts 
		FROM ds_stream_listing 
		where status="ACTIVE" GROUP BY org_id
	)
AS lst ON og.org_id=lst.org_id

LEFT JOIN  ds_csv_top60city AS top60 ON concat (og.city,og.state) = concat (top60.city,top60.state)
LEFT JOIN ds_csv_seller_gstin AS  selgst ON selgst.org_id = og.org_id
			   ;
