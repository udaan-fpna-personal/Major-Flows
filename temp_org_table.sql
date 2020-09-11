drop table temp_org_inferred_category;
create table if not exists temp_org_inferred_category as 
SELECT	org_id,
		collect_set(get_json_object(`data`, '$.categories[0].categoryName'))[0] as inferred_product_category 
	FROM ds_user_accounts_org_prefs WHERE section = 'insights' and sub_section = 'categories' and status = "ENABLED" group by org_id;

drop table temp_org_selling;
create table if not exists temp_org_selling as 
SELECT	org_id,
		get_json_object(`data`, '$.isSellingEnabled') as selling, 
		created_at, 
		row_number() OVER (PARTITION BY org_id ORDER BY created_at asc) as row_num, 
		get_json_object(data,'$.sellingCategories') cat, 
		get_json_object(data,'$.sellingCategories[0]') pref_cat 
	FROM ds_user_accounts_org_prefs 
	WHERE section = 'selling' and sub_section = 'conditions' and status = "ENABLED" and data like '%isSellingEnabled%' and data like '%sellingCategories%';
	 
drop table temp_org_table1;
CREATE TABLE IF NOT EXISTS temp_org_table1 STORED AS ORC AS 

SELECT	og.org_id AS org_id,
		og.org_name AS org_name, 
		og.user_id_owner AS owner_user_id, 
		og.status as status, 
		og.pref_product_category as pref_product_category,
		
		us.owner_user_name AS owner_user_name, 
		us.owner_phone AS owner_phone, 
		
		ou.pincode AS pincode, 
		ou.city AS city, 
		ou.state AS state, 
		
		og.created_at AS created_at, 
		og.registration_week AS registration_week, 
		og.pref_category AS pref_category, 
		og.org_type AS org_type, 
		
		sell.selling AS is_selling_enabled, 
		sell.created_at AS selling_enabled_time, 
		
		og.gstin as gstin,
		og.gstin_value as gstin_value, 

		CASE WHEN sell.cat LIKE '%appliances%' OR  sell.cat LIKE '%electronics%' or sell.cat LIKE '%computers%' THEN "Acc & CE" 
		WHEN sell.cat LIKE '%mobiles%' THEN "Phones"
		WHEN sell.cat LIKE '%clothing%' OR sell.cat LIKE '%fashion%' THEN "Clothing" 
		WHEN sell.cat LIKE '%fmcg%' THEN "Food-FMCG" 
		WHEN sell.cat LIKE '%home_and_kitchen%' THEN "Home & Kitchen" 
		WHEN sell.cat LIKE '%stationary%' THEN "Stationery & Office Supplies" 
		WHEN sell.cat LIKE '%toy_and_babycare%' THEN "Toys & Baby Care" 																							
		WHEN sell.cat LIKE '%footwear%' THEN "Footwear" 																							
			END AS selling_category,


		sell.cat as pref_selling_categories
		
		
	FROM 
	
		(
		select	org_id, 
				REGEXP_REPLACE(og.display_name,'\\n','') AS org_name, 
				user_id_owner, 
				status, 
				head_office_org_unit_ref, 
				created_at, 
				CONCAT(YEAR(from_unixtime(cast((og.created_at+19800000)/1000 as bigint), 'yyyy-MM-dd')),"/",WEEKOFYEAR(from_unixtime(cast((og.created_at +19800000)/1000 as bigint), 'yyyy-MM-dd'))) AS registration_week, 
				get_json_object(og.data,'$.pref_category') pref_category, 
				lower(get_json_object(og.data,'$.pref_category[0]') ) as pref_product_category, 
				get_json_object(og.data,'$.org_type') org_type, 
				GET_JSON_OBJECT(og.data,'$.identity\[0\].type') gstin, 
				GET_JSON_OBJECT(og.data,'$.identity\[0\].value') gstin_value 
				
			from ds_user_accounts_orgs og) AS og 
					LEFT JOIN
						(select	REGEXP_REPLACE(us.full_name,'\\n','') owner_user_name, 
						REGEXP_REPLACE(us.mobile_primary,'-','') owner_phone, 
						user_id 
						from ds_user_accounts_users us
						) 
					AS us ON us.user_id=og.user_id_owner 
					LEFT JOIN 
						(select	get_json_object(ou.unit_address,'$.pincode') AS pincode, 
						get_json_object(ou.unit_address,'$.city') AS city, 
						get_json_object(ou.unit_address,'$.state') AS state, 
						org_unit_id from ds_user_accounts_org_units ou
						)
					AS ou ON og.head_office_org_unit_ref=ou.org_unit_id 
					LEFT JOIN 
						(SELECT DISTINCT 
						org_id, 
						get_json_object(data,'$.isSellingEnabled') AS selling, 
						created_at, 
						row_number() OVER (PARTITION BY org_id ORDER BY created_at asc) as row_num, 
						get_json_object(data,'$.sellingCategories') cat 
						
					FROM ds_user_accounts_org_prefs 
					WHERE data like '%isSellingEnabled%' OR  data like '%sellingCategories%'
					) AS sell ON og.org_id=sell.org_id AND sell.row_num = 1

;

drop table if exists temp_org_table2;
create table if not exists temp_org_table2 stored as ORC as 
select	tot.*, 
		tc.inferred_product_category, 
		coalesce(tc.inferred_product_category, tot.pref_product_category) as product_category 
	from temp_org_table1 tot 
		left join temp_org_inferred_category tc on tc.org_id = tot.org_id;
		
		

drop table temp_org_table;
CREATE TABLE IF NOT EXISTS temp_org_table STORED AS ORC AS SELECT * FROM temp_org_table2;
drop table if exists temp_org_table2;
drop table  if exists temp_org_table1;
drop table temp_org_selling;
