drop table if exists corpfpa.log_charge_dump;
create table if not exists corpfpa.log_charge_dump as 
 SELECT 
 sg.shipment_group_id AS shipment_group_id, 
 sum(inv.amount_in_paisa)/100 AS logistics_amount, 
 inv.buyer_org_id as log_billed_to, 
 inv.seller_org_id as log_billed_by 
 
	FROM ds_sql_shipment_group sg 
		LEFT JOIN ds_sql_invoice_shipment_group_association isa ON sg.shipment_group_id=isa.shipment_group_id 
		LEFT JOIN  ds_sql_invoice inv ON inv.invoice_id=isa.invoice_id 
		
		
	WHERE inv.invoice_type='LOGISTIC_SERVICE' AND sg.current_active=1 AND inv.current_active = 1
	GROUP BY sg.shipment_group_id, inv.buyer_org_id, inv.seller_org_id ; 


drop table if  exists corpfpa.seller_charges_order_commissions_exploded ; 
create table if not exists corpfpa.seller_charges_order_commissions_exploded as 
Select 
id,
goodsInvoiceId,
goodsInvoiceRefId,
from_unixtime(cast((goodsInvoiceCreatedAt+19800000)/1000 as bigint),'yyyy-MM-dd') goodsInvoiceCreatedAt,
sellerChargesInvoiceId,
sellerChargesInvoiceRefId,
from_unixtime(cast((createdAt+19800000)/1000 as bigint),'yyyy-MM-dd') createdAt,
from_unixtime(cast((updatedAt+19800000)/1000 as bigint),'yyyy-MM-dd') updatedAt,
sellerGSTN,
sellerOrgId,
buyerOrgId,
platformGSTN,
platformOrgId,
orderId,
shipmentGroupId,
status as commission_line_status ,
from_unixtime(cast((deliveryTimestamp+19800000)/1000 as bigint),'yyyy-MM-dd')  deliveryTime,
invoiceAmountPaise,
defaultCommission,
orderCommissionLines,
receivableId,
flags,
ocls.commissionLineId,
ocls.commissionPaise,
ocls.commissionTaxPaise,
ocls.totalCommissionPaise,
ocls.entity ,
ocls.invoiceLineDescription,
ocls.invoiceLineId,
ocls.invoicedAmountInPaise,
ocls.listingId,
ocls.orderLineId, 
ocls.rate

from ds_cms_order_commissions lateral view explode(orderCommissionLines) vtable as ocls ; 


drop table if exists corpfpa.sri_monetization ; 
create table if not exists corpfpa.sri_monetization as 
select 
mis.dts_tag ,
mis.bulk_retail,
mis.order_id , 
case when udi.order_id is not null then 'Yes' else 'No' end as udaan_invoice_tag,
mis.shipment_group_id,
mis.order_line_id , 
count(mis.order_line_id) over (partition by mis.order_id) as total_lines ,
mis.fulfillment_center ,
mis.order_status , 
mis.shipment_status,
mis.order_date ,
substr(mis.order_date ,1,7) as order_month  ,
to_date(mis.first_shipped) as first_shipped,
to_date(mis.delivered) as delivered,
substr(to_date(mis.delivered) as delivered,1,7)  as delivered_month , 
mis.buyer_org_id ,
mis.seller_org_id  , 
mis.seller_org_unit_id, 
oug.legal_name as seller_org_unit_Legal_Name , 
oug.trade_name as seller_org_unit_Trade_Name, 
oug.gstin  as seller_org_unit_GSTIN , 
oug.state_name  as seller_org_unit_State , 
get_json_object(ou.unit_address,"$.city") as seller_org_unit_city ,
mis.business_unit ,
mis.category , 
mis.sub_category , 
mis.vertical , 
mis.brand , 
mis.listing_id ,
mis.listing_title,
mis.sku_id ,
mis.vertical_spec , 
mis.total_line_amount ,
mis.invoiced_value,
mis.sg_amount as mis_order_level_invoice , 
mis.logistics_amount as order_level_logistics_charge , 
mis.logistics_charge as line_level_logistics_charge , 
Case  when log.log_billed_by = 'ORGXSDSDKWF9ZCHE4E94PZBQ2XK5G' then 'HCPL'
	    when log.log_billed_by = 'ORGNHLFHWCKYMQJHG37PRBXX0KDFF' then 'HLPL'
	    when log.log_billed_by IN ('ORG0162SPQ290DNPGFZ8QD2SH1W31','ORGXW851TGXM2CFSF941NTC6D2KDY','ORGNX5DX102QFCKXFNJ724DSNTS5T') then 'Udaan'
      ELSE log.log_billed_by END as Logistics_issued_by ,
Case  when log.log_billed_to IN ('ORG0162SPQ290DNPGFZ8QD2SH1W31','ORGXW851TGXM2CFSF941NTC6D2KDY','ORGNX5DX102QFCKXFNJ724DSNTS5T') then 'Udaan' 
	    when log.log_billed_to = mis.seller_org_id then 'Seller'
	    when log.log_billed_to = mis.buyer_org_id then  'Buyer' ELSE log.log_billed_to END as Logistics_billed_to,

mis.num_of_items,
case when X2.orderid is not null then X2.receivableid		else X.receivableid	 END 	receivable_id,
case when X2.sellerchargesinvoiceid is not null then X2.sellerchargesinvoiceid  else X.sellerchargesinvoiceid  END sellerchargesinvoiceid  ,
case when X2.sellerchargesinvoiceid is not null then X2.sellerchargesinvoicerefid  else X.sellerchargesinvoicerefid  END sellerchargesinvoicerefid  ,
case when X2.goodsInvoiceId is not null then X2.goodsInvoiceId else X.goodsInvoiceId END goodsInvoiceId  ,
case when X2.goodsInvoiceRefId is not null then X2.goodsInvoiceRefId  else X.goodsInvoiceRefId  END goodsInvoiceRefId  ,
case when X2.orderid is not null then X2.invoiceamountpaise		else X.invoiceamountpaise	 END 		order_level_invoice,
case when X2.orderid is not null then X2.invoicedamountinpaise	else X.invoicedamountinpaise	END 	line_level_invoice ,
case when X2.orderid is not null then X2.entity.type 				else X.entity.type 		END 		rate_type , 
case when X2.orderid is not null then X2.rate 					else X.rate 	END 					rate		,		
case when X2.orderid is not null then X2.commission_line_status 					else X.commission_line_status 	END 					commission_line_status		,	 


case when X2.orderid is not null then X2.commissionPaise			else X.commissionPaise	 END 			commissionpaise			,
case when X2.orderid is not null then X2.commissionTaxPaise		else X.commissionTaxPaise		END 	commissiontaxpaise				,
case when X2.orderid is not null then X2.totalCommissionPaise     else X.totalCommissionPaise   END  	totalcommissionPaise 
FROM 

common.mis_table  mis 
  LEFT JOIN corpfpa.seller_charges_order_commissions_exploded X   ON mis.order_line_id =x.orderlineid 
  LEFT JOIN corpfpa.seller_charges_order_commissions_exploded X2  ON mis.order_id=x2.orderid AND X2.orderlineid is null 
  LEFT JOIN ds_sql_invoice_line udi  on udi.order_id=mis.order_id
  LEFT JOIN ds_sql_cnc_org cnc on cnc.org_id=mis.seller_org_id 
  LEFT JOIN ds_sql_org_unit_gstin  oug on oug.org_unit_id = mis.seller_org_unit_id
  LEFT JOIN ds_sql_org_units ou on ou.org_unit_id= mis.seller_org_unit_id
  LEFT JOIN corpfpa.log_charge_dump Log on mis.shipment_group_id=log.shipment_group_id


where MIS.include is null 
 ;



drop table if exists corpfpa.seller_charges_order_commissions_exploded ;