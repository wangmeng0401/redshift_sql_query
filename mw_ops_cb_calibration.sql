drop table if exists bi_work.mw_ops_cb_calibration_acceptance_rate;
create table bi_work.mw_ops_cb_calibration_acceptance_rate as (
with changeaddress as 
	(select distinct 
		objectid transactionid 
	from mars__revolveclothing_com___db.objectchangelog
	where	objectid in (select transactionid from mars__revolveclothing_com___db.shipment where orderdate >= '2019-01-01')
		and fieldname in ('shipping.street', 'shipping.name', 'shipping.street2', 'shipping.city', 'shipping.state', 'shipping.zipCode')
	)

select 
	date_part('year', orderdate) as orderyr, date_part('month', orderdate) as ordermon, site, payment, gateway,
	case when country = 'US' then 'US' else 'INTL' end as region,
	case when extrastatus ilike 'fraud%' then 'Fraud' when extrastatus ilike 'ato fraud%' then 'ATO Fraud' else 'Others' end extrastatus,
	case when cancelcategory = 'Shipped' then 'Shipped' else 'Not Shipped' end as shipped,
	case when returndate > '2000-01-01' then 'Returned' else 'Not Returned' end as returned,
	case when refunddate > '2000-01-01' then 'Refunded' else 'Not Refunded' end as refunded,
	case when cancelcategory = 'Shipped' and trackingnumber_list <> '' and t3.transactionid is null then 'Customer Shipped' else 'Not Customer Shipped' end as customershipped,
	case when cancelcategory = 'Shipped'  and (returndate > '2000-01-01' or refunddate > '2000-01-01') then 'Recalled' else 'Not Recalled' end as recalled,
	case when cancelcategory = 'Shipped' and trackingnumber_list <> '' and t3.transactionid is not null then 'Fraud Shipped' else 'Not Fraud Shipped' end as fraudshipped,
	case when trackingnumber_list = '' then 1 else 0 end as canceledbeforeshipment, t1.pfraudrange,
	sum(amount) amount, 
	count(invoice) orders,
	sum(case when cancelcategory = 'Shipped' then amount else 0 end) as shippedamount,
	count(case when cancelcategory = 'Shipped' then invoice else null end) as shippedorders,
	sum(case when cancelcategory in ('Cancelled by Customers after Held', 'Cancelled Auto after Held', 'Cancelled Fraud') then amount else 0 end) as cancelfraudamount,
	count(case when cancelcategory in ('Cancelled by Customers after Held', 'Cancelled Auto after Held', 'Cancelled Fraud') then invoice else null end) as cancelfraudorders,
	sum(case when site_revolve = 1 and domestic = 1 then amount else 0 end) as rvdom_amt,
	sum(case when site_revolve = 1 and domestic = 0 then amount else 0 end) as rvintl_amt,
	sum(case when site_revolve = 0 and domestic = 1 then amount else 0 end) as fwddom_amt,
	sum(case when site_revolve = 0 and domestic = 0 then amount else 0 end) as fwdintl_amt,
	max(t4.rvdom_ideal_acceptance_rate) rvdom_ideal_acceptance_rate, 
	max(t4.rvintl_ideal_acceptance_rate) rvintl_ideal_acceptance_rate, 
	max(t4.fwddom_ideal_acceptance_rate) fwddom_ideal_acceptance_rate, max(t4.fwdintl_ideal_acceptance_rate) fwdintl_ideal_acceptance_rate
from 
	(select 
		*, CASE WHEN site = 'R' THEN 1 ELSE 0 END AS site_revolve,CASE WHEN country = 'US' THEN 1 ELSE 0 END AS domestic 
	from bi_work.mw_orderverify_cs_dashboard_v3
	where cancelcategory <> 'Pending' and orderdate >= '2019-01-01') t1
left join 
	(select 
		transactionid, max(extrastatus) as extrastatus,
		listagg(distinct trackingnumber,',') within group (order by shipmentid) trackingnumber_list,max(returndate) as returndate,max(refunddate) as refunddate
	from mars__revolveclothing_com___db.shipment group by 1) t2 on t1.transactionid = t2.transactionid 
left join changeaddress t3 on t1.transactionid = t3.transactionid
inner join bi_work.ja_ideal_acceptance_rate_v2 t4 on rpt = 'CB' and t1.pfraudrange = t4.pfraudrange
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15	
);

drop table if exists bi_work.mw_ops_cb_calibration_cb_monthly_rate_accounting_way;
create table bi_work.mw_ops_cb_calibration_cb_monthly_rate_accounting_way as (
select 
	* 
from 
	(select 
		* 
	from 
		bi_work.mw_netcb_costoffraud_dashboard_v2
	where 
		to_date(yr::varchar(4) + '-' + mo::varchar(2), 'YYYY-MM') >= '2017-07-01' and to_date(yr::varchar(4) + '-' + mo::varchar(2), 'YYYY-MM') <= current_date) a 
left join 
	(select 
		date_part('year', orderdate) as orderyr, 
		date_part('month', orderdate) as ordermon,
		site sitear, 
		case when country = 'US' then 'US' else 'INTL' end as regionar,
	--	case when extrastatus ilike 'fraud%' then 'Fraud' 
	--	when extrastatus ilike 'ato fraud%' then 'ATO Fraud'  
	--	else 'Others' end extrastatus,  
		payment paymentar, gateway gatewayar,t1.pfraudrange,
		sum(amount) amount,count(invoice) orders,sum(case when cancelcategory = 'Shipped' then amount else 0 end) as shippedamount,
		count(case when cancelcategory = 'Shipped' then invoice else null end) as shippedorders,
		sum(case when cancelcategory in ('Cancelled by Customers after Held', 'Cancelled Auto after Held', 'Cancelled Fraud') then amount else 0 end) as cancelfraudamount,
		count(case when cancelcategory in ('Cancelled by Customers after Held', 'Cancelled Auto after Held', 'Cancelled Fraud') then invoice else null end) as cancelfraudorders,
		sum(case when site_revolve = 1 and domestic = 1 then amount else 0 end) as rvdom_amt,
		sum(case when site_revolve = 1 and domestic = 0 then amount else 0 end) as rvintl_amt,
		sum(case when site_revolve = 0 and domestic = 1 then amount else 0 end) as fwddom_amt,
		sum(case when site_revolve = 0 and domestic = 0 then amount else 0 end) as fwdintl_amt,
		max(t4.rvdom_ideal_acceptance_rate) rvdom_ideal_acceptance_rate, max(t4.rvintl_ideal_acceptance_rate) rvintl_ideal_acceptance_rate, max(t4.fwddom_ideal_acceptance_rate) fwddom_ideal_acceptance_rate, max(t4.fwdintl_ideal_acceptance_rate) fwdintl_ideal_acceptance_rate
	from 
		(select *,CASE WHEN site = 'R' THEN 1 ELSE 0 END AS site_revolve,CASE WHEN country = 'US' THEN 1 ELSE 0 END AS domestic 
		from bi_work.mw_orderverify_cs_dashboard_v3
		where cancelcategory <> 'Pending' and orderdate >= '2019-01-01') t1
	left join 
		(select transactionid
		--, max(extrastatus) as extrastatus
		from mars__revolveclothing_com___db.shipment 
		group by 1) t2 on t1.transactionid = t2.transactionid 
	inner join bi_work.ja_ideal_acceptance_rate_v2 t4 on rpt = 'CB' and t1.pfraudrange = t4.pfraudrange
	group by 1, 2, 3, 4, 5, 6, 7
	) b on a.yr = b.orderyr and a.mo = b.ordermon and a.site = b.sitear and a.gateway = b.gatewayar and a.payment = b.paymentar and a.region = b.regionar and a.pfraud_bucket = b.pfraudrange
);

drop table if exists bi_work.mw_ops_cb_calibration_monthly_cb_details;
create table bi_work.mw_ops_cb_calibration_monthly_cb_details as (
WITH v AS -- verifiedby
	(SELECT transactionid, 
	MAX(     
		CASE 
			WHEN INITCAP(lower(split_part(verifiedby,'@',1))) IN ('Alex Bojorquez', 'Alexandria.Bojorquez') THEN 'Alexandria Bojorquez'
			WHEN INITCAP(lower(TRIM(SPLIT_PART(verifiedby,'@',1)))) = 'Kamsat' THEN 'Sam Chung'
			WHEN INITCAP(lower(split_part(verifiedby,'@',1))) LIKE '%.%' THEN SPLIT_PART(INITCAP(lower(split_part(verifiedby,'@',1))),'.',1)||' '||SPLIT_PART(INITCAP(lower(split_part(verifiedby,'@',1))),'.',2) 
			ELSE INITCAP(lower(split_part(verifiedby,'@',1))) 
		End) AS verifiedby 
	FROM  mars__revolveclothing_com___db.shipment 
	WHERE orderdate>='2000-01-01'
	GROUP BY 1
	),

f AS -- pfraud 
	(select transactionid, max(pfraud) as pfraud					
	from 
		(select transactionid, pfraud 			
		from bi_work.mw_pfraud_temp -- bf 2018.10 	
		union 		
		select transactionid, pfraud 		
		from marie__shard.pfraud) j  -- after 2018.10 	
	group by 1
	), 

cb AS 
	(SELECT 
		invoice,status,t1.amount /coalesce(x.rate,1) as amount,t1.id,casenum,receivedate,chargebackdate,settledate,reason,reasoncode,cbcovered,site,
		CASE 
			WHEN INITCAP(lower(split_part(entryby,'@',1))) IN ('Alex Bojorquez', 'Alexandria.Bojorquez') THEN 'Alexandria Bojorquez' -- not in CB team, but in case in the future transfers over
			WHEN INITCAP(lower(TRIM(SPLIT_PART(entryby,'@',1)))) = 'Kamsat' THEN 'Sam Chung'	
			WHEN INITCAP(lower(split_part(entryby,'@',1))) LIKE '%.%' THEN SPLIT_PART(INITCAP(lower(split_part(entryby,'@',1))),'.',1)||' '||SPLIT_PART(INITCAP(lower(split_part(entryby,'@',1))),'.',2) 
			ELSE INITCAP(lower(split_part(entryby,'@',1))) 
	 End AS entryby	
	FROM 
		(select * from (select *, row_number() over (partition by invoice order by receivedate desc) as receiverank 
		from mars__revolveclothing_com___db.opschargeback) where receiverank = 1  and invoice <>'' AND invoice is not null 
		) t1 
	LEFT JOIN 
		(select * from (select *,row_number() over (partition by code,updatedate order by name desc) as r FROM mars__logs.currencyexchangedailylog) where r =1) x 
		ON x.code = t1.currency and x.updatedate = t1.receivedate
	WHERE t1.receivedate>='2017-1-1' AND t1.receivedate<= current_date
	)

SELECT  
	o.invoicenum,cb.id,cb.status,f.pfraud,ceiling(f.pfraud*20)/20 AS pfraud_ceiling,o.amount,o.avsresult AS AVS,
	case when lower(trim(o.shippingstreet)) = lower(trim(o.ccstreet)) then 'Yes' else 'No' end as billingshippingmatch,
	t3.request_verify AS held,trim(nvl(v.verifiedby, '')) as verifiedby,
	cb.casenum,cb.receivedate::date, 
	CASE WHEN cb.settledate>'1970-01-01' THEN cb.settledate ::date  END AS settledate,
	CASE WHEN cb.chargebackdate>'1971-01-01' THEN cb.chargebackdate::date END AS chargebackdate, 
	cb.reason,cb.reasoncode,cb.entryby,
	cb.reasoncode||' - '||
		CASE 
			WHEN reasoncode = 'R_R01' THEN 'RFI'
			WHEN reasoncode = 'R_C01' THEN 'Credit Not Processed'
			WHEN reasoncode = 'R_C02' THEN 'Refund Not Received'
			WHEN reasoncode = 'R_C03' THEN 'Cancelled Merchandise'
			WHEN reasoncode = 'R_C04' THEN 'Merchandise/item Not Received'
			WHEN reasoncode = 'R_C05' THEN 'Not As Described or Defective'
			WHEN reasoncode = 'R_C06' THEN 'Goods/services returned or refused'
			WHEN reasoncode = 'R_C07' THEN 'Refused/services/merchandise Credit Due'
			WHEN reasoncode = 'R_C08' THEN 'Goods/services not received or only partially received'
			WHEN reasoncode = 'R_C09' THEN 'Paid Another Way'
			WHEN reasoncode = 'R_C10' THEN 'Authorization Non-Compliance'
			WHEN reasoncode = 'R_C11' THEN 'Incorrect Translation Code/Amount'
			WHEN reasoncode = 'R_C12' THEN 'Cardholder Request Due to Dispute'
			WHEN reasoncode = 'R_C13' THEN 'Cardholder Dispute'
			WHEN reasoncode = 'R_C14' THEN 'Account Number Not On File'
			WHEN reasoncode = 'R_C15' THEN 'Authorization Related Chargeback'
			WHEN reasoncode = 'R_C16' THEN 'Duplicate Charge'
			WHEN reasoncode = 'R_C17' THEN 'Overcharge'
			WHEN reasoncode = 'R_C18' THEN 'Blank'
			WHEN reasoncode = 'R_C19' THEN 'Blank'
			WHEN reasoncode = 'R_F00' THEN 'Full Recourse-Fraud'
			WHEN reasoncode = 'R_F01' THEN 'Fraud'
			WHEN reasoncode = 'R_F02' THEN 'Unauthorized Transaction'
			WHEN reasoncode = 'R_F03' THEN 'No Cardholder Authorization'
			WHEN reasoncode = 'R_F04' THEN 'Fraud - Cardholder did not authorise or participate in transaction'
			WHEN reasoncode = 'R_F05' THEN 'Code not allowable for dispute'
			WHEN reasoncode = 'R_F06' THEN 'Fraud - Card not Present'
			WHEN reasoncode = 'R_F07' THEN 'Fraud - Card Absent'
			WHEN reasoncode = 'R_F08' THEN 'Fraud - Cardholder Does Not Recognize Fraud 4863'
		END AS standardized_reason_code,
	nvl(s.report_siteflag, cb.site) AS site,    
	CASE 
		WHEN paymentgatewaytype = 'GIP' THEN 'REACH' WHEN paymentgatewaytype = 'FACEBOOK' THEN 'Facebook'
		WHEN paymentgatewaytype = 'CITCON' THEN 'Citcon'
		ELSE
			CASE 
				WHEN INITCAP(LOWER(TRIM(o.cctype))) LIKE 'M%' THEN 'Authorize'
				WHEN INITCAP(LOWER(TRIM(o.cctype))) LIKE 'V%' THEN 'Authorize'
				WHEN INITCAP(LOWER(TRIM(o.cctype))) LIKE '%Am%' AND INITCAP(LOWER(TRIM(o.cctype))) NOT IN ('Amazon','Amzn') THEN 'Authorize'
				WHEN INITCAP(LOWER(TRIM(o.cctype))) LIKE 'Dis%' THEN 'Discover'
				WHEN INITCAP(LOWER(TRIM(o.cctype))) LIKE 'Pay%' THEN 'PayPal'
				WHEN INITCAP(LOWER(TRIM(o.cctype))) LIKE '%Alipay%' THEN 'Alipay'
				WHEN INITCAP(LOWER(TRIM(o.cctype))) LIKE 'Afterpay%' THEN 'Afterpay'
				WHEN INITCAP(LOWER(TRIM(o.cctype))) LIKE 'Klarna%' THEN 'Klarna'
				WHEN INITCAP(LOWER(TRIM(o.cctype))) IN ('','Unknown') THEN 'Unknown'
				ELSE 'Others' 
			END
	END AS gateway,
	CASE 
		WHEN INITCAP(LOWER(TRIM(o.paymenttokenservice))) ='Applepay' OR INITCAP(LOWER(TRIM(o.cctype))) ='Applepay' THEN 'ApplePay'
		WHEN INITCAP(LOWER(TRIM(o.cctype))) LIKE 'M%' THEN 'Master Card'
		WHEN INITCAP(LOWER(TRIM(o.cctype))) LIKE 'V%' THEN 'Visa'
		WHEN INITCAP(LOWER(TRIM(o.cctype))) LIKE '%Am%' AND INITCAP(LOWER(TRIM(o.cctype))) NOT IN ('Amazon','Amzn') THEN 'Amex'
		WHEN INITCAP(LOWER(TRIM(o.cctype))) LIKE 'Dis%' THEN 'Discover'
		WHEN INITCAP(LOWER(TRIM(o.cctype))) LIKE 'Pay%' THEN 'PayPal'
		WHEN INITCAP(LOWER(TRIM(o.cctype))) LIKE '%Alipay%' THEN 'Alipay'
		WHEN INITCAP(LOWER(TRIM(o.cctype))) LIKE 'Afterpay%' THEN 'Afterpay'
		WHEN INITCAP(LOWER(TRIM(o.cctype))) LIKE 'Klarna%' THEN 'Klarna'
		WHEN INITCAP(LOWER(TRIM(o.cctype))) IN ('','Unknown') THEN 'Unknown'
		ELSE 'Others' 
	END AS payment,
	CASE WHEN lower(o.shippingcountry) IN ('us','united states','usa','u.s.a.','u.s.a','u.s.') THEN 'US' ELSE 'INTL' END AS region, 
	cb.amount AS cb_received, 
	CASE WHEN cb.status = 'Dropped' THEN cb.amount Else '' END AS amount_dropped, 
	CASE WHEN cb.status = 'Charged back' THEN cb.amount ELSE '' END amount_chargedback,
	CASE WHEN trim(lower(invoicenum)) in 										
		(select distinct trim(lower(invoice)) from mars__revolveclothing_com___db.orderchangelog where "action" ='Riskified Approved') THEN 'Yes' ELSE 'No' 
	END AS Riskified_Approved,									
	CASE WHEN cbcovered = 'Yes' THEN 'Yes'
		 ELSE 
			CASE WHEN trim(lower(invoicenum)) in 										
						(select distinct trim(lower(invoice)) 									
						 from mars__revolveclothing_com___db.orderchangelog 
						 where "action" ='Riskified Approved')
					AND reasoncode ilike 'R_F%' then 'Yes'
					Else 'No'
				END 
	END AS cbcovered, --- backfill based on code, also devops may backfill soon too
	CASE WHEN trim(lower(invoicenum)) in
			(select distinct trim(lower(invoice)) 									
			 from mars__revolveclothing_com___db.orderchangelog 
			 where "action" ='Riskified Approved')
		AND reasoncode ilike 'R_F%' THEN 0 
		ELSE cb.amount - (CASE WHEN cb.status = 'Dropped' THEN cb.amount Else '' END) - (CASE WHEN cb.status = 'Charged back' THEN cb.amount ELSE '' END) 
	END AS CB_at_risk
FROM cb
LEFT JOIN mars__revolveclothing_com___db.orders o ON TRIM(LOWER(cb.invoice)) = TRIM(LOWER(o.invoicenum))
LEFT JOIN bi_report.siteflag s ON o.ordertype = s.ordertype
LEFT JOIN f ON trim(lower(o.transactionid)) = trim(lower(f.transactionid)) 
left join bi_work.mw_orderverify_cs_dashboard_v3 t3 on TRIM(LOWER(cb.invoice)) = TRIM(LOWER(t3.invoice))
LEFT JOIN v ON trim(lower(o.transactionid)) = trim(lower(v.transactionid))
ORDER BY id
);