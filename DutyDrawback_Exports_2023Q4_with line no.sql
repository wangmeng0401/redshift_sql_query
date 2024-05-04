------------------------------------------------------------------
-- PULL ALL ELIGIBLE EXPORTS
------------------------------------------------------------------
-- EXCLUDE: Chile / Mexico / Canada / Puerto Rico (done in final output)
------------------------------------------------------------------
DROP TABLE IF EXISTS bi_work.dd_eligible_exports;
-- gathering serialnumber info, receivedate, cost, ponum, etc.
CREATE TABLE bi_work.dd_eligible_exports AS
WITH ALLSHIPMENT AS (
	SELECT 
		t4.sitename, 
		t1.invoicenum AS invoice, 
		t1.transactionid, 
		t1.shipmentid, 
		t2.trackingnumber, 
		t1.sstatus,
		t1.shippingcountry AS customercountry, 
		t7.countrycode AS customercountrycode, 
		t7.us, 
		t1.rdate::DATE AS dispatchdate, 
		DATEADD(d,1,t1.rdate)::DATE AS exportdate, 
		t2.deliverydate,
		REPLACE(t1.serialnumber, 's', '') AS serialnumber_join,
		's'||REPLACE(t1.serialnumber, 's', '') AS serialnumber,
		t1.productcode, 
		t1.ssales, 
		t2.cost, 
		t3.name AS productname,
		t3.isportfolio,  
		t5.shippingoption,
		ROW_NUMBER() OVER (PARTITION BY REPLACE(t1.serialnumber, 's', '') ORDER BY t1.rdate DESC) AS rn,
		FIRST_VALUE(t7.us) OVER (PARTITION BY REPLACE(t1.serialnumber, 's', '') ORDER BY t1.rdate rows between unbounded preceding and unbounded following) AS firstshipmentcountry_us,
		FIRST_VALUE(t1.rdate) OVER (PARTITION BY REPLACE(t1.serialnumber, 's', '') ORDER BY CASE WHEN t7.us = 1 THEN '2050-01-01' ELSE t1.rdate END rows between unbounded preceding and unbounded following) AS firstintlshipmentdate
	FROM 
		bi_report.shipmentnumber_rs t1
	INNER JOIN 
		mars__revolveclothing_com___db.shipment t2 ON t1.shipmentid = t2.shipmentid
	INNER JOIN 
		mars__revolveclothing_com___db.orders t5 ON t2.transactionid = t5.transactionid
	LEFT OUTER JOIN 
		bi_report.siteflag t4 ON t1.ordertype = t4.ordertype
	LEFT JOIN 
		bi_work.fw_countryregion t7 ON t1.shippingcountry = t7.country
	LEFT JOIN 
		mars__revolveclothing_com___db.product t3 ON t1.productcode = UPPER(TRIM(t3.code))
	WHERE 
		t1.rdate >= '2000-01-01' 
		-- AND t3.isportfolio = 1
		-- t1.rdate >= DATE_TRUNC('month',DATEADD(YEAR,-5,CURRENT_DATE)) AND t1.rdate < DATE_TRUNC('quarter',DATEADD(QUARTER,-1,CURRENT_DATE))
		AND REPLACE(t1.serialnumber, 's', '') <> ''
	), 

TEMP AS (
	SELECT DISTINCT 
		t2.serialnumber::varchar(30) AS serialnumber, 
		t2.cost, 
		t2.ponum, 
		t6.fponum, 
		t6.masterfponum, 
		t2.receivedate, 
		COALESCE( NULLIF(REGEXP_REPLACE(UPPER(TRIM(t6.alliancestylecode)),'[ ]',''),''), UPPER(COALESCE(NULLIF(REGEXP_REPLACE(TRIM(t5.alliancestylecode),'[ ]',''),''), CONCAT(REGEXP_REPLACE(TRIM(t5.stylenum), '[ ]', '-'), TRIM(t5.color)))) ) AS alliancestylecode
	FROM 
		mars__revolveclothing_com___db.inventoryitem t2
	LEFT OUTER JOIN 
		mars__revolveclothing_com___db.poitem t5 ON UPPER(TRIM(t2.ponum)) = UPPER(TRIM(t5.ponum)) AND UPPER(TRIM(t2.product)) = UPPER(TRIM(t5.productcode))
	LEFT OUTER JOIN 
		mars__alliance.factorypo t6 ON t5.id = t6.revolvepoid AND fpostatus = 'Received' 
		AND CASE WHEN t2.ismexicoserial = 1 THEN '-MX' ELSE '-US' END = CASE WHEN RIGHT(UPPER(TRIM(t6.masterfponum)),3) IN ('-US','-MX') THEN RIGHT(UPPER(TRIM(t6.masterfponum)),3) ELSE '-US' END
	WHERE 
		t2.receivedate >= '2014-05-01' AND t2.qtyreceived = 1 -- this may include item imported prior to this date, email Gavin regarding import date
	) 

SELECT DISTINCT 
	t1.*, 
	ROUND(t1.ssales, 2) AS exportprice,
	t2.ponum, 
	t2.fponum, 
	t2.masterfponum, 
	t2.receivedate,
	CASE WHEN t1.isportfolio = 1 THEN t2.alliancestylecode ELSE '' END AS alliancestylecode
FROM 
	ALLSHIPMENT t1
LEFT JOIN 
	TEMP t2 ON t1.serialnumber_join = t2.serialnumber
WHERE 
	t1.rn = 1 AND t1.us = 0 AND t1.sstatus = 'shipped' --AND t1.customercountrycode NOT IN ('CA','CL','MX','PR') 
	AND t1.isportfolio = 1
	AND t1.exportdate >= '2021-01-01' AND t1.exportdate < '2024-01-01'
	AND (t1.firstshipmentcountry_us = 0 OR DATEDIFF(day, t2.receivedate, t1.firstintlshipmentdate) <= 365);

DROP TABLE IF EXISTS bi_work.dd_exports_pre;
CREATE TABLE bi_work.dd_exports_pre AS
WITH HTSCODES AS (
	SELECT DISTINCT 
		entry_no, 
		po_numbers, 
		REGEXP_REPLACE(alliancestylecode,'[ ]','') alliancestylecode, -- Remove space from alliancecode imported before 2022Q4
		import_date, 
		hs_code, 
		hs_code2, 
		adv_rate, 
		adv_rate2, 
		unit_1, 
		description,
		ROW_NUMBER() OVER (PARTITION BY REGEXP_REPLACE(alliancestylecode,'[ ]',''), po_numbers ORDER BY import_date DESC) AS rn
	FROM 
		bi_work.dd_importsfinal_ALL
	WHERE 
		hs_code IN (SELECT hs_code FROM bi_work.dd_import_outputfile_ALL)
	), 

FIRSTENTRY_PRE AS (
	SELECT DISTINCT 
		alliancestylecode, 
		FIRST_VALUE(entry_no) OVER (PARTITION BY alliancestylecode ORDER BY import_date, 
		entry_no ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS entry_no
	FROM 
		HTSCODES
	WHERE 
		adv_rate > 0
	), 

FIRSTENTRYPONUM AS (
	SELECT DISTINCT 
		t1.entry_no, 
		t1.alliancestylecode, 
		FIRST_VALUE(po_numbers) OVER (PARTITION BY t1.alliancestylecode ORDER BY adv_rate DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS po_numbers
	FROM 
		FIRSTENTRY_PRE t1
	INNER JOIN 
		HTSCODES t2 ON t1.alliancestylecode = t2.alliancestylecode AND t1.entry_no = t2.entry_no
	), 

EXPORTSMAPPED AS (
	SELECT 
		TIMEZONE('America/Los_Angeles', TO_TIMESTAMP(GETDATE(),'YYYY-MM-DD HH24:MI:SS'))::DATE AS pull_date, 
		t1.sitename AS exporter, 
		t1.exportdate, 
		'Air' AS carrier, 
		t1.trackingnumber, 
		t1.customercountrycode AS destination, 
		t1.productcode, 
		t1.invoice, t1.serialnumber, 
		'' AS tracking2, 1 AS qty, 
		'PCS' as unitofmeasure,
		t2.hs_code, t2.hs_code2, 
		t1.productname as description, 
		t1.exportprice, t2.unit_1, 
		t2.entry_no, 0 as substituted,
		t1.alliancestylecode, 
		t2.description AS importdescription, t1.deliverydate
	FROM 
		bi_work.dd_eligible_exports t1 
	INNER JOIN 
		HTSCODES t2 ON t1.masterfponum = t2.po_numbers AND t1.alliancestylecode = t2.alliancestylecode
	WHERE 
		t1.isportfolio = 1 AND t2.rn = 1
	-- AND (
	-- 	(t1.exportdate >= '2023-04-01' AND t1.exportdate < '2023-07-01' )
	--     OR (t1.exportdate >= '2020-01-01' AND t1.exportdate < '2023-04-01' AND t1.customercountrycode IN ('CA','CL','MX','PR'))
	--   )
		AND t1.exportdate >= '2023-10-01' AND t1.exportdate < '2024-01-01' 
-- 		AND t1.customercountrycode NOT IN ('CA','CL','MX')
-- 		AND t1.exportdate >= '2023-10-01' AND t1.exportdate < '2024-01-01' AND t1.customercountrycode IN ('CA','CL','MX')
	),

SUB_MAPPING AS (
	SELECT 
		t1.entry_no, t1.alliancestylecode, t2.hs_code, 
		t2.hs_code2, t2.adv_rate, t2.adv_rate2, 
		t2.unit_1, t2.description,
		ROW_NUMBER() OVER (PARTITION BY t1.entry_no, t1.alliancestylecode, t2.hs_code ORDER BY adv_rate DESC) AS rn
	FROM 
		FIRSTENTRYPONUM t1 
	LEFT JOIN 
		HTSCODES t2 ON t1.alliancestylecode = t2.alliancestylecode AND t1.po_numbers = t2.po_numbers AND t1.entry_no = t2.entry_no
	), 

EXPORTS_SUB AS (
	SELECT DISTINCT 
		TIMEZONE('America/Los_Angeles', TO_TIMESTAMP(GETDATE(),'YYYY-MM-DD HH24:MI:SS'))::DATE AS pull_date, 
		t1.sitename AS exporter, t1.exportdate, 'Air' AS carrier, t1.trackingnumber, t1.customercountrycode AS destination, 
		t1.productcode, t1.invoice, t1.serialnumber, '' AS tracking2, 1 AS qty, 'PCS' as unitofmeasure,
		t2.hs_code, t2.hs_code2, t1.productname as description, t1.exportprice, t2.unit_1, t2.entry_no, 1 as substituted,
		t1.alliancestylecode, t2.description AS importdescription, t1.deliverydate
	FROM 
		bi_work.dd_eligible_exports t1 
	INNER JOIN 
		SUB_MAPPING t2 ON t1.alliancestylecode = t2.alliancestylecode
	WHERE 
		t1.isportfolio = 1 AND t2.rn = 1 
	-- AND (
	-- 	(t1.exportdate >= '2023-04-01' AND t1.exportdate < '2023-07-01' )
	--     OR (t1.exportdate >= '2020-01-01' AND t1.exportdate < '2023-04-01' AND t1.customercountrycode IN ('CA','CL','MX','PR'))
	--   )
		AND t1.exportdate >= '2023-10-01' AND t1.exportdate < '2024-01-01' 
-- 		AND t1.customercountrycode NOT IN ('CA','CL','MX','PR')
-- 		AND t1.exportdate >= '2023-10-01' AND t1.exportdate < '2024-01-01' AND t1.customercountrycode IN ('CA','CL','MX','PR')
		AND t1.serialnumber NOT IN (SELECT serialnumber FROM EXPORTSMAPPED)
	)

SELECT 
	* 
FROM 
	EXPORTSMAPPED
UNION ALL 
SELECT 
	* 
FROM 
	EXPORTS_SUB;

-- select serialnumber, hs_code, count(*), pull_date 
-- from bi_work.dd_exports_pre
-- where pull_date = TIMEZONE('America/Los_Angeles', TO_TIMESTAMP(GETDATE(),'YYYY-MM-DD HH24:MI:SS'))::DATE
-- group by 1,2,4 order by 3 desc;

DELETE FROM bi_work.dd_exports WHERE pull_date = TIMEZONE('America/Los_Angeles', TO_TIMESTAMP(GETDATE(),'YYYY-MM-DD HH24:MI:SS'))::DATE;

insert into bi_work.dd_exports 
select * from bi_work.dd_exports_pre t1
-- where pull_date = '2020-03-19' and exportdate >= '2018-01-01' and exportdate < '2019-10-01';
-- where pull_date = '2020-04-08' and exportdate >= '2019-10-01' and exportdate < '2020-01-01';
-- where pull_date = '2020-07-16' and exportdate >= '2020-01-01' and exportdate < '2020-07-01';
-- where pull_date = '2020-10-09' and exportdate >= '2020-07-01' and exportdate < '2020-07-01';
-- where pull_date = '2021-01-07' AND exportdate >= '2020-07-01' AND exportdate < '2020-10-01' AND t1.destination NOT IN ('CA','CL','MX','PR');
-- where pull_date = '2021-01-07' AND exportdate >= '2020-01-01' AND exportdate < '2020-10-01' AND t1.destination IN ('CA','CL','MX','PR');
-- where pull_date = '2021-04-13' and exportdate >= '2020-10-01' and exportdate < '2021-01-01' AND t1.destination NOT IN ('CA','CL','MX','PR'); 
-- where pull_date = '2021-05-17' and exportdate >= '2021-01-01' and exportdate < '2021-07-01' AND t1.destination NOT IN ('CA','CL','MX','PR');
-- where pull_date = '2021-10-11' and exportdate >= '2021-07-01' and exportdate < '2022-04-01' AND t1.destination NOT IN ('CA','CL','MX','PR');
-- where pull_date = '2022-01-27' and exportdate >= '2021-07-01' and exportdate < '2022-04-01' AND t1.destination NOT IN ('CA','CL','MX','PR')
-- where pull_date = '2022-04-07' and exportdate >= '2021-10-01' and exportdate < '2022-04-01' AND t1.destination NOT IN ('CA','CL','MX','PR')
-- where pull_date = '2022-07-29' and exportdate >= '2022-01-01' and exportdate < '2022-04-01' AND t1.destination NOT IN ('CA','CL','MX','PR')
-- where pull_date = '2022-10-24' and exportdate >= '2022-04-01' and exportdate < '2022-07-01' AND t1.destination NOT IN ('CA','CL','MX','PR')
-- where pull_date = '2023-01-22' and exportdate >= '2022-07-01' and exportdate < '2023-04-01' AND t1.destination NOT IN ('CA','CL','MX','PR')
-- where pull_date = '2023-04-21' and exportdate >= '2022-10-01' and exportdate < '2023-04-01' AND t1.destination NOT IN ('CA','CL','MX','PR')
-- where pull_date = '2023-08-30' and exportdate >= '2023-01-01' and exportdate < '2023-04-01' AND t1.destination NOT IN ('CA','CL','MX','PR')
-- where pull_date = '2023-10-25' and exportdate >= '2023-04-01' and exportdate < '2023-07-01' AND t1.destination NOT IN ('CA','CL','MX')
-- where pull_date = '2024-02-22' and exportdate >= '2023-07-01' and exportdate < '2023-10-01' AND t1.destination NOT IN ('CA','CL','MX')
-- where pull_date = '2024-03-28' and exportdate >= '2023-10-01' and exportdate < '2024-01-01' AND t1.destination NOT IN ('CA','CL','MX')
-- where pull_date = '2024-04-11' and exportdate >= '2023-10-01' and exportdate < '2024-01-01' AND t1.destination Not IN ('CA','CL','MX')
where pull_date = '2024-05-01' and exportdate >= '2023-10-01' and exportdate < '2024-01-01' AND t1.destination Not IN ('CA','CL','MX')
and serialnumber not in (select serialnumber from bi_work.dd_exports where exportdate < '2023-10-01');

select date_trunc('quarter', exportdate), count(*),count(distinct serialnumber),count(distinct pull_date)
from bi_work.dd_exports
-- where destination NOT IN ('CA','CL','MX','PR') 
where destination IN ('CA','CL','MX','PR') 
group by 1
order by 1;

-- FINAL OUTPUT FILE - NOT CA / CL / MX / PR
DELETE FROM bi_work.dd_exports_outputfile_final WHERE exportdate >= '2023-10-01' and exportdate < '2024-01-01';
-- WHERE pull_date = TIMEZONE('America/Los_Angeles', TO_TIMESTAMP(GETDATE(),'YYYY-MM-DD HH24:MI:SS'))::DATE;
INSERT INTO bi_work.dd_exports_outputfile_final
-- select serialnumber, count(*) from (
with temp as (
	select 
		* 
	from 
		bi_work.dd_exports
	where 
		exportdate >= '2023-10-01' and exportdate < '2024-01-01' and pull_date = TIMEZONE('America/Los_Angeles', TO_TIMESTAMP(GETDATE(),'YYYY-MM-DD HH24:MI:SS'))::DATE and destination not in ('CA','CL','MX')
	), 

temp2 as (
	select 
		serialnumber, 
		count(*) 
	from 
		temp
	group by 
		1 
	having 
		count(*) > 1
	) 

select 
	* 
from 
	temp
where 
	serialnumber not in (select serialnumber from temp2)
	and serialnumber not in (select serialnumber from bi_work.dd_exports_outputfile_final where exportdate < '2023-10-01')
	and exportprice > 0 and trackingnumber <> ''
order by 
	exportdate, invoice, serialnumber
-- ) group by 1 order by 2 desc
;

select 
select 
	* 
from 
	bi_work.dd_exports_outputfile_final
where 
	exportdate >= '2023-10-01' and exportdate < '2024-01-01' 
-- 	and pull_date = TIMEZONE('America/Los_Angeles', TO_TIMESTAMP(GETDATE(),'YYYY-MM-DD HH24:MI:SS'))::DATE
order by 
	exportdate, invoice, serialnumber;

-- FINAL OUTPUT FOR CA / CL / MX / PR
INSERT INTO bi_work.dd_exports_outputfile_final
-- select serialnumber, count(*) from (
with temp as (
select * from bi_work.dd_exports
where exportdate >= '2023-10-01' and exportdate < '2024-01-01'
and destination in ('CA','CL','MX','PR')
), 

temp2 as (
select serialnumber, count(*) from temp
group by 1 having count(*) > 1
) 

select * from temp
where serialnumber not in (select serialnumber from temp2)
and serialnumber not in (select serialnumber from bi_work.dd_exports_outputfile_final /*where destination not in ('CA','CL','MX','PR')*/)
and exportprice > 0 and trackingnumber <> ''
order by exportdate, invoice, serialnumber
-- ) group by 1 order by 2 desc
;