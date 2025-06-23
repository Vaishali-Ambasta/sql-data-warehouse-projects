insert into silver.erp_loc_a101(
cid,
cntry
)
select
replace(cid, '-', '') as cid ,
case when trim(cntry) = 'DE' then 'Germany'
	when trim(cntry) in ('US', 'USA') THEN 'United States'
	when trim(cntry) = '' or cntry is null then 'n/a'
	else trim(cntry)
	end as
cntry
from bronze.erp_loc_a101;

select cst_key from silver.crm_cust_info;

select distinct cntry from bronze.erp_loc_a101;

select * from silver.erp_loc_a101;