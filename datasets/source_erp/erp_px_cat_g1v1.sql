insert into silver.erp_px_cat_g1v2(
id,
cat,
subcat,
maintenance
)
select id,
cat,
subcat,
maintenance
from bronze.erp_px_cat_g1v2;

select prd_key from silver.crm_prd_info;

select * from bronze.erp_px_cat_g1v2
where cat != trim(cat) or subcat != trim(subcat) or maintenance != trim(maintenance);

select distinct cat from bronze.erp_px_cat_g1v2;

select distinct subcat from bronze.erp_px_cat_g1v2;

select distinct maintenance from bronze.erp_px_cat_g1v2;