INSERT INTO silver.erp_cust_az12(
cid,
bdate,
gen)
SELECT
case when cid like 'NAS%' THEN substring(cid, 4, len(cid))
ELSE cid
end as cid,
case when bdate > getdate() then null
else bdate 
end as bdate,
case when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
when upper(trim(gen)) in ('M', 'MALE') then 'Male'
ELSE 'n/a'
end as gen 
FROM bronze.erp_cust_az12;


