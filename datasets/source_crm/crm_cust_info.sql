-- Check for Nulls or Duplicates in primary key

select cst_id, count(*) 
from silver.crm_cust_info 
group by cst_id 
having count(*)>1 or cst_id is null;

INSERT INTO silver.crm_cust_info(
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date)
select cst_id,
cst_key,
TRIM(cst_firstname) AS cst_first_name,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN upper(trim(cst_marital_status)) = 'S' then 'Single'
	 when upper(trim(cst_marital_status)) = 'M' then 'Married'
	 else 'n/a'
end cst_marital_status,
CASE WHEN upper(trim(cst_gndr)) = 'F' then 'Female'
	 when upper(trim(cst_gndr)) = 'M' then 'Male'
	 else 'n/a'
end cst_gndr,
cst_create_date
from bronze.crm_cust_info;

-- check unwanted spaces

select cst_firstname from silver.crm_cust_info
where cst_firstname != trim(cst_firstname);

select cst_lastname from silver.crm_cust_info
where cst_lastname != trim(cst_lastname);

----for distinct gender and marital status

select distinct cst_gndr from silver.crm_cust_info;
select distinct cst_marital_status from silver.crm_cust_info;

select * from silver.crm_cust_info;

--------------------------------------------------------

select * from bronze.crm_prd_info;

select prd_id, count(*) 
from bronze.crm_prd_info 
group by prd_id 
having count(*)>1 or prd_id is null;

SELECT prd_id,
    SUBSTRING(PRD_KEY,1,5) AS cat_id,

      prd_key
      ,prd_nm
      ,prd_cost
      ,prd_line
      ,prd_start_dt
      ,prd_end_dt
  FROM DataWarehouse.bronze.crm_prd_info;