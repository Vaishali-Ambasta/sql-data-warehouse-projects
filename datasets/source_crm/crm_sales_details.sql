INSERT INTO silver.crm_sales_details(
        sls_ord_num
      ,sls_prd_key
      ,sls_cust_id
      ,sls_order_dt
      ,sls_ship_dt
      ,sls_due_dt
      ,sls_sales
      ,sls_quantity
      ,sls_price
 )
SELECT sls_ord_num
      ,sls_prd_key
      ,sls_cust_id
      , case when sls_order_dt = 0 or len(sls_order_dt )!=8 then null
            else cast(cast(sls_order_dt as varchar) as date)
        end as sls_order_dt
      , case when sls_ship_dt = 0 or len(sls_ship_dt )!=8 then null
            else cast(cast(sls_ship_dt as varchar) as date)
        end as sls_ship_dt
      , case when sls_due_dt = 0 or len(sls_due_dt )!=8 then null
            else cast(cast(sls_due_dt as varchar) as date)
        end as sls_due_dt
      ,case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price)
      then sls_quantity * ABS(sls_price)
      ELSE 
      sls_sales
      END AS sls_sales
      ,sls_quantity
      ,case when sls_price is null or sls_price <= 0
      then sls_sales / nullif(sls_quantity,0)
      else sls_price
      end as sls_price
  FROM DataWarehouse.bronze.crm_sales_details;


  where sls_cust_id not in (select cst_id from silver.crm_cust_info);

  select * from silver.crm_cust_info;

  select nullif(sls_due_dt,0) sls_due_dt from silver.crm_sales_details
  where sls_due_dt < = 0 
  or len(sls_due_dt )!=8 
  or sls_due_dt > 20500101
  or sls_due_dt < 19000101;

  select * from silver.crm_sales_details
  where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt;

  select distinct sls_sales AS old_sls_sales
      ,sls_quantity
      ,sls_price AS old_sls_price
      ,case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price)
      then sls_quantity * ABS(sls_price)
      ELSE 
      sls_sales
      END AS sls_sales
      ,case when sls_price is null or sls_price <= 0
      then sls_sales / nullif(sls_quantity,0)
      else sls_price
      end as sls_price

      from bronze.crm_sales_details
      where sls_sales != sls_quantity*sls_price 
      or sls_sales is null or  sls_quantity is null or sls_price is null
      or sls_sales <=0 or  sls_quantity <=0 or sls_price <=0
      order by sls_sales,sls_quantity, sls_price;

 --where sls_ord_num != trim(sls_ord_num);

 IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO

CREATE TABLE silver.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt DATE,
    sls_ship_dt  DATE,
    sls_due_dt   DATE,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);
GO

SELECT * FROM silver.crm_sales_details;