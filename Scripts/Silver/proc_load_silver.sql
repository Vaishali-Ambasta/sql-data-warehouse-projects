/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/



EXEC silver.load_silver;

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        print '=======================================';
        print 'Loading Silver Layer';
        print '=======================================';

        print '---------------------------------------';
        print 'Loading CRM Table';
        print '---------------------------------------';

        SET @start_time = GETDATE();
    print '>>Truncating the table crm_cust_info';
    truncate table silver.crm_cust_info; 
    print '>>Inserting into the table crm_cust_info';

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

    SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        print '----------------------------------------------------'

        SET @start_time = GETDATE();

    print '>>Truncating the table crm_prd_info';
    truncate table silver.crm_prd_info; 
    print '>>Inserting into the table crm_prd_info';

    insert into silver.crm_prd_info
    (
        prd_id         
        ,cat_id     
        ,prd_key         
        ,prd_nm        
        ,prd_cost      
        ,prd_line       
        ,prd_start_dt    
        ,prd_end_dt 
     )
    SELECT prd_id
          ,REPLACE(SUBSTRING(prd_key,1,5),'-', '_') as cat_id
          ,SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key
          ,prd_nm
          ,isnull(prd_cost,0) as prd_cost
          ,Case when upper(trim(prd_line)) = 'M' then 'Mountain'
                when upper(trim(prd_line)) = 'R' then 'Road'
                when upper(trim(prd_line)) = 'S' then 'Other Sales'
                when upper(trim(prd_line)) = 'T' then 'Toring'
                else 'n/a'
            end as prd_line
          ,cast(prd_start_dt as date) as prd_start_dt
          ,cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as date) 
          as prd_end_dt
      FROM DataWarehouse.bronze.crm_prd_info;

      SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        print '----------------------------------------------------'

        SET @start_time = GETDATE();

    print '>>Truncating the table crm_sales_details';
    truncate table silver.crm_sales_details; 
    print '>>Inserting into the table crm_sales_details';

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
              SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        print '----------------------------------------------------'

        print '---------------------------------------';
        print 'Loading ERP Table';
        print '---------------------------------------';

        SET @start_time = GETDATE();


     print '>>Truncating the table erp_cust_az12';
    truncate table silver.erp_cust_az12; 
    print '>>Inserting into the table erp_cust_az12';

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
    SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        print '----------------------------------------------------'

        SET @start_time = GETDATE();

    print '>>Truncating the table erp_loc_a101';
    truncate table silver.erp_loc_a101; 
    print '>>Inserting into the table erp_loc_a101';

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
    SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        print '----------------------------------------------------'

        SET @start_time = GETDATE();

    print '>>Truncating the table erp_px_cat_g1v2';
    truncate table silver.erp_px_cat_g1v2; 
    print '>>Inserting into the table erp_px_cat_g1v2';


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
    SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        print '----------------------------------------------------'

        SET @batch_end_time = GETDATE();
        PRINT '====================================================='
        PRINT 'Loading Silver layer is completed'
        PRINT '   >>Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '====================================================='
        END TRY
        BEGIN CATCH
            PRINT '============================================'
            PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
            PRINT 'Error Message' + ERROR_MESSAGE();
            PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
            PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
            PRINT '============================================'
        END CATCH
END;
