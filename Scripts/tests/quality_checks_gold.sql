/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'gold.dim_customers'
-- ====================================================================
-- Check for Uniqueness of Customer Key in gold.dim_customers
-- Expectation: No results 
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.product_key'
-- ====================================================================
-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results 

select prd_key, count(*) from 
(
SELECT pn.prd_id
      ,pn.cat_id
      ,pn.prd_key
      ,pn.prd_nm
      ,pn.prd_cost
      ,pn.prd_line  
      ,pn.prd_start_dt
      ,pc.cat
      ,pc.subcat
      ,pc.maintenance
  FROM DataWarehouse.silver.crm_prd_info pn
  left join silver.erp_px_cat_g1v2 pc
  on pn.cat_id = pc.id
  where prd_end_dt is null
  ) t group by prd_key
  having count(*) >1;

-- ====================================================================
-- Checking 'gold.fact_sales'
-- ====================================================================
-- Check the data model connectivity between fact and dimensions

select * from gold.fact_sales f
 left join gold.dim_customers c
 on c.customer_key = f.customer_key
 left join gold.dim_products p
 on p.product_key = f.product_key
 where c.customer_key is null
 or p.product_key is null;
