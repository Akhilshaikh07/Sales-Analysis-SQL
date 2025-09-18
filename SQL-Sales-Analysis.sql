                              -- Sales_Analysis project-- 
                                                
                                                
		 -- CHANGE_OVER_TIME 
 -- 1 analyze sales performance over time -- 


SELECT YEAR(order_date_clean) AS ORDER_YEAR, MONTH(order_date_clean)AS ORDER_MONTHH,
SUM(sales_amount) AS TOTAL_SALES,
COUNT(customer_key) AS TOTAL_CUSTOMERS,
SUM(quantity) AS TOTAL_QUANTITY
FROM sales3
WHERE order_date_clean IS NOT NULL 
GROUP BY YEAR(order_date_clean),MONTH(order_date_clean)
ORDER BY YEAR(order_date_clean),MONTH(order_date_clean);


  -- CUMULATIVE ANALYSIS-- 
  -- 1 calculate the total sales per month--
  -- 2 and the running total of sales over time-- 
  
  
  
  SELECT MONTHH,
  ORDER_YEAR,
  TOTAL_SALES,
  SUM(TOTAL_SALES) OVER (PARTITION BY ORDER_YEAR ORDER BY MONTHH),
  AVG(MOOVING_AVG) OVER (PARTITION BY ORDER_YEAR ORDER BY MONTHH)
  FROM 
  (SELECT MONTH(order_date_clean) AS MONTHH,YEAR(order_date_clean)AS ORDER_YEAR,
  SUM(sales_amount) AS TOTAL_SALES,
  AVG(price) AS MOOVING_AVG
  FROM sales3
  WHERE order_date_clean IS NOT NULL
  GROUP BY MONTH(order_date_clean),YEAR(order_date_clean) 
  ) AS A;


          -- PERFORMANCE ANALYSIS--
   -- Analyze the yearly performance of products
   -- by comparing each products sales to both its 
   -- avrage sales performance and the previous year-- 


WITH A AS (
SELECT YEAR(sales3.order_date_clean) AS ORDER_YEAR,
product2.product_name,
SUM(sales3.sales_amount) AS CURRENT_SALES
FROM sales3
JOIN product2
USING(product_key)
WHERE order_date_clean IS NOT NULL
GROUP BY YEAR(sales3.order_date_clean),product2.product_name
)

SELECT 
ORDER_YEAR,
 product_name,
 CURRENT_SALES,
 ROUND(AVG(CURRENT_SALES) OVER (PARTITION BY product_name),0) AS AVG_SALES,
 CURRENT_SALES-ROUND(AVG(CURRENT_SALES) OVER (PARTITION BY product_name),0) AS diff_avg ,
 CASE
       WHEN   CURRENT_SALES-ROUND(AVG(CURRENT_SALES) OVER (PARTITION BY product_name),0) > "comparision" THEN "above avg"
       WHEN   CURRENT_SALES-ROUND(AVG(CURRENT_SALES) OVER (PARTITION BY product_name),0) < "comparision" THEN "belov avg"
       ELSE "avg"
       END,
 COALESCE(LAG(CURRENT_SALES,1) OVER (PARTITION BY product_name ORDER BY ORDER_YEAR),0) AS previous_year_sales,
 ROUND(COALESCE(((CURRENT_SALES-COALESCE(LAG(CURRENT_SALES,1) OVER (PARTITION BY product_name ORDER BY ORDER_YEAR),0))
 /COALESCE(LAG(CURRENT_SALES,1) OVER (PARTITION BY product_name ORDER BY ORDER_YEAR),0))*100,0),0) AS diff_py,
 CASE 
       WHEN COALESCE(LAG(CURRENT_SALES,1) OVER (PARTITION BY product_name ORDER BY ORDER_YEAR),0) > "previous_year_sales" THEN "INCRACE"
       WHEN COALESCE(LAG(CURRENT_SALES,1) OVER (PARTITION BY product_name ORDER BY ORDER_YEAR),0) < "previous_year_sales" THEN "DICRESE"
       ELSE "NO CHANGE"
 END
 FROM A 
 ORDER BY product_name,ORDER_YEAR;
 
 
        -- PART TO WHOLE PROPORTIONAL-
        
        -- analyze how an individual part is performing compared to the overall
        -- wich category contribut the most to overall sales-- 
        
        WITH A AS (
        SELECT SUM(sales3.sales_amount) AS TOTAL_SALES,
        product2.category
        FROM sales3
        JOIN product2
        USING(product_key)
        GROUP BY  product2.category
        )
       SELECT 
       category,
       TOTAL_SALES,
       SUM(TOTAL_SALES) OVER () AS OVERAL_SALES,
       CONCAT(ROUND((TOTAL_SALES /  SUM(TOTAL_SALES) OVER ())*100,2),"%") AS PERCENTAGE
       FROM A;
        
              
              
            -- DETA SEGMENTATION-- 
            
	  -- Segment products into cost ranges and
      -- count how many products fall into each segment
      
      WITH A AS (
      SELECT 
      product_key,
      product_name,
      cost,
	CASE 
        WHEN cost<100 THEN "bolow 100"
        WHEN cost BETWEEN 100 AND 500 THEN "100-500"
        WHEN cost BETWEEN 500 AND 1000 THEN "500-1000"
        ELSE "above-1000"
	END AS cost_range
     FROM product2        
    )
    SELECT  
      cost_range,
      COUNT(product_key) AS Total_product
      FROM A
	GROUP BY cost_range  
     ORDER BY  Total_product DESC;  
        
        
	-- Group customers into three segments based on their spending behavior
     -- VIP atleast 12 month of history and spending more than 5,000   
       -- REGULAR atleast 12 month of history but spending = 5,000--   
        -- NEW lifespan less  then 12 month-- 
        
        
        WITH A AS(
        SELECT 
        SUM(sales3.sales_amount) AS TOTAL_SPENT,
        customer1.customer_key,
        MIN(sales3.order_date_clean) AS FIRST_ORDER,
        MAX(sales3.order_date_clean) AS LAST_ORDER,
         TIMESTAMPDIFF(MONTH, MIN(sales3.order_date_clean), MAX(sales3.order_date_clean)) AS lifespan_months
        FROM sales3
        JOIN customer1
        USING(customer_key)
             WHERE sales3.order_date_clean IS NOT NULL
        GROUP BY customer1.customer_key
        )
        
        SELECT 
        CUSTOMER_SIGMENT,
        COUNT(customer_key) AS Total_customers
        FROM
       (SELECT  
        customer_key,
        TOTAL_SPENT,
        lifespan_months,
        CASE 
            WHEN lifespan_months >=12 AND TOTAL_SPENT>5000 THEN "VIP"
            WHEN lifespan_months>=12 AND TOTAL_SPENT <=5000 THEN "Regular"
            ELSE "New"
	END AS CUSTOMER_SIGMENT
        FROM A) AS AA
        GROUP BY CUSTOMER_SIGMENT
        ORDER BY Total_customers DESC;
     

      -- CUSTOMER REPORT-- 
      
      -- perpose
            -- this report consolidates key customers metrics and behavior 
	 -- Highlighth
              -- gathers  essential  fields such as names ages and transction detail 
              --  and age group 
              -- total order 
              -- total sales
              -- total quantity purches
              -- total product
              -- lifespan (in month)
	-- calculate valueble KPIs
              -- avarge order value 
              -- average monthly spent
         CREATE VIEW customer_report AS      
         WITH A AS (
         -- base query: retrieves core coulmn  from table
              SELECT
				sales3.order_number,
                 sales3. product_key,
                 sales3. sales_amount,
                 sales3.order_date,
                  sales3.quantity,
                 customer1. customer_key ,
                  customer1.customer_number,
                 CONCAT( customer1. first_name, " ",customer1. last_name) AS customer_name,
                 customer1.birthdate_clear,
               TIMESTAMPDIFF(YEAR, customer1.birthdate_clear, CURDATE()) AS AGE
                 FROM sales3
                 JOIN customer1
                 USING(customer_key)
                 WHERE order_date IS NOT NULL )
           , customer_aggregation AS (
              
              SELECT 
         customer_key,
         customer_number,
         customer_name,
         AGE,
         birthdate_clear,
         COUNT(DISTINCT(customer_number)) AS Total_customer,
         SUM(sales_amount) AS total_sales,
         SUM(quantity) as toatl_quantity,
         COUNT(DISTINCT(product_key)) AS total_product,
          TIMESTAMPDIFF(MONTH,MIN(birthdate_clear), MAX(birthdate_clear)) AS Life_span
         FROM A
         GROUP BY
         customer_key,
         customer_number,
         customer_name,
         AGE,
		birthdate_clear
        )
        SELECT 
          customer_key,
         customer_number,
         customer_name,
         AGE,
	CASE 
       WHEN AGE < 20 THEN "Under-20"
       WHEN AGE BETWEEN 20 AND 29 THEN "20-29"
       WHEN AGE BETWEEN 29 AND 39 THEN "29-39"
	  WHEN AGE BETWEEN 39 AND 49 THEN "39-49"
       ELSE "50 AND ABOVE"
       END AS Age_Group,
         birthdate_clear,
       Total_customer,
       total_sales,
         toatl_quantity,
        total_product,
        Life_span,
        -- Avrage_order_value-- 
        total_sales/Total_customer AS avg_VALUE,
          -- Avrage_monthly_spend-- 
CASE
    WHEN Life_span >=0 THEN total_sales
    ELSE Life_span/total_sales
    END Avg_monthly_spend
        FROM customer_aggregation ;
         
        
        