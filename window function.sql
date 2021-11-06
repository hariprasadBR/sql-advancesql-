/*
RANK(): Rank of the current row within its partition, with gaps
DENSE_RANK(): Rank of the current row within its partition, without gaps
PERCENT_RANK(): Percentage rank value, which always lies between 0 and 1
*/


-- using rank function 
SELECT Customer_Name,
		ROUND(sales,2) as rounded_sales,
        RANK() OVER(ORDER BY sales DESC) AS sales_rank
FROM market_fact_full AS m
INNER JOIN cust_dimen AS c
ON m.cust_id=c.cust_id
WHERE customer_name='RICK WILSON';

-- now on same examples and call out top 10 sales from customer  by common table expression.

WITH /* common table name*/ rank_info AS
(
SELECT customer_name,
		ord_id,
        Round(sales,2) AS rounded_sales,
        RANK() OVER(ORDER BY sales DESC) AS sales_rank
FROM market_fact_full AS m
INNER JOIN cust_dimen AS c
ON m.cust_id=c.cust_id
WHERE customer_name='RICK WILSON'
)
SELECT *
FROM rank_info
WHERE sales_rank<=10;


WITH /* common table name*/ rank_info AS
(
SELECT customer_name,
		ord_id,
        Round(sales,2) AS rounded_sales,
        RANK() OVER(ORDER BY sales DESC) AS sales_rank
FROM market_fact_full AS m
INNER JOIN cust_dimen AS c
ON m.cust_id=c.cust_id
WHERE customer_name='RICK WILSON'
)
SELECT customer_name,sales_rank
FROM rank_info
WHERE sales_rank<=10;

-- Example for Ddense rank and rank comparision

SELECT ord_id,
		discount,
        customer_name,
        RANK() OVER(ORDER BY discount DESC)AS Discount_rank,
        DENSE_RANK() OVER(ORDER BY discount DESC) AS Discount_dense_rank
        
        
FROM market_fact_full AS m
INNER JOIN cust_dimen AS c
ON m.cust_id=c.cust_id
WHERE customer_name='RICK WILSON';

-- row number dense rank percent rank
/*
you can use the 'row number' function for the following use cases:

To determine the top 10 selling products out of a large variety of products
To determine the top three winners in a car race
To find the top five areas in different cities in terms of GDP growth
 

The main advantage of the 'row number' function over all the other types of rank functions is that it returns unique values
*/


SELECt customer_name,
		COUNT(DISTINCT ord_id) AS ord_count,
        RANK() OVER(ORDER BY COUNT(DISTINCT ord_id) DESC) AS ord_count_rank,
        DENSE_RANK() OVER(ORDER BY COUNT(DISTINCT ord_id) DESC) AS ord_count_dense_rank,
        PERCENT_RANK() OVER(ORDER BY COUNT(DISTINCT ord_id) DESC) AS ord_count_percent_rank,
        ROW_NUMBER() OVER(ORDER BY COUNT(DISTINCT ord_id) DESC) AS ord_count_row_number
FROM market_fact_full AS m
INNER JOIN cust_dimen AS c
ON m.cust_id=c.cust_id
GROUP BY customer_name;

-- partitioning example
/*
when we use categorical column there are 3 to four categories and rank should be reset after each category is completed,
for each category separate ranking is given
*/

WITH shipping_summary AS
(
SELECT ship_mode,
		MONTH(ship_date) AS shipping_month,
        COUNT(*) AS shipments
FROM shipping_dimen
GROUP BY ship_mode,
		MONTH(ship_date)
)
SELECT *,
		RANK() OVER(PARTITION BY ship_mode ORDER BY shipments DESC) AS shipping_rank,
        DENSE_RANK() OVER(PARTITION BY ship_mode ORDER BY shipments DESC) AS shipping_desnse_rank,
		ROW_NUMBER() OVER(ORDER BY shipments DESC) AS shipping_row_number
FROM shipping_summary;

-- introduce  to window function
/*
So, as you learnt in this video, the same window can be used to define multiple 'over' clauses.  
You can define the window once, give it a name and then refer to the name in the 'over' clauses. 
A named window makes it easier to experiment with multiple window definitions and observe their effects on the query result
You only need to modify the window definition in the 'window' clause, rather than using multiple 'over' clause definitions.
*/


SELECT ord_id,
		discount,
        customer_name,
        RANK() OVER w AS disc_rank,
        DENSE_RANK() OVER w AS disc_dense_rank,
        ROW_NUMBER() OVER w AS disc_row_number
        
FROM market_fact_full AS m
INNER JOIN  cust_dimen AS c
ON m.Cust_id=c.Cust_id
WINDOW w AS (PARTITION BY customer_name ORDER BY discount DESC);

-- moving average
WITH shipping_summary AS
(
SELECT ship_date,
		SUM(Shipping_Cost) AS daily_total
FROM market_fact_full AS m
INNER JOIN shipping_dimen AS s
ON m.Ship_id=s.Ship_id
GROUP BY Ship_Date
)
SELECT *,
		SUM(daily_total) OVER w1 AS running_total,
        AVG(daily_total) OVER w2 AS running_avg
FROM shipping_summary
WINDOW w1 AS(ORDER BY daily_total ROWS UNBOUNDED PRECEDING),
w2 AS (ORDER BY daily_total ROWS 6 PRECEDING);

-- lead and lag

WITH customer_ord AS 
(
SELECT c.customer_name,
		m.ord_id,
        o.order_date
FROM market_fact_full AS m
LEFT JOIN orders_dimen AS o
ON m.ord_id=o.ord_id
LEFT JOIN cust_dimen AS c
ON m.Cust_id=c.Cust_id
WHERE customer_name='RICK WILSON'
GROUP BY 
		c.customer_name,
        m.ord_id,
        o.order_date
		
)
SELECT *,
		LEAD(order_date,1) OVER(ORDER BY order_date,ord_id) AS next_order_date
FROM customer_ord 
GROUP BY customer_name,
        ord_id,
        order_date;



WITH customer_ord AS 
(
SELECT c.customer_name,
		m.ord_id,
        o.order_date
FROM market_fact_full AS m
LEFT JOIN orders_dimen AS o
ON m.ord_id=o.ord_id
LEFT JOIN cust_dimen AS c
ON m.Cust_id=c.Cust_id
WHERE customer_name='RICK WILSON'
GROUP BY 
		c.customer_name,
        m.ord_id,
        o.order_date
		
),
next_date_summary AS
(
SELECT *,
		LEAD(order_date,1,'2015-01-01') OVER(ORDER BY order_date,ord_id) AS next_order_date
FROM customer_ord 
GROUP BY customer_name,
        ord_id,
        order_date
)
SELECT *,
		DATEDIFF(next_order_date,order_date) as days_diff
FROM next_date_summary;


-- lag


WITH customer_ord AS 
(
SELECT c.customer_name,
		m.ord_id,
        o.order_date
FROM market_fact_full AS m
LEFT JOIN orders_dimen AS o
ON m.ord_id=o.ord_id
LEFT JOIN cust_dimen AS c
ON m.Cust_id=c.Cust_id
WHERE customer_name='RICK WILSON'
GROUP BY 
		c.customer_name,
        m.ord_id,
        o.order_date
		
),
next_date_summary AS
(
SELECT *,
		LAG(order_date,1,'2015-01-01') OVER(ORDER BY order_date,ord_id) AS next_order_date
FROM customer_ord 
GROUP BY customer_name,
        ord_id,
        order_date
)
SELECT *,
		DATEDIFF(order_date,next_order_date) as days_diff
FROM next_date_summary;

/*
Rank functions: The different types of rank functions are as follows:

RANK(): Rank of the current row within its partition, with gaps
DENSE_RANK(): Rank of the current row within its partition, without gaps
PERCENT_RANK(): Percentage rank value; it will always lie between 0 and 1
ROW_NUMBER(): Assigns unique numeric values to each row, starting from 1

Named windows: A named window makes it easier to define and reuse multiple window functions. 

Order of SQL statements: The order in which the various SQL statements appear in a query is as follows:

SELECT
FROM
JOIN
WHERE
GROUP BY
HAVING
WINDOW
ORDER BY

Frames: Frames are used to subset a set of consecutive rows and calculate moving averages.
 A query using a frame has multiple components as shown in the diagram given below.
 
 Lead and lag functions: These functions are used to compare a row value with the next or the previous row value. The syntax for the 'lead' function is as follows:
 */
 
 
-- Case statements and cursors
/*
 profit<-500 -->huge_loss
 profit -500 to 0 -->bearable loss
 profit 0 to 500 --> decent profit
 profit >500 -->huge profit
 */
 
 SELECT market_fact_id,
		profit,
        CASE
			WHEN profit <-500 THEN 'Huge Loss'
            WHEN profit Between -500 AND 0 THEN 'Bearable Loss'
            WHEN profit Between 0 AND 500 THEN 'Decent Profit'
            ELSE 'Great Profit'
		END AS profit_type
FROM market_fact_full;


WITH cust_summary AS
(
	SELECT m.cust_id,
		c.customer_name,
		ROUND(SUM(m.sales)) AS total_sales,
		PERCENT_RANK() OVER(ORDER BY ROUND(SUM(m.sales)) DESC) AS perc_rank
	FROM market_fact_full AS m
	LEFT JOIN cust_dimen AS c
	ON m.cust_id=c.cust_id
	GROUP BY cust_id
 )
 SELECT *,
		CASE 
			WHEN perc_rank<0.1 THEN 'GOLD'
            WHEN perc_rank<0.5 THEN 'SILVER'
            ELSE 'BRONZE'
		END AS customer_category
FROM cust_summary;

-- user define function

DELIMITER $$

 CREATE  FUNCTION profitType(profit INT)
 RETURNS VARCHAR(30) DETERMINISTIC
 
 BEGIN
 
 DECLARE message VARCHAR(30);
 IF profit<-500 THEN 
		SET message ='HUGE LOSS';
ELSEIF profit BETWEEN -500 AND 0 THEN 
		SET message ='Bearable LOSS';
ELSEIF profit BETWEEN 0 AND 500 THEN 
		SET message='DECENT PROFIT';
ELSE
	SET message='HUGE PROFIT';
END IF;

RETURN message;
END;
$$
DELIMITER ;

SELECT profitType(40) AS profit;

DELIMITER $$

CREATE PROCEDURE get_sales_customer(sales_input INT)

BEGIN
	SELECT DISTINCT cust_id,
				ROUND(sales) AS sales_amount
	FROM 
		market_fact_full
	WHERE ROUND(sales)>sales_input
    ORDER BY sales;
END $$
DELIMITER ;
  
call get_sales_customer(300)