--Module test 1  ===============================================

WITH CTE_OPEN as (
    SELECT user_id, MAX(timestamp) as open_time 
    FROM facebook_session
    WHERE action = "page_load"
    GROUP BY user_id
), CTE_CLOSE as (
    SELECT user_id, MAX(timestamp) as close_time 
    FROM facebook_session
    WHERE action = "page_exit"
    GROUP BY user_id
)
SELECT o.user_id, timediff(c.close_time,o.open_time) as session_time
FROM CTE_OPEN o
LEFT JOIN CTE_CLOSE c
ON o.user_id = c.user_id
WHERE o.open_time is not NULL and c.close_time is not NULL
ORDER BY o.user_id

SELECT Id,
case 
    when p_id is null then "Root"
    when Id in (SELECT p_id FROM tree) then "Inner"
    else "Leaf"
end as Type
FROM tree
ORDER BY Id

SELECT product_id, SUM(quantity) as total_quantity 
FROM sales 
GROUP BY product_id
ORDER BY product_id

WITH CTE1 as(
    SELECT DISTINCT user FROM (
        SELECT user1 as user FROM facebook
        UNION 
        SELECT user2 as user FROM facebook
    ) X
)
SELECT us.user as user1,ROUND((count(us.user)/ (SELECT count(*) FROM CTE1)    *100),3  )as popularity_percent
FROM CTE1 us 
JOIN facebook f 
ON us.user = f.user1 or us.user = f.user2
GROUP BY user
ORDER BY popularity_percent DESC, us.user

SELECT e.emp_name, e.department,e.salary as employee_salary, m.salary as manager_salary
FROM employee e 
JOIN employee m ON e.manager_id = m.emp_id
WHERE e.salary>m.salary
ORDER BY e.emp_name DESC

SELECT s.customer_id as c_id, SUM(m.price) as total_spent
FROM sales s
LEFT JOIN menu m ON s.product_id = m.product_id
GROUP BY s.customer_id
ORDER BY total_spent DESC
--==============================================================
