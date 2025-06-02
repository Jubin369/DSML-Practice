--Day 4 - Filtering and Subqueries==============================
SELECT original_title, tagline, director 
FROM movies 
order by popularity DESC 
limit 5

SELECT 
    original_title, 
    round((vote_count/(vote_count+104.0)*vote_average)+(104.0/(104.0+vote_count)*5.97),2 )as Weighted_avg_rating 
FROM movies 
ORDER BY Weighted_avg_rating DESC, original_title ASC 
LIMIT 10

SELECT 
concat("Book Name:",book_name,",Author:",author,",Book price:",book_price)as Book_Description 
FROM books 
ORDER BY Book_Description

SELECT *, 
    round((salary*20/100)+salary) as New_salary 
FROM employees 
ORDER BY emp_id

SELECT * 
FROM job_history 
WHERE employee_id <> 101 
ORDER BY employee_id, job_id

SELECT 
    original_title 
FROM movies 
WHERE release_year > 2014 and vote_average > 7 
ORDER BY original_title

SELECT 
    name,population,area 
FROM world 
WHERE area >= 3000000 or population >= 25000000 
ORDER BY name

SELECT 
    orderNumber, requiredDate 
FROM orders 
WHERE orderDate >= "2003-12-01" and orderDate <= "2003-12-31" and status = "Shipped" 
ORDER BY orderNumber

SELECT * 
FROM employees 
WHERE last_name IN ("King", "Taylor", "Grant") 
ORDER BY employee_id

SELECT 
    original_title, director, genres, cast, budget, revenue, runtime, vote_average 
FROM movies 
WHERE keywords LIKE '%sport%' OR keywords LIKE '%sequel%' OR keywords LIKE '%suspense%' 
ORDER BY original_title

SELECT 
    DISTINCT author_id as id 
FROM views 
WHERE author_id = viewer_id 
ORDER BY id

SELECT 
    employee_id, first_name, last_name, salary 
FROM employees 
WHERE 
    department_id in (10,50,80) and 
    salary>= 5000 and salary <= 10000 and 
    commission_pct is NULL 
ORDER BY employee_id 

SELECT * 
FROM movies 
order by revenue desc 
limit 1 
offset 2

SELECT 
    employee_number, first_name, last_name, salary 
FROM employees 
ORDER BY salary DESC 
LIMIT 2 
OFFSET 3 

SELECT * 
FROM city 
WHERE countrycode = "JPN" 
ORDER BY id

SELECT 
    employee_id, concat(first_name," ",last_name) as full_name, phone_number 
FROM employees 
WHERE first_name LIKE "%n" 
ORDER BY employee_id

SELECT 
    employee_id, first_name, last_name, job_id, manager_id 
FROM employees 
WHERE department_id is NULL 
ORDER BY employee_id

SELECT 
    DISTINCT user_id 
FROM purchases 
WHERE time_stamp between "2022-03-08" and "2022-03-20" 
    and amount >= 1000 
ORDER BY user_id
--==============================================================

--Day 5 - Subqueries continued, Group By and Aggregation========

SELECT 
    employee_id, if(isNULL(commission_pct),0,round(commission_pct,2)) as commission_pct 
FROM employees 
ORDER BY employee_id

SELECT 
    employee_id, 
    concat(first_name," ",last_name) as full_name, 
    salary 
FROM employees 
WHERE department_id IN(
    select department_id 
    from departments 
    where lower(department_name) IN ("administration","marketing","human resources")
) 
ORDER BY employee_id

SELECT * 
FROM employees 
WHERE employee_id not IN (
    select employee_id 
    from job_history 
    where employee_id=employee_id
) 
ORDER BY employee_id

select 
    concat(first_name,' ', last_name) as 'full_name',
    salary, 
    department_id, 
    job_id 
from employees 
where job_id = 
    (
        select job_id 
        from employees 
        where employee_id = 107
    )
order by full_name;

SELECT 
    employee_id, 
    salary,
    case 
        when salary > 20000 then "Class A"
        when salary between 10000 and 20000 then "Class B"
        else "Class C"
    end as Salary_bin   
FROM employees 
ORDER BY employee_id

SELECT employee_id, first_name, last_name, salary,
    case 
        when job_id = "FI_ACCOUNT" or job_id ="AC_ACCOUNT" then 1
        else 0
    end as Accountant 
FROM employees 
ORDER BY employee_id

SELECT x, y, z, 
    case 
        when (x+y>z) and (y+z>x) and (x+z>y) then "Yes"
        else "No"
    end as triangle
FROM triangle 
ORDER BY x, y, z

SELECT round(sum(long_w),4) as sum 
FROM station 
WHERE long_w > 38.7880 and long_w < 137.2345

SELECT (salary * months) as earnings, 
count(*) AS num_employees 
FROM employee 
GROUP BY earnings 
ORDER BY earnings DESC 
LIMIT 1;

SELECT MAX(num) AS num
FROM (
    SELECT num
    FROM mynumbers
    GROUP BY num
    HAVING COUNT(num) = 1
) AS t;

SELECT employeeNumber, firstName, lastName, 
IFNULL(email, 'N/A') AS email, 
IFNULL(phone, 'N/A') AS phone
FROM employees
ORDER BY employeeNumber;

select employee_id, first_name, last_name, job_id 
from employees 
where department_id in 
(
    select department_id 
    from departments 
    where location_id in 
    (
        select location_id from locations 
        where city = 'Seattle'
    )
)
order by employee_id;

SELECT 
 COUNT(DISTINCT customer_id) AS rich_count 
FROM store 
WHERE amount > 500;

SELECT 
 original_title, 
 ROUND((((revenue-budget)/budget)*100),2) AS 'Profit_percentage' 
FROM movies;

select employee_number,
concat(upper(SUBSTR(first_name,1,1)), lower(SUBSTR(first_name,2)),' ', lower(last_name)) as "Full Name"
from employees
order by employee_number;

SELECT name 
FROM students 
WHERE marks > 75 
ORDER BY name, id;
--==============================================================

--Day 6 - Joins=================================================

SELECT 
    round(max(lat_n),4) as max 
FROM station 
WHERE lat_n < 138.2523

SELECT cate_id, SUM(receive_qty*purch_price)   
FROM purchase              
GROUP BY cate_id;

SELECT actor_id, director_id 
FROM actordirector 
GROUP BY actor_id,director_id 
HAVING count(*) >=3 
ORDER BY actor_id

SELECT * 
FROM employees 
WHERE salary IN (
    SELECT max(salary) 
    FROM employees 
    WHERE salary < (
        SELECT max(salary) 
        FROM employees 
        WHERE salary < (
            SELECT max(salary) 
            FROM employees
        )
    )
)

SELECT concat(first_name," ",last_name) as full_name 
FROM employees 
WHERE employee_id IN (
    SELECT manager_id 
    FROM employees 
    GROUP BY manager_id 
    HAVING count(*)>= 4
)
ORDER BY full_name

SELECT * 
FROM departments 
WHERE department_id IN (
    SELECT department_id 
    FROM employees 
    GROUP BY department_id 
    HAVING min(salary) >= 9000
)
ORDER BY department_id

SELECT p.product_name, s.year, s.price  
FROM product p
JOIN sales s
using (product_id)
ORDER BY s.year, p.product_name

--==============================================================
--Approach 1:
select d.department_id, d.department_name 
from departments d 
left join employees e 
on d.department_id = e.department_id 
WHERE e.department_id is null
order by d.department_id;

--Approach 2:
SELECT * 
FROM departments 
WHERE department_id NOT IN (
    SELECT department_id 
    FROM employees 
    WHERE department_id=department_id
)
ORDER BY department_id
--==============================================================

--==============================================================
--Approach 1:
SELECT candidate_id
FROM candidates
WHERE skill IN ('Python', 'Tableau', 'MySQL')
GROUP BY candidate_id
HAVING COUNT(DISTINCT skill) = 3
ORDER BY candidate_id;

--Approach 2:
SELECT candidate_id 
FROM
(SELECT candidate_id, COUNT(*) AS skills 
 FROM candidates
 WHERE skill IN ('Python','Tableau','MySQL')
 GROUP BY candidate_id) a
WHERE skills = 3
ORDER BY candidate_id;
--==============================================================

--==============================================================
--Approach 1:
select query_name, 
round(sum(rating/position)/count(query_name),2) as quality,
round(sum(case when rating < 3 then 1 else 0 end)*100/count(query_name),2) as poor_query_percentage
from queries
group by query_name
ORDER BY query_name;

--Approach 2:
SELECT
query_name,
ROUND(AVG(rating/position), 2) AS quality,
ROUND(AVG(rating < 3)*100, 2) AS poor_query_percentage
FROM queries
GROUP BY query_name
ORDER BY query_name;
--==============================================================

select department_id as Department, 
count(*) as No_of_employees,
case 
when count(*)=1 then "Junior Department"
when count(*)<=4 then "Intermediate Department"
when count(*)> 4 then "Senior Department"
end as Department_level
from employees
group by department_id 
order by No_of_employees,Department ;

--==============================================================

--Day 7 - Practice Group By and Aggregation , Joins=============
--==============================================================
--Approach 1:
SELECT DISTINCT c.title
FROM TVProgram t
JOIN Content c
ON t.content_id  = c.content_id   
WHERE 
 t.program_date LIKE '2020-06-%' 
 AND c.Kids_content = 'Y' 
 AND c.content_type = 'Movies'
ORDER BY c.title;

--Approach 2:
SELECT DISTINCT c.title 
FROM Content c
JOIN TVProgram t
using (content_id)
WHERE t.program_date between "2020-06-01" and "2020-06-31" and c.Kids_content ="Y" and c.content_type = "Movies"
ORDER BY c.title
--==============================================================

select city.name
from country 
inner join city 
on country.code = city.countryCode
where country.continent = 'Africa'
order by city.name;

SELECT e.name, b.bonus 
FROM employee e
LEFT JOIN bonus b
using(empId)
WHERE b.bonus < 1000 or b.bonus IS NULL
ORDER BY b.bonus

SELECT e.employeeNumber, e.firstName, e.lastName  
FROM employees e
LEFT JOIN customers c
ON e.employeeNumber = c.salesRepEmployeeNumber
WHERE c.customerNumber is NULL 
ORDER BY e.employeeNumber

SELECT e.employee_id, e.first_name, e.last_name, d.department_id, d.department_name  
FROM employees e
RIGHT JOIN departments d
using(department_id)
ORDER BY e.employee_id, d.department_id, e.first_name

SELECT DISTINCT s.buyer_id
FROM Sales s
JOIN Product p
USING(product_id)
WHERE p.product_name = 'S8'
AND s.buyer_id NOT IN (SELECT DISTINCT buyer_id FROM Sales JOIN Product USING(product_id) WHERE product_name = 'iPhone')
ORDER BY s.buyer_id

SELECT 
    c.customer_id, 
    concat (c.customer_first_name," ",c.customer_last_name) as customer_name,
    round(avg(cp.quantity* cp.cost_to_customer_per_qty),2) as avg_amount,
    case 
        when avg(cp.quantity* cp.cost_to_customer_per_qty) < 7 then "Low Spender"
        when avg(cp.quantity* cp.cost_to_customer_per_qty) between 7 and 10 then "Medium Spender"
        when avg(cp.quantity* cp.cost_to_customer_per_qty) > 10 then "High Spender"
    end as spend_category
FROM customer_purchases cp
JOIN customer c
USING(customer_id)
GROUP BY c.customer_id, customer_name
ORDER BY c.customer_id

SELECT p.productCode, p.productName,pl.textDescription
FROM products p
JOIN productlines pl
using (productLine)
ORDER BY p.productCode

SELECT session_id
FROM Playback p 
LEFT JOIN Ads a ON a.customer_id = p.customer_id AND a.timestamp BETWEEN p.start_time AND p.end_time
WHERE a.customer_id IS NULL
ORDER BY session_id 

--==============================================================

--Day 8 - Joins continued=======================================

SELECT e.employee_id, concat(e.first_name," ",e.last_name) as full_name, e.salary, e.phone_number, e.department_id, d.department_name, l.street_address, l.city, c.country_name, r.region_id, r.region_name 
FROM employees e
LEFT JOIN departments d ON e.department_id = d.department_id
LEFT JOIN locations l  ON d.location_id = l.location_id
LEFT JOIN countries c ON l.country_id = c.country_id
LEFT JOIN regions r ON c.region_id = r.region_id
WHERE lower(r.region_name) = "europe"
ORDER BY e.salary desc, e.employee_id

SELECT e.employee_id, d.department_name, j.job_id, j.job_title, j.min_salary
FROM employees e
INNER JOIN departments d ON e.department_id = d.department_id
INNER JOIN job_history jh ON e.employee_id = jh.employee_id
INNER JOIN jobs j ON jh.job_id = j.job_id
WHERE j.job_title like "%sales%" and j.min_salary>=6000
ORDER BY e.employee_id, j.min_salary asc

SELECT ce.employee_id 
FROM employees ce
LEFT JOIN employees mel ON ce.manager_id = mel.employee_id
WHERE ce.manager_id is not null and ce.salary < 15000 and mel.employee_id is NULL
ORDER BY ce.employee_id

SELECT d.department_name, COUNT(e.employee_id) as No_of_Employees, sum(e.salary) as Total_Salary 
FROM departments d
LEFT JOIN employees e using(department_id)
WHERE d.department_name is not NULL
GROUP BY d.department_name
ORDER BY d.department_name

SELECT concat(e.first_name," ", e.last_name) as full_name 
FROM employees e
LEFT JOIN (SELECT department_id, SUM(salary) AS department_salary FROM employees GROUP BY department_id) d ON e.department_id = d.department_id
WHERE e.salary > 0.4 * d.department_salary OR d.department_salary IS NULL 
ORDER BY full_name

SELECT MIN(round(sqrt(pow((p1.x - p2.x),2) + pow((p1.y - p2.y),2)),2)) as shortest
FROM points p1 , points p2
WHERE NOT (p1.x = p2.x and p1.y = p2.y)

SELECT e1.symbol as metal, e2.symbol as nonmetal 
FROM elements e1, elements e2
WHERE e1.type = "Metal" and e2.type ="Nonmetal"
ORDER BY metal, nonmetal

SELECT DISTINCT l1.account_id 
FROM loginfo l1
JOIN loginfo l2 ON l1.account_id = l2.account_id and l1.ip_address <> l2.ip_address
WHERE l1.login between l2.login and l2.logout
ORDER BY l1.account_id

SELECT name 
FROM salesperson
WHERE sales_id NOT IN 
(SELECT s1.sales_id 
FROM salesperson s1 
JOIN orders o ON s1.sales_id = o.sales_id 
JOIN company c ON o.com_id = c.com_id 
WHERE c.name = 'RED')
ORDER BY name

SELECT e.employee_id, e.first_name, e.last_name
FROM employees e 
LEFT JOIN employees m ON (e.manager_id = m.employee_id and e.manager_id is not null)
WHERE e.hire_date < m.hire_date
ORDER BY e.employee_id

SELECT c.customer_id, c.customer_name
FROM orders as o
JOIN customers as c using(customer_id)
GROUP BY c.customer_id, c.customer_name 
HAVING sum(o.product_name="Bread") > 0 AND sum(o.product_name="Milk") > 0 AND sum(o.product_name="Eggs") = 0
ORDER BY customer_name
--==============================================================

--Day 9 - SQL : Case study1=====================================
--Approach 1:
SELECT film_id, rental_rate, av.avg_rental
FROM film, (SELECT avg(rental_rate) as avg_rental from film) as av
Having (rental_rate >= av.avg_rental)
ORDER BY film_id

--Approach 2:
select 
    f.film_id, 
    f.rental_rate, 
    tbl.avg_rental 
from (
    select 
        avg(rental_rate) as avg_rental
    from film) tbl
join film f 
on tbl.avg_rental <= f.rental_rate
order by film_id;
--==============================================================

--Day 10 - Window Functions=====================================
SELECT h.team_name AS home_team, a.team_name AS away_team
CROSS JOIN teams AS a
WHERE h.team_name != a.team_name
ORDER BY home_team, away_team

--==============================================================
--Approach 1:
select followee follower, 
count(follower) as num
from follow
where followee in (select follower from follow)
and followee in (select followee from follow)
group by followee
order by follower;

--Approach 2:
SELECT f1.follower, count(distinct f2.follower) as num
FROM follow f1
INNER JOIN follow f2 ON f1.follower = f2.followee
GROUP BY f1.follower
ORDER BY f1.follower
--==============================================================

SELECT first_col, second_col
FROM (SELECT first_col, ROW_NUMBER() OVER(ORDER BY first_col ASC) AS r FROM data ) a 
JOIN (SELECT second_col, ROW_NUMBER() OVER(ORDER BY second_col DESC) AS r FROM data ) b 
ON a.r = b.r

SELECT 
tb.employee_id, tb.first_name, tb.job_id 
FROM (SELECT *,dense_rank() over(partition by job_id ORDER BY salary desc) as rk FROM employees) as tb
WHERE tb.rk = 5
ORDER BY tb.employee_id

SELECT score, dense_rank() over (ORDER BY score DESC) as "rank" FROM scores

SELECT 
concat(first_name," ",last_name) as full_name,  
department_id, salary,
row_number() over(partition by department_id order by salary DESC) as emp_row_no, 
rank() over(partition by department_id order by salary DESC) as emp_rank, 
dense_rank() over(partition by department_id order by salary DESC)as emp_dense_rank
FROM employees
ORDER BY department_id, salary DESC

SELECT 
company_id, employee_id, employee_name, 
round(
    case 
        WHEN max(salary) over(partition by company_id) < 1000 THEN salary 
        WHEN max(salary) over(partition by company_id) BETWEEN 1000 AND 10000 THEN salary - (salary * 0.24)
        ELSE salary - (salary * 0.49)
    end, 0) salary
FROM salaries
ORDER BY company_id, employee_id

--==============================================================
--Approach 1:
SELECT 
employee_id, first_name, last_name, department_id, salary 
FROM (
    SELECT * , avg(salary) over(partition by department_id) as average_salary
    FROM employees
    ) as tb
WHERE salary < average_salary
ORDER BY employee_id

--Approach 2:
SELECT e.employee_id, e.first_name,e.last_name, e.department_id, e.salary
FROM employees e
JOIN (
 SELECT department_id, AVG(salary) AS avg_salary
 FROM employees
 GROUP BY department_id
) AS dept_avg 
ON e.department_id = dept_avg.department_id
WHERE e.salary < dept_avg.avg_salary
ORDER BY employee_id;

--Approach 3:
SELECT employee_id, first_name, last_name, department_id, salary
FROM employees e1
WHERE salary < (SELECT AVG(salary) 
FROM employees e2 
WHERE e2.department_id = e1.department_id)
ORDER BY employee_id;
--==============================================================

SELECT cu.name as customer_name,cu.customer_id,od.order_id,od.order_date 
FROM 
(
    SELECT *,rank() over(partition by customer_id order by order_date DESC) as recent FROM orders
) as od
LEFT JOIN customers as cu using (customer_id)
WHERE od.recent <= 3
ORDER BY customer_name,cu.customer_id,od.order_date DESC

SELECT *, dense_rank() over(order by salary) as team_id
FROM employees
WHERE salary NOT IN (SELECT salary FROM employees GROUP BY salary HAVING COUNT(*) = 1)
ORDER BY team_id, employee_id
--==============================================================

--Day 11 - Window Functions Continued===========================
SELECT account_id, day,
SUM(CASE WHEN type = 'Deposit' THEN amount ELSE -amount END) OVER(partition by account_id order by day asc) AS balance
FROM transactions
ORDER BY account_id, day

SELECT player_id,event_date, sum(games_played) over (partition by player_id order by event_date) as games_played_so_far
FROM activity
ORDER BY player_id,games_played_so_far

SELECT employee_id,salary,
lead(salary,1) over(order by salary) as next_higher_salary,
(lead(salary,1) over(order by salary) -salary) as salary_difference
FROM employees 
ORDER BY salary,salary_difference

SELECT productLine, productName, MSRP, 
NTH_VALUE(productName, 3) OVER (PARTITION BY productLine ORDER BY MSRP DESC) AS thirdMostExpensive_Product,
NTH_VALUE(productName, 5) OVER (PARTITION BY productLine ORDER BY MSRP DESC) AS fifthMostExpensive_Product
FROM products
ORDER BY productLine, MSRP DESC

SELECT DISTINCT e.first_name, 
FIRST_VALUE(jh.start_date) OVER(PARTITION BY jh.employee_id ORDER BY jh.start_date) AS 'first_day_job'
FROM job_history jh JOIN employees e ON jh.employee_id = e.employee_id
ORDER BY e.first_name

SELECT t.order_id FROM 
    (SELECT order_id, max(avg(quantity)) over() as max_avg_qty, max(quantity) as max_qty
    FROM ordersdetails
    GROUP BY order_id) as t
WHERE t.max_qty > t.max_avg_qty
ORDER BY t.order_id

SELECT c.customerName, 
SUM(p.amount) AS total_amount,
NTILE(4) OVER (ORDER BY SUM(p.amount) DESC) AS sales_quartile
FROM customers c JOIN payments p ON c.customerNumber = p.customerNumber
GROUP BY c.customerName
ORDER BY total_amount DESC, sales_quartile

SELECT DISTINCT e.first_name, 
MAX(jh.start_date) OVER(PARTITION BY jh.employee_id) AS 'recent_job'
FROM job_history jh JOIN employees e ON jh.employee_id = e.employee_id
ORDER BY e.first_name

select distinct first_name, first_value(start_date)
over(partition by jhist.employee_id
order by start_date desc
RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as 'recent_job'
from job_history jhist
join employees emp 
on jhist.employee_id=emp.employee_id
order by first_name;

SELECT first_name, 
MAX(start_date) AS "recent_job" 
FROM employees 
JOIN job_history 
USING (employee_id) 
GROUP BY employee_id,first_name 
ORDER BY first_name;

SELECT id, visit_date, people
FROM (
    SELECT id, visit_date, people, 
    lead(people) over (order by id asc) as next1, 
    lead(people,2) over (order by id asc) as next2, 
    lag(people) over (order by id asc) as prev1, 
    lag(people,2) over (order by id asc) as prev2 
    from mall
) as mall_p
WHERE (people >= 100 and next1 >= 100 and next2 >= 100) or 
(people >= 100 and prev1 >= 100 and prev2 >= 100) or 
(people >= 100 and prev1 >= 100 and next1 >= 100)
ORDER BY visit_date

SELECT DISTINCT first_name, last_name,
 first_value(max_salary) OVER (PARTITION BY jh.employee_id ORDER BY start_date) AS 'first_job_sal'
FROM job_history jh 
JOIN employees e ON jh.employee_id = e.employee_id 
JOIN jobs j ON jh.job_id = j.job_id
ORDER BY first_name

SELECT 
o.customerNumber, o.orderNumber, od.productCode, 
(od.quantityOrdered * od.priceEach)as sales, 
NTH_VALUE(od.quantityOrdered * od.priceEach, 3)
 OVER (
    PARTITION BY o.customerNumber 
    ORDER BY (od.quantityOrdered * od.priceEach) DESC 
    RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
) AS thirdHighestSales
FROM orders o
JOIN orderdetails od ON o.orderNumber = od.orderNumber
ORDER BY customerNumber, sales DESC
--==============================================================

--Day 13 - SQL : Case study2====================================
SELECT employee_id, department_id, hire_date, salary,
FIRST_VALUE(salary) over (partition by department_id order by hire_date) as first_dep_salary,
FIRST_VALUE(salary) over (partition by department_id order by hire_date DESC) as latest_dep_salary,
last_value(salary) over(partition by department_id order by hire_date rows between unbounded preceding and unbounded following) as last_dep_salary
FROM employees
ORDER BY department_id
--==============================================================

--Day 17 - Date & Time Functions================================
SELECT employee_id, 
   first_name, 
   last_name, 
   round(datediff("2022-06-08",hire_date)/365,2) as Total_years
FROM employees
WHERE datediff("2022-06-08",hire_date)/365 >=28
ORDER BY employee_id

SELECT 
    employee_id, first_name, last_name, salary, hire_date,
    extract(day from hire_date) as Day, 
    extract(month from hire_date) as Month, 
    extract(year from hire_date) as Year
FROM employees
WHERE extract(month from hire_date) = 1 and 
    extract(year from hire_date) =2000 and
    salary>5000
ORDER BY employee_id

SELECT 
    e.employee_id, e.first_name, e.last_name, e.salary, d.department_name, e.hire_date, l.city
FROM employees e
LEFT JOIN departments d on e.department_id = d.department_id
LEFT JOIN locations l on d.location_id = l.location_id
WHERE hire_date between "1998-01-01" and DATE_ADD("1998-01-01",interval 90 day)
ORDER BY employee_id

SELECT 
    lower(trim(product_name)) as product_name,
    DATE_FORMAT(sale_date, "%Y-%m") as sale_date,
    count(*) as total
FROM sales
GROUP BY LOWER(TRIM(product_name)), DATE_FORMAT(sale_date, "%Y-%m")
ORDER BY product_name, sale_date

SELECT 
    user_id,
    sum(number_of_comments) as comments_count
FROM fb_comments
WHERE created_at between DATE_SUB("2020-02-10",interval 30 day) and "2020-02-10"
GROUP BY user_id
ORDER BY user_id

SELECT DISTINCT emp.employee_id, emp.first_name, emp.last_name, emp.salary, 
    department_name, DATEDIFF('2022-06-08', emp.hire_date)/365 as 'Experience'
FROM employees emp 
JOIN employees mng ON emp.employee_id = mng.manager_id 
JOIN departments d ON d.department_id = emp.department_id
WHERE DATEDIFF('2022-06-08', emp.hire_date)/365 > 25
ORDER BY emp.employee_id

SELECT 
    employee_id, first_name, last_name, salary, hire_date, department_id
FROM (
    SELECT employee_id, first_name, last_name, salary, hire_date, department_id, 
    DENSE_RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) AS 'salary_rank' 
    FROM employees 
    WHERE hire_date BETWEEN DATE_SUB('1998-01-01', INTERVAL 6 MONTH) AND '1998-01-01'
) t
WHERE salary_rank = 1
ORDER BY department_id, employee_id

SELECT 
    extract(year from hire_date) as 'Year',
    count(employee_id) as Employees_count
FROM employees
GROUP BY extract(year from hire_date)
ORDER BY Employees_count DESC , Year

SELECT e.employee_id
FROM employees e
LEFT JOIN logs l ON e.employee_id = l.employee_id
GROUP BY e.employee_id, e.needed_hours
HAVING (SUM(CEIL(IFNULL(TIMESTAMPDIFF(SECOND, l.in_time, l.out_time),0) / 60)) / 60) < e.needed_hours
ORDER BY e.employee_id
--==============================================================

--Day 18 - Advanced Constructs - CTEs & Views===============================================

WITH CTE1 as(
   SELECT employee_id, first_name, last_name, salary, 
   salary + salary *(IFNULL(commission_pct,0)) as 'Net_Salary' FROM employees
),
CTE2 as(
   SELECT * FROM CTE1 WHERE Net_Salary>15000
)
SELECT * FROM CTE2 ORDER BY employee_id

With t as
(select employee_id,first_name, last_name, salary, 
salary+(salary * ifnull(commission_pct,0)) 'Net_Salary' 
from employees)
select * from t 
where Net_Salary > 15000
order by employee_id;

WITH CTE AS (
SELECT employee_id, experience, 
SUM(salary) OVER(PARTITION BY experience ORDER BY salary ASC) AS RN
FROM candidates)
SELECT employee_id
FROM CTE
WHERE experience = 'Senior' AND RN < 70000
UNION
SELECT employee_id
FROM CTE
WHERE experience = 'Junior' AND RN < 
(SELECT 70000 - IFNULL(MAX(RN),0) FROM CTE WHERE experience = 'Senior' AND RN < 70000)
ORDER BY employee_id;

WITH CTE1 as(
    SELECT e.employee_id as employee_id FROM employees e
    LEFT JOIN salaries s
    on s.employee_id = e.employee_id
    WHERE ISNULL(s.salary)
    UNION
    SELECT s.employee_id as employee_id FROM employees e
    RIGHT JOIN salaries s
    on s.employee_id = e.employee_id
    WHERE ISNULL(e.name)
)
SELECT employee_id FROM CTE1 
ORDER BY employee_id

SELECT e.employee_id
FROM employees e
LEFT JOIN salaries s 
ON e.employee_id = s.employee_id
WHERE s.salary IS NULL
UNION
SELECT s.employee_id
FROM salaries s 
LEFT JOIN employees e 
ON e.employee_id = s.employee_id
WHERE e.name IS NULL
ORDER BY employee_id;

WITH CTE1 as
(   
    SELECT product_id, 'store1' as store, store1 as price FROM Products WHERE store1 is not null 
    UNION 
    SELECT product_id, 'store2' as store, store2 as price FROM Products WHERE store2 is not null 
    UNION 
    SELECT product_id, 'store3' as store, store3 as price FROM Products WHERE store3 is not null
)
SELECT * FROM CTE1 ORDER BY product_id, store

WITH CTE AS (SELECT user_id, spend_date, 
    SUM(CASE platform WHEN 'mobile' THEN amount ELSE 0 END) as ma,
    SUM(CASE platform WHEN 'desktop' THEN amount ELSE 0 END) as da
FROM Spending
GROUP BY user_id, spend_date)
SELECT spend_date, 'desktop' as platform, 
    SUM(CASE WHEN da > 0 AND ma = 0 THEN da ELSE 0 END) total_amount, 
    SUM(CASE WHEN da > 0 AND ma = 0 THEN 1 ELSE 0 END) total_users
FROM CTE
GROUP BY spend_date
UNION ALL
SELECT spend_date, 'mobile' as platform, 
     SUM(CASE WHEN ma > 0 AND da = 0 THEN ma ELSE 0 END) total_amount, 
     SUM(CASE WHEN ma > 0 AND da = 0 THEN 1 ELSE 0 END) total_users
FROM CTE
GROUP BY spend_date
UNION ALL
SELECT spend_date, 'both' as platform, 
    SUM(CASE WHEN da > 0 AND ma > 0 THEN ma + da ELSE 0 END) total_amount, 
    SUM(CASE WHEN da > 0 AND ma > 0 THEN 1 ELSE 0 END) total_users
FROM CTE
GROUP BY spend_date 
order by spend_date, platform

--==============================================================
create or replace VIEW Manager_details as(
    SELECT distinct e.employee_id, 
        concat(e.first_name," ", e.last_name) as Manager, 
        e.salary, 
        e.phone_number,
        e.department_id,
        d.department_name, 
        l.street_address,
        l.city, 
        c.country_name,
        DENSE_RANK() over(order by e.salary desc) as salary_rank
    FROM employees e 
    LEFT JOIN employees mg 
    on e.employee_id = mg.manager_id
    LEFT JOIN departments d 
    on e.department_id = d.department_id
    LEFT JOIN locations l 
    on d.location_id = l.location_id
    LEFT JOIN countries c
    on l.country_id = c.country_id
);
SELECT employee_id, 
        Manager, 
        salary, 
        department_name, 
        city, 
        country_name
FROM Manager_details 
WHERE salary_rank <= 5
ORDER BY salary DESC, Manager;
--==============================================================

SELECT d.department_name, 
    AVG(e.salary) AS avg_salary, 
    NTILE(10) OVER (ORDER BY AVG(e.salary) DESC) AS salary_decile
FROM employees e JOIN departments d ON e.department_id = d.department_id
GROUP BY d.department_name
ORDER BY avg_salary DESC, salary_decile ASC

--==============================================================
with order_counts as(
select o.customer_id, p.product_name, p.product_id,
count(o.order_id) as order_count
from orders o
join products p 
on p.product_id = o.product_id
group by o.customer_id, p.product_name, p.product_id)

select oc.customer_id, oc.product_id, oc.product_name
from order_counts oc
where
oc.order_count = 
(select max(order_count) from order_counts x 
where x.customer_id = oc.customer_id) 
order by oc.customer_id, oc.product_id;
--==============================================================

--==============================================================
create or replace VIEW emp_view as(
    SELECT distinct e.employee_id, 
        e.first_name,
        e.last_name, 
        e.salary, 
        e.department_id,
        d.department_name, 
        d.location_id,
        l.street_address,
        l.city
    FROM employees e 
    LEFT JOIN departments d 
    on e.department_id = d.department_id
    LEFT JOIN locations l 
    on d.location_id = l.location_id
);
SELECT employee_id, first_name, last_name, 
    salary, department_id, department_name, 
    location_id, street_address, city
FROM emp_view
WHERE city in ("Seattle", "Southlake")
ORDER BY employee_id
--==============================================================
--==============================================================

--Day 19 - SQL : Case study3====================================
with cte1 as (
   SELECT s.user_id,
      c.time_stamp as first_time,
      lead(c.time_stamp) over(partition by s.user_id order by c.time_stamp) as second_time
   FROM signups s
   left join confirmations c
   on s.user_id = c.user_id
)
select distinct user_id from cte1
WHERE datediff(second_time,first_time)<=1
ORDER BY user_id

SELECT DISTINCT user_id
FROM
(
SELECT C.user_id, 
TIMESTAMPDIFF(SECOND, LAG(C.time_stamp) OVER(PARTITION BY user_id ORDER BY time_stamp ASC),
C.time_stamp) AS difference
FROM confirmations AS C
) AS A
WHERE A.difference <= 86400
ORDER BY user_id;
--==============================================================

--Day 20 - SQL Query Optimization===============================
SELECT DATE_FORMAT(day,"%W, %M %e, %Y") as day FROM days
ORDER BY day

SELECT d.dept_name, ifnull(count(s.student_id),0) as student_number FROM department d
LEFT JOIN student s using (dept_id)
GROUP BY d.dept_name
ORDER BY student_number desc,d.dept_name

SELECT 
    e.employee_id, 
    concat(e.first_name," ",e.last_name) as full_name,
    j.job_title
FROM employees e
JOIN job_history jh ON e.employee_id = jh.employee_id
JOIN jobs j ON j.job_id = jh.job_id
WHERE DATEDIFF(jh.end_date, jh.start_date) <365
ORDER BY e.employee_id,j.job_title

SELECT user_id, MAX(time_stamp) as last_stamp FROM logins
WHERE EXTRACT(YEAR FROM time_stamp) = 2020
GROUP BY user_id
ORDER BY user_id

SELECT orderNumber, orderDate,DATE_ADD(orderDate, INTERVAL 30 DAY) as order_date_plus_30_days  
FROM orders
ORDER BY orderNumber

SELECT 
    employee_id, 
    first_name, 
    department_id, 
    job_id, 
    salary, 
    ntile(4) OVER(ORDER BY salary) AS 'Quartile'
FROM employees
ORDER BY Quartile, salary, employee_id

SELECT 
    user_id AS buyer_id, 
    join_date, 
    COUNT(o.order_id) AS orders_in_2019
FROM users u
LEFT JOIN orders o ON u.user_id = o.buyer_id AND EXTRACT(YEAR from order_date)= 2019
GROUP BY user_id, join_date
ORDER BY user_id

SELECT 
    mindate AS login_date,
    count(user_id) AS user_count
FROM (
    SELECT user_id, MIN(activity_date) AS mindate
    FROM traffic
    WHERE activity = 'login'
    GROUP BY user_id 
) t
WHERE datediff('2019-06-30', mindate) <= 90
GROUP BY mindate
ORDER BY login_date
--==============================================================

--Day 22 - SQL: Miscellaneous Topics============================
--==============================================================
SELECT distinct d.department_name 
FROM employees e
JOIN departments d ON e.department_id = d.department_id
WHERE salary > ANY(SELECT man.salary FROM employees man WHERE e.manager_id = man.employee_id)
ORDER BY d.department_name

SELECT d.department_name
FROM departments d
WHERE EXISTS (
  SELECT 1
  FROM employees e
  WHERE e.department_id = d.department_id
    AND e.salary > (
      SELECT m.salary
      FROM employees m
      WHERE m.employee_id = e.manager_id))
ORDER BY d.department_name;
--==============================================================

SELECT d.department_id ,d.department_name
FROM departments d
WHERE EXISTS (
  SELECT 1
  FROM employees e
  WHERE e.department_id = d.department_id
    AND e.salary > (
      SELECT avg(salary)
      FROM employees
      WHERE department_id = e.department_id)
    AND e.salary < (
      SELECT MAX(salary)
      FROM employees
      WHERE department_id = e.department_id
    ))
ORDER BY d.department_id;

SELECT orderNumber, productCode 
FROM orderdetails
WHERE quantityOrdered < ALL(
    SELECT distinct quantityOrdered FROM orderdetails WHERE orderlineNumber =4
)
ORDER BY orderNumber

SELECT p.productCode
FROM products p
WHERE p.buyPrice < ANY (
    SELECT p2.buyPrice
    FROM products p2
    WHERE p2.productLine = p.productLine
    AND p2.productCode != p.productCode)
ORDER BY productCode;

SELECT d.department_name, GROUP_CONCAT(e.last_name SEPARATOR ', ') AS last_names
FROM departments d
INNER JOIN employees e ON d.department_id = e.department_id
GROUP BY d.department_id
HAVING COUNT(*) > 1;

--==============================================================
Approach 1:

SELECT P.post_id,
  CASE WHEN COUNT(DISTINCT K.topic_id) = 0
    THEN  'Ambiguous!'
    ELSE GROUP_CONCAT(DISTINCT K.topic_id ORDER BY K.topic_id  SEPARATOR',')
    END AS topic
FROM
  Posts P LEFT JOIN Keywords K
ON
  LOWER(P.content) LIKE CONCAT('% ', LOWER(K.word), ' %') OR
  LOWER(P.content) LIKE CONCAT('% ', LOWER(K.word)) OR
  LOWER(P.content) LIKE CONCAT(LOWER(K.word), ' %')
GROUP BY P.post_id
ORDER BY P.post_id;

Approach 2:

SELECT
P.post_id,
IFNULL(GROUP_CONCAT(DISTINCT K.topic_id ORDER BY K.topic_id  SEPARATOR','), 'Ambiguous!') AS topic
FROM posts AS P
LEFT JOIN keywords AS K
ON CONCAT(' ', LOWER(P.content),' ') LIKE CONCAT('% ', LOWER(K.word), ' %')
GROUP BY P.post_id
ORDER BY P.post_id;
--==============================================================

SELECT 
    e.employee_id,
    e.employee_name,
    e.employee_salary as current_salary,
    (e.employee_salary + COALESCE(b.incentive_amount, b.special_bonus, e.employee_salary * 0.08)) AS updated_salary_with_incentive_amount
FROM 
    employee e
LEFT JOIN 
    bonus b ON e.employee_id = b.employee_id
ORDER BY 
    e.employee_id;

SELECT employee_id, date, COUNT(*) AS occurrences
FROM employee_attendance
GROUP BY employee_id,employee_name, date
HAVING COUNT(*) > 1
ORDER BY employee_id,date;

SELECT m.employee_id, m.first_name, m.last_name, m.salary AS manager_salary
FROM employees m
WHERE EXISTS (
    SELECT 1
    FROM employees e
    WHERE e.manager_id = m.employee_id
    AND e.salary > m.salary / 2)
ORDER BY m.employee_id;

SELECT productCode
FROM products p
WHERE productLine = 'Ships'
AND buyPrice > ALL (
  SELECT buyPrice
  FROM products
  WHERE productLine = 'Ships'
  AND productCode <> p.productCode)
ORDER BY productCode;

select
concat(group_concat(term order by power desc separator ''), '=0') as equation
from (
select CONCAT(
           CASE
               WHEN factor > 0 THEN CONCAT('+', factor)
               ELSE factor
           END,
           CASE
               WHEN power = 1 THEN 'X'
               WHEN power = 0 THEN ''
               ELSE CONCAT('X^', power)
           END)term, power from terms)t;

SELECT productName, productCode
FROM products
WHERE productCode = ANY
(SELECT DISTINCT productCode
FROM orderdetails
WHERE quantityOrdered > 60)
ORDER BY productCode;
--==============================================================

--Day 24 - SQL : Case study4====================================

SELECT i.Store_ID, 
   SUM(i.Stock_ON_Hand) as total_stock, 
   ROUND(SUM(i.Stock_ON_Hand * p.Product_Price),2) as inventory_cost 
FROM inventory i
LEFT JOIN products p
ON i.Product_ID = p.Product_ID
GROUP BY Store_ID
ORDER BY inventory_cost DESC
--==============================================================
