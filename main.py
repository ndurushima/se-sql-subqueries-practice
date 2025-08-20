import sqlite3
import pandas as pd
conn = sqlite3.Connection('data.sqlite')


# rewrite using a subqery
"""SELECT
    customerNumber,
    contactLastName,
    contactFirstName
FROM customers
JOIN orders 
    USING(customerNumber)
WHERE orderDate = '2003-01-31'
;"""

q = """
SELECT
    customerNumber,
    contactLastName,
    contactFirstName
FROM customers
WHERE customerNumber IN (
    SELECT customerNumber 
    FROM orders 
    WHERE orderDate = '2003-01-31'
)
;
"""
print(pd.read_sql(q, conn))


# step 2 Select the total number of orders for each product name. Sort the results by the total number of items sold for that product.
q = """
SELECT 
    productName,
    COUNT(orderNumber) AS numberOrders,
    SUM(quantityOrdered) AS totalUnitsSold
FROM products
JOIN orderDetails
    USING(productCode)
GROUP BY productName
ORDER BY totalUnitsSold DESC
;
"""
print(pd.read_sql(q, conn))


# step 3 - Select the product name and the total number of people who have ordered each product. Sort the results in descending order.
q = """
SELECT productName, COUNT(DISTINCT customerNumber) as numPurchasers
FROM products
JOIN orderDetails
    USING(productCode)
JOIN orders
    USING(orderNumber)
GROUP BY productName
ORDER BY numPurchasers DESC
;
"""
print(pd.read_sql(q, conn))


# step 4 - Select the employee number, first name, last name, city (of the office), and office code of the employees who sold products that have been ordered by fewer than 20 people
q = """
SELECT employeeNumber, firstName, lastName, o.city, officeCode FROM employees AS e
JOIN offices AS o
    USING(officeCode)
JOIN customers AS c
    ON e.employeeNumber = c.salesRepEmployeeNumber
JOIN orders 
    USING(customerNumber)
JOIN orderDetails
    USING(orderNumber)
WHERE productCode IN (
    SELECT productCode
    FROM products
    JOIN orderDetails
        USING(productCode)
    JOIN orders
        USING(orderNumber)
    GROUP BY productCode
    HAVING COUNT(DISTINCT customerNumber) < 20
)
;
"""
print(pd.read_sql(q, conn))


# Select the employee number, first name, last name, and number of customers for employees whose customers have an average credit limit over 15K.
q = """
SELECT employeeNumber, firstName, lastName, COUNT(customerNumber) AS numCustomers
FROM employees AS e
JOIN customers AS c
    ON e.employeeNumber = c.salesRepEmployeeNumber
GROUP BY employeeNumber
HAVING AVG(creditLimit) > 15000
;
"""
print(pd.read_sql(q, conn))


conn.close()