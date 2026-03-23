SELECT name FROM sys.tables;

-- Top states by total transaction amount
SELECT state, SUM(transaction_amount) AS total_amount
FROM aggregated_transactions
GROUP BY state
ORDER BY total_amount DESC;


-- Top states by transaction count
SELECT state, SUM(transaction_count) AS total_count
FROM aggregated_transactions
GROUP BY state
ORDER BY total_count DESC;


-- Year-wise total transaction amount
SELECT year, SUM(transaction_amount) AS total_amount
FROM aggregated_transactions
GROUP BY year
ORDER BY year;


-- Year-on-year growth percentage
SELECT 
    year,
    SUM(transaction_amount) AS total_amount,
    LAG(SUM(transaction_amount)) OVER (ORDER BY year) AS prev_year,
    ROUND(
        (SUM(transaction_amount) - LAG(SUM(transaction_amount)) OVER (ORDER BY year)) * 100.0 /
        LAG(SUM(transaction_amount)) OVER (ORDER BY year), 2
    ) AS growth_percentage
FROM aggregated_transactions
GROUP BY year;


-- User growth over years
SELECT year, SUM(registered_users) AS total_users
FROM aggregated_users
GROUP BY year
ORDER BY year;


-- Transaction distribution by type (percentage share)
SELECT 
    transaction_type,
    SUM(transaction_count) AS total_count,
    ROUND(
        SUM(transaction_count) * 100.0 / SUM(SUM(transaction_count)) OVER(), 2
    ) AS percentage
FROM aggregated_transactions
GROUP BY transaction_type
ORDER BY total_count DESC;


-- Average transaction value
SELECT 
    ROUND(SUM(transaction_amount) * 1.0 / SUM(transaction_count), 2) AS avg_value
FROM aggregated_transactions;


-- Top 5 states in latest year (2024)
SELECT TOP 5 state, SUM(transaction_amount) AS total_amount
FROM aggregated_transactions
WHERE year = 2024
GROUP BY state
ORDER BY total_amount DESC;


-- Bottom 5 states by transaction amount
SELECT TOP 5 state, SUM(transaction_amount) AS total_amount
FROM aggregated_transactions
GROUP BY state
ORDER BY total_amount ASC;


-- Contribution percentage by state
SELECT 
    state,
    SUM(transaction_amount) AS total_amount,
    ROUND(
        SUM(transaction_amount) * 100.0 / SUM(SUM(transaction_amount)) OVER(), 2
    ) AS contribution_percentage
FROM aggregated_transactions
GROUP BY state
ORDER BY total_amount DESC;


-- Quarterly transaction trend
SELECT year, quarter, SUM(transaction_amount) AS total_amount
FROM aggregated_transactions
GROUP BY year, quarter
ORDER BY year, quarter;


-- Peak transaction year
SELECT TOP 1 year, SUM(transaction_amount) AS total_amount
FROM aggregated_transactions
GROUP BY year
ORDER BY total_amount DESC;


-- Total app opens per year (engagement)
SELECT 
    year,
    SUM(app_opens) AS total_opens
FROM aggregated_users
GROUP BY year
ORDER BY year;


-- Transactions per user (engagement metric)
SELECT 
    t.year,
    SUM(t.transaction_count) * 1.0 / SUM(u.registered_users) AS txn_per_user
FROM aggregated_transactions t
JOIN aggregated_users u
ON t.state = u.state AND t.year = u.year AND t.quarter = u.quarter
GROUP BY t.year
ORDER BY t.year;


-- States with above average transaction value
SELECT state, SUM(transaction_amount) AS total_amount
FROM aggregated_transactions
GROUP BY state
HAVING SUM(transaction_amount) >
(
    SELECT AVG(total_amt) FROM
    (SELECT SUM(transaction_amount) AS total_amt FROM aggregated_transactions GROUP BY state) t
);


-- Transaction trend by type over years
SELECT year, transaction_type, SUM(transaction_amount) AS total_amount
FROM aggregated_transactions
GROUP BY year, transaction_type
ORDER BY year;


-- Fastest growing states (2024 vs 2023)
SELECT TOP 5 
    state,
    SUM(CASE WHEN year = 2024 THEN transaction_amount ELSE 0 END) -
    SUM(CASE WHEN year = 2023 THEN transaction_amount ELSE 0 END) AS growth
FROM aggregated_transactions
GROUP BY state
ORDER BY growth DESC;


-- User distribution by state
SELECT state, SUM(registered_users) AS total_users
FROM aggregated_users
GROUP BY state
ORDER BY total_users DESC;


-- Transaction value per user (state-wise)
SELECT 
    t.state,
    SUM(t.transaction_amount) * 1.0 / SUM(u.registered_users) AS amount_per_user
FROM aggregated_transactions t
JOIN aggregated_users u
ON t.state = u.state AND t.year = u.year AND t.quarter = u.quarter
GROUP BY t.state
ORDER BY amount_per_user DESC;


-- Top transaction type for each year
SELECT year, transaction_type, total_amount
FROM (
    SELECT 
        year,
        transaction_type,
        SUM(transaction_amount) AS total_amount,
        RANK() OVER (PARTITION BY year ORDER BY SUM(transaction_amount) DESC) AS rnk
    FROM aggregated_transactions
    GROUP BY year, transaction_type
) t
WHERE rnk = 1;