import pandas as pd
import mysql.connector

# -----------------------------
# CONNECT TO DATABASE
# -----------------------------

connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="password",
    database="payments_monitoring"
)

# -----------------------------
# DAILY TRANSACTIONS
# -----------------------------

query_daily = """
SELECT
DATE(transaction_time) AS day,
COUNT(*) AS total_transactions,
SUM(CASE WHEN status='Success' THEN 1 ELSE 0 END) AS successful_transactions,
SUM(CASE WHEN status='Failed' THEN 1 ELSE 0 END) AS failed_transactions
FROM payments
GROUP BY DATE(transaction_time)
ORDER BY day
"""

df_daily = pd.read_sql(query_daily, connection)

df_daily.to_csv("daily_transactions.csv", index=False)

# -----------------------------
# TRANSACTION VOLUME ANOMALY
# -----------------------------

mean_volume = df_daily["total_transactions"].mean()
std_volume = df_daily["total_transactions"].std()

threshold_volume = mean_volume + (2 * std_volume)

df_daily["volume_anomaly"] = df_daily["total_transactions"] > threshold_volume

df_daily.to_csv("transaction_volume_anomalies.csv", index=False)

# -----------------------------
# PAYMENT AMOUNT ANOMALY
# -----------------------------

query_payments = """
SELECT
transaction_id,
payment_type,
amount,
country,
transaction_time
FROM payments
"""

df_payments = pd.read_sql(query_payments, connection)

mean_amount = df_payments["amount"].mean()
std_amount = df_payments["amount"].std()

threshold_amount = mean_amount + (3 * std_amount)

df_payments["amount_anomaly"] = df_payments["amount"] > threshold_amount

df_payments[df_payments["amount_anomaly"]].to_csv(
    "payment_amount_anomalies.csv", index=False
)

# -----------------------------
# FAILURE RATE ANOMALY
# -----------------------------

query_failure = """
SELECT
DATE(transaction_time) AS day,
SUM(CASE WHEN status='Failed' THEN 1 ELSE 0 END) AS failed,
COUNT(*) AS total
FROM payments
GROUP BY DATE(transaction_time)
"""

df_failure = pd.read_sql(query_failure, connection)

df_failure["failure_rate"] = df_failure["failed"] / df_failure["total"]

mean_failure = df_failure["failure_rate"].mean()
std_failure = df_failure["failure_rate"].std()

threshold_failure = mean_failure + (2 * std_failure)

df_failure["failure_anomaly"] = df_failure["failure_rate"] > threshold_failure

df_failure.to_csv("failure_rate_anomalies.csv", index=False)

# -----------------------------
# PAYMENT TYPE DISTRIBUTION
# -----------------------------

query_types = """
SELECT
payment_type,
COUNT(*) AS transactions
FROM payments
GROUP BY payment_type
"""

df_types = pd.read_sql(query_types, connection)

df_types.to_csv("payment_type_distribution.csv", index=False)

# -----------------------------
# FAILURE REASONS
# -----------------------------

query_failures = """
SELECT
failure_reason,
COUNT(*) AS failure_count
FROM payments
WHERE status='Failed'
GROUP BY failure_reason
"""

df_failures = pd.read_sql(query_failures, connection)

df_failures.to_csv("failure_reasons.csv", index=False)

# -----------------------------
# COUNTRY DISTRIBUTION
# -----------------------------

query_country = """
SELECT
country,
COUNT(*) AS transactions
FROM payments
GROUP BY country
"""

df_country = pd.read_sql(query_country, connection)

df_country.to_csv("transactions_by_country.csv", index=False)

# -----------------------------
# HIGH VALUE TRANSACTIONS
# -----------------------------

query_high_value = """
SELECT
transaction_id,
payment_type,
amount,
country,
transaction_time
FROM payments
WHERE amount > 10000
"""

df_high = pd.read_sql(query_high_value, connection)

df_high.to_csv("high_value_transactions.csv", index=False)

# -----------------------------
# GEO-VELOCITY ANOMALY
# -----------------------------

query_geo_velocity = """
SELECT *
FROM (
    SELECT
        customer_id,
        transaction_time,
        country,
        LAG(country) OVER (
            PARTITION BY customer_id
            ORDER BY transaction_time
        ) AS previous_country,
        TIMESTAMPDIFF(
            MINUTE,
            LAG(transaction_time) OVER (
                PARTITION BY customer_id
                ORDER BY transaction_time
            ),
            transaction_time
        ) AS minutes_since_last
    FROM payments
) t
WHERE previous_country IS NOT NULL
AND country <> previous_country
AND minutes_since_last <= 5
"""

df_geo_velocity = pd.read_sql(query_geo_velocity, connection)

df_geo_velocity.to_csv("geo_velocity_anomalies.csv", index=False)

# -----------------------------
# CUSTOMER RISK SCORE
# -----------------------------

# High amount anomaly customers (+40 points)
query_high_amount_risk = """
SELECT DISTINCT customer_id, 40 AS risk_points
FROM payments
WHERE amount > 20000
"""
df_risk_high = pd.read_sql(query_high_amount_risk, connection)

# Geo velocity anomaly customers (+50 points)
query_geo_risk = """
SELECT DISTINCT customer_id, 50 AS risk_points
FROM (
    SELECT
        customer_id,
        transaction_time,
        country,
        LAG(country) OVER (
            PARTITION BY customer_id
            ORDER BY transaction_time
        ) AS previous_country,
        TIMESTAMPDIFF(
            MINUTE,
            LAG(transaction_time) OVER (
                PARTITION BY customer_id
                ORDER BY transaction_time
            ),
            transaction_time
        ) AS minutes_since_last
    FROM payments
) t
WHERE previous_country IS NOT NULL
AND country <> previous_country
AND minutes_since_last <= 5
"""
df_risk_geo = pd.read_sql(query_geo_risk, connection)

# Burst transaction anomaly customers (+30 points)
query_burst_risk = """
SELECT customer_id, 30 AS risk_points
FROM payments
GROUP BY customer_id, DATE(transaction_time)
HAVING COUNT(*) >= 20
"""
df_risk_burst = pd.read_sql(query_burst_risk, connection)

# Combine all risk signals
df_risk_all = pd.concat([df_risk_high, df_risk_geo, df_risk_burst])

# Aggregate total risk score per customer
df_risk_scores = (
    df_risk_all.groupby("customer_id")
    .sum()
    .reset_index()
    .sort_values("risk_points", ascending=False)
)

# Export dataset
df_risk_scores.to_csv("customer_risk_scores.csv", index=False)

print("Reporting datasets + anomaly detection generated successfully.")