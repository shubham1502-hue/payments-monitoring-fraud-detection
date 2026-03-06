import pandas as pd
import numpy as np
from faker import Faker
import random

fake = Faker()

num_transactions = 10000

payment_types = ["Card", "Wire", "ACH"]
countries = ["USA", "UK", "India", "Germany", "Canada", "Singapore"]

failure_reasons = [
    "Insufficient Funds",
    "Network Timeout",
    "Fraud Suspected",
    "Authentication Failed",
    None
]

data = []

for i in range(num_transactions):

    transaction_id = f"TXN{i+1:06d}"
    customer_id = f"C{random.randint(1000,9999)}"

    payment_type = np.random.choice(payment_types, p=[0.65,0.15,0.20])

    amount = round(np.random.exponential(scale=2000),2)

    country = random.choice(countries)

    timestamp = fake.date_time_between(start_date='-30d', end_date='now')

    processing_time = round(np.random.normal(5,2))

    if processing_time < 1:
        processing_time = 1

    status = np.random.choice(["Success","Failed"], p=[0.97,0.03])

    failure_reason = None

    if status == "Failed":
        failure_reason = random.choice(failure_reasons[:-1])

    data.append([
        transaction_id,
        customer_id,
        payment_type,
        amount,
        "USD",
        timestamp,
        processing_time,
        status,
        failure_reason,
        country
    ])

columns = [
    "transaction_id",
    "customer_id",
    "payment_type",
    "amount",
    "currency",
    "transaction_time",
    "processing_time_seconds",
    "status",
    "failure_reason",
    "country"
]

df = pd.DataFrame(data, columns=columns)

# -------------------------------------------------
# Inject Synthetic Anomalies for Monitoring System
# -------------------------------------------------

# 1. Extreme high-value transactions (amount anomalies)
high_value_indices = np.random.choice(df.index, size=10, replace=False)
df.loc[high_value_indices, "amount"] = np.random.uniform(50000, 200000, size=10)

# 2. Failure spike on a specific day (system outage simulation)
df["transaction_time"] = pd.to_datetime(df["transaction_time"])
anomaly_day = df["transaction_time"].dt.date.sample(1).iloc[0]

failure_indices = df[df["transaction_time"].dt.date == anomaly_day].sample(
    frac=0.4, random_state=42
).index

df.loc[failure_indices, "status"] = "Failed"
df.loc[failure_indices, "failure_reason"] = "Network Timeout"

# 3. Transaction volume spike (simulate traffic burst)
volume_spike = df.sample(300, replace=True).copy()

# choose a spike day
spike_day = df["transaction_time"].dt.date.sample(1).iloc[0]

# spread spike transactions across the spike day with random seconds
volume_spike["transaction_time"] = [
    pd.to_datetime(spike_day) + pd.Timedelta(seconds=random.randint(0, 86400))
    for _ in range(len(volume_spike))
]

# assign new IDs so they look like new incoming transactions
volume_spike["transaction_id"] = [
    f"TXN_SPIKE_{i}" for i in range(len(volume_spike))
]

df = pd.concat([df, volume_spike], ignore_index=True)

# 4. Customer behavior anomaly (rapid burst transactions from one customer)
burst_customer = f"C{random.randint(1000,9999)}"

burst_transactions = df.sample(20, replace=True).copy()
burst_time = pd.to_datetime(df["transaction_time"].sample(1).iloc[0])

# force all transactions to belong to the same customer
burst_transactions["customer_id"] = burst_customer

# create rapid transactions within the same minute
burst_transactions["transaction_time"] = [
    burst_time + pd.Timedelta(seconds=i*2) for i in range(len(burst_transactions))
]

# give them unique IDs
burst_transactions["transaction_id"] = [
    f"TXN_BURST_{i}" for i in range(len(burst_transactions))
]

df = pd.concat([df, burst_transactions], ignore_index=True)

# 5. Geo-velocity anomaly (same customer transacting in multiple countries within minutes)
geo_customer = f"C{random.randint(1000,9999)}"

geo_transactions = df.sample(3, replace=True).copy()
geo_time = pd.to_datetime(df["transaction_time"].sample(1).iloc[0])

geo_transactions["customer_id"] = geo_customer

# force different countries rapidly
geo_countries = random.sample(countries, 3)
geo_transactions["country"] = geo_countries

# create transactions a few minutes apart
geo_transactions["transaction_time"] = [
    geo_time,
    geo_time + pd.Timedelta(minutes=2),
    geo_time + pd.Timedelta(minutes=4)
]

geo_transactions["transaction_id"] = [
    f"TXN_GEO_{i}" for i in range(len(geo_transactions))
]

df = pd.concat([df, geo_transactions], ignore_index=True)

df.to_csv("payments_transactions.csv", index=False)

print("Dataset generated successfully.")