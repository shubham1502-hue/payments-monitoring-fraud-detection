from __future__ import annotations

from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATASETS_DIR = ROOT / "datasets"
RAW_TRANSACTIONS = DATASETS_DIR / "payments_transactions.csv"
SAMPLE_TRANSACTIONS = ROOT / "sample_data" / "payments_transactions_sample.csv"


def load_transactions() -> pd.DataFrame:
    path = RAW_TRANSACTIONS if RAW_TRANSACTIONS.exists() else SAMPLE_TRANSACTIONS
    df = pd.read_csv(path)
    df["transaction_time"] = pd.to_datetime(df["transaction_time"])
    return df


def export(df: pd.DataFrame, filename: str) -> None:
    DATASETS_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(DATASETS_DIR / filename, index=False)


def build_reports(df: pd.DataFrame) -> None:
    df = df.copy()
    df["day"] = df["transaction_time"].dt.date
    df["is_success"] = df["status"].eq("Success")
    df["is_failed"] = df["status"].eq("Failed")

    daily = (
        df.groupby("day")
        .agg(
            total_transactions=("transaction_id", "count"),
            successful_transactions=("is_success", "sum"),
            failed_transactions=("is_failed", "sum"),
        )
        .reset_index()
    )
    export(daily, "daily_transactions.csv")

    mean_volume = daily["total_transactions"].mean()
    std_volume = daily["total_transactions"].std()
    daily["volume_anomaly"] = daily["total_transactions"] > mean_volume + (2 * std_volume)
    export(daily, "transaction_volume_anomalies.csv")

    amount_threshold = df["amount"].mean() + (3 * df["amount"].std())
    payment_amount_anomalies = df[df["amount"] > amount_threshold][
        ["transaction_id", "customer_id", "payment_type", "amount", "country", "transaction_time"]
    ]
    export(payment_amount_anomalies, "payment_amount_anomalies.csv")

    failure = (
        df.groupby("day")
        .agg(failed=("is_failed", "sum"), total=("transaction_id", "count"))
        .reset_index()
    )
    failure["failure_rate"] = failure["failed"] / failure["total"]
    failure_threshold = failure["failure_rate"].mean() + (2 * failure["failure_rate"].std())
    failure["failure_anomaly"] = failure["failure_rate"] > failure_threshold
    export(failure, "failure_rate_anomalies.csv")

    payment_type_distribution = (
        df.groupby("payment_type")
        .size()
        .reset_index(name="transactions")
        .sort_values("transactions", ascending=False)
    )
    export(payment_type_distribution, "payment_type_distribution.csv")

    failure_reasons = (
        df[df["status"].eq("Failed")]
        .groupby("failure_reason")
        .size()
        .reset_index(name="failure_count")
        .sort_values("failure_count", ascending=False)
    )
    export(failure_reasons, "failure_reasons.csv")

    country_distribution = (
        df.groupby("country")
        .size()
        .reset_index(name="transactions")
        .sort_values("transactions", ascending=False)
    )
    export(country_distribution, "transactions_by_country.csv")

    high_value = df[df["amount"] > 10000][
        ["transaction_id", "customer_id", "payment_type", "amount", "country", "transaction_time"]
    ]
    export(high_value, "high_value_transactions.csv")

    ordered = df.sort_values(["customer_id", "transaction_time"]).copy()
    ordered["previous_country"] = ordered.groupby("customer_id")["country"].shift(1)
    ordered["previous_time"] = ordered.groupby("customer_id")["transaction_time"].shift(1)
    ordered["minutes_since_last"] = (
        ordered["transaction_time"] - ordered["previous_time"]
    ).dt.total_seconds() / 60
    geo_velocity = ordered[
        ordered["previous_country"].notna()
        & ordered["country"].ne(ordered["previous_country"])
        & ordered["minutes_since_last"].le(5)
    ][["customer_id", "transaction_time", "country", "previous_country", "minutes_since_last"]]
    export(geo_velocity, "geo_velocity_anomalies.csv")

    risk_frames = []
    high_amount_customers = df[df["amount"] > 20000][["customer_id"]].drop_duplicates()
    high_amount_customers["risk_points"] = 40
    risk_frames.append(high_amount_customers)

    geo_customers = geo_velocity[["customer_id"]].drop_duplicates()
    geo_customers["risk_points"] = 50
    risk_frames.append(geo_customers)

    burst_customers = (
        df.groupby(["customer_id", "day"])
        .size()
        .reset_index(name="transactions")
        .query("transactions >= 20")[["customer_id"]]
        .drop_duplicates()
    )
    burst_customers["risk_points"] = 30
    risk_frames.append(burst_customers)

    risk_scores = (
        pd.concat(risk_frames, ignore_index=True)
        .groupby("customer_id")["risk_points"]
        .sum()
        .reset_index()
        .sort_values("risk_points", ascending=False)
    )
    export(risk_scores, "customer_risk_scores.csv")


if __name__ == "__main__":
    transactions = load_transactions()
    build_reports(transactions)
    print("Reporting datasets and anomaly signals generated in datasets/.")
