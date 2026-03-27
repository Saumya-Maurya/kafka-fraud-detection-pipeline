"""
Fraud Pipeline — Analysis Notebook
====================================
Run this after the pipeline has been running for ~5 minutes to see
real results from PostgreSQL.

Convert to Jupyter: jupytext --to notebook notebooks/fraud_analysis.py
"""

# %% [markdown]
# ## Fraud Detection Pipeline — Results Analysis
# Connect to PostgreSQL and explore fraud patterns detected by the pipeline.

# %%
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine, text

# Connect to local PostgreSQL
engine = create_engine("postgresql://fraud_user:fraud_pass@localhost:5432/fraud_db")

# %% [markdown]
# ### 1. Overall Pipeline Stats

# %%
with engine.connect() as conn:
    summary = pd.read_sql(text("""
        SELECT
            COUNT(*)                                              AS total_transactions,
            SUM(CASE WHEN final_is_fraud THEN 1 ELSE 0 END)      AS fraud_count,
            ROUND(100.0 * AVG(CASE WHEN final_is_fraud THEN 1.0 ELSE 0.0 END), 3)
                                                                  AS fraud_rate_pct,
            ROUND(SUM(amount), 2)                                 AS total_volume_usd,
            ROUND(SUM(CASE WHEN final_is_fraud THEN amount ELSE 0 END), 2)
                                                                  AS fraud_volume_usd,
            ROUND(AVG(EXTRACT(EPOCH FROM (NOW() - event_time))), 1)
                                                                  AS avg_age_seconds
        FROM transactions
    """), conn)

print(summary.T.to_string())

# %% [markdown]
# ### 2. Fraud Rate Over Time

# %%
with engine.connect() as conn:
    hourly = pd.read_sql(text("""
        SELECT * FROM fraud_metrics ORDER BY hour
    """), conn)

fig, axes = plt.subplots(1, 2, figsize=(14, 4))

axes[0].plot(hourly["hour"], hourly["fraud_rate_pct"], color="#E24B4A", linewidth=2)
axes[0].set_title("Fraud Rate % — Hourly")
axes[0].set_xlabel("Hour")
axes[0].set_ylabel("Fraud Rate (%)")
axes[0].tick_params(axis="x", rotation=45)

axes[1].bar(hourly["hour"], hourly["total_transactions"], color="#378ADD", alpha=0.7, label="Total")
axes[1].bar(hourly["hour"], hourly["fraud_count"], color="#E24B4A", label="Fraud")
axes[1].set_title("Transaction Volume vs Fraud — Hourly")
axes[1].legend()
axes[1].tick_params(axis="x", rotation=45)

plt.tight_layout()
plt.savefig("docs/fraud_rate_over_time.png", dpi=150, bbox_inches="tight")
plt.show()

# %% [markdown]
# ### 3. Rule Performance

# %%
with engine.connect() as conn:
    rules = pd.read_sql(text("SELECT * FROM rule_performance"), conn)

print(rules.to_string(index=False))

fig, ax = plt.subplots(figsize=(8, 4))
bars = ax.barh(rules["final_rule"], rules["alert_count"], color="#378ADD")
ax.bar_label(bars)
ax.set_title("Fraud Alerts by Rule")
ax.set_xlabel("Alert Count")
plt.tight_layout()
plt.savefig("docs/rule_performance.png", dpi=150, bbox_inches="tight")
plt.show()

# %% [markdown]
# ### 4. Amount Distribution — Fraud vs Legitimate

# %%
with engine.connect() as conn:
    amounts = pd.read_sql(text("""
        SELECT amount, final_is_fraud
        FROM transactions
        WHERE amount < 2000
        LIMIT 5000
    """), conn)

fig, ax = plt.subplots(figsize=(10, 4))
for label, group in amounts.groupby("final_is_fraud"):
    ax.hist(
        group["amount"],
        bins=60,
        alpha=0.6,
        label="Fraud" if label else "Legitimate",
        color="#E24B4A" if label else "#378ADD",
    )
ax.set_title("Transaction Amount Distribution")
ax.set_xlabel("Amount (USD)")
ax.set_ylabel("Count")
ax.legend()
plt.tight_layout()
plt.savefig("docs/amount_distribution.png", dpi=150, bbox_inches="tight")
plt.show()

# %% [markdown]
# ### 5. Top High-Risk Users

# %%
with engine.connect() as conn:
    top_users = pd.read_sql(text("""
        SELECT
            user_id,
            COUNT(*)    AS total_txn,
            SUM(CASE WHEN final_is_fraud THEN 1 ELSE 0 END) AS fraud_txn,
            ROUND(100.0 * AVG(CASE WHEN final_is_fraud THEN 1.0 ELSE 0.0 END), 1)
                        AS fraud_rate_pct,
            ROUND(SUM(CASE WHEN final_is_fraud THEN amount ELSE 0 END), 2)
                        AS fraud_amount_usd
        FROM transactions
        GROUP BY user_id
        HAVING SUM(CASE WHEN final_is_fraud THEN 1 ELSE 0 END) > 0
        ORDER BY fraud_txn DESC
        LIMIT 10
    """), conn)

print(top_users.to_string(index=False))
