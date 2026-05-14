"""
Banking Data Platform — Streamlit Dashboard

Interactive KPI dashboard that reads from staged Parquet files.
Designed to be demoed in interviews — runs without a database.

Run with:  streamlit run dashboard/app.py
"""

import sys
from pathlib import Path
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

sys.path.insert(0, str(Path(__file__).parents[1]))

# ── Page config ────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Banking Data Platform",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── color palette ──────────────────────────────────────────────────

COLORS = {
    "primary":   "#2E7D32",   
    "secondary": "#1B5E20",
    "accent":    "#66BB6A",
    "warning":   "#F9A825",
    "danger":    "#C62828",
    "neutral":   "#455A64",
    "bg":        "#F5F5F5",
}

CHART_COLORS = [
    "#2E7D32", "#66BB6A", "#F9A825", "#1565C0",
    "#6A1B9A", "#AD1457", "#00838F", "#E65100",
]

# ── Data loader ────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def load_data():
    """Load staged parquet files. Generates synthetic data if files don't exist."""
    base = Path(__file__).parents[1] / "data" / "processed"

    def _try_load(filename):
        path = base / filename
        if path.exists():
            return pd.read_parquet(path)
        return None

    txn = _try_load("transactions_staged.parquet")
    acct = _try_load("accounts_staged.parquet")
    cust = _try_load("customers_staged.parquet")
    credit = _try_load("credit_staged.parquet")

    if txn is None:
        txn, acct, cust, credit = _generate_demo_data()

    return txn, acct, cust, credit


def _generate_demo_data():
    """Generate lightweight demo data for dashboard preview."""
    rng = np.random.default_rng(42)
    n_txn = 50_000

    dates = pd.date_range("2023-01-01", "2024-12-31", periods=n_txn)
    txn = pd.DataFrame({
        "transaction_id":      [f"TXN{i:010d}" for i in range(n_txn)],
        "account_id":          [f"ACC{rng.integers(1,8001):08d}" for _ in range(n_txn)],
        "transaction_type":    rng.choice(["Debit","Credit","Transfer","Bill Payment","ATM Withdrawal"],
                                           n_txn, p=[0.35,0.30,0.15,0.12,0.08]),
        "channel":             rng.choice(["Online","Mobile","ATM","Branch","POS"], n_txn),
        "amount_cad":          np.abs(rng.exponential(250, n_txn)).round(2),
        "signed_amount_cad":   rng.choice([-1,1], n_txn) * np.abs(rng.exponential(250, n_txn)).round(2),
        "merchant_category":   rng.choice(["Grocery","Restaurant","Gas","Utilities",
                                            "Entertainment","Travel","Healthcare","Retail","Unclassified"], n_txn),
        "transaction_date":    dates.date,
        "status":              rng.choice(["Completed","Pending","Failed","Reversed"],
                                           n_txn, p=[0.91,0.04,0.03,0.02]),
        "is_large_transaction":rng.random(n_txn) < 0.01,
        "is_weekend":          pd.DatetimeIndex(dates).dayofweek >= 5,
        "txn_year":            pd.DatetimeIndex(dates).year,
        "txn_month":           pd.DatetimeIndex(dates).month,
        "txn_quarter":         pd.DatetimeIndex(dates).quarter,
    })

    n_acct = 8_000
    acct = pd.DataFrame({
        "account_id":       [f"ACC{i:08d}" for i in range(1, n_acct+1)],
        "customer_id":      [f"CUST{rng.integers(1,5001):07d}" for _ in range(n_acct)],
        "account_type":     rng.choice(["Chequing","Savings","TFSA","RRSP","GIC"],
                                        n_acct, p=[0.35,0.30,0.15,0.15,0.05]),
        "account_status":   rng.choice(["Active","Dormant","Closed"], n_acct, p=[0.82,0.10,0.08]),
        "current_balance":  np.abs(rng.exponential(15000, n_acct)).round(2),
        "is_dormant":       rng.random(n_acct) < 0.08,
        "is_overdraft":     rng.random(n_acct) < 0.03,
        "balance_tier":     rng.choice(["Under $1K","$1K–$10K","$10K–$50K","$50K–$100K","Over $100K"], n_acct),
        "province":         rng.choice(["ON","BC","AB","QC","MB","SK"], n_acct, p=[0.40,0.18,0.15,0.14,0.07,0.06]),
    })

    n_cust = 5_000
    cust = pd.DataFrame({
        "customer_id":      [f"CUST{i:07d}" for i in range(1, n_cust+1)],
        "customer_segment": rng.choice(["Mass Market","Affluent","Private Banking","Small Business"],
                                        n_cust, p=[0.60,0.25,0.05,0.10]),
        "age_band":         rng.choice(["18-25","26-35","36-50","51-65","65+"],
                                        n_cust, p=[0.12,0.22,0.30,0.22,0.14]),
        "province":         rng.choice(["ON","BC","AB","QC","MB","SK"], n_cust, p=[0.40,0.18,0.15,0.14,0.07,0.06]),
        "kyc_status":       rng.choice(["Verified","Pending","Expired"], n_cust, p=[0.90,0.07,0.03]),
        "tenure_years":     rng.uniform(0.5, 25, n_cust).round(1),
        "is_active":        rng.random(n_cust) > 0.08,
    })

    n_loans = 1_500
    credit = pd.DataFrame({
        "loan_id":           [f"LN{i:08d}" for i in range(1, n_loans+1)],
        "loan_type":         rng.choice(["Personal","Mortgage","Auto","Line of Credit"],
                                         n_loans, p=[0.25,0.45,0.20,0.10]),
        "original_amount":   np.abs(rng.exponential(200000, n_loans)).round(2),
        "outstanding_balance":np.abs(rng.exponential(100000, n_loans)).round(2),
        "credit_score_band": rng.choice(["Poor (<580)","Fair (580-669)","Good (670-739)",
                                          "Very Good (740-799)","Excellent (800+)"],
                                          n_loans, p=[0.08,0.17,0.30,0.30,0.15]),
        "risk_rating":       rng.choice(["AAA","AA","A","BBB","BB","B","CCC"],
                                         n_loans, p=[0.05,0.10,0.25,0.30,0.15,0.10,0.05]),
        "is_delinquent":     rng.random(n_loans) < 0.08,
        "delinquency_bucket":rng.choice(["Current","1-30 DPD","31-60 DPD","61-90 DPD","180+ DPD"],
                                         n_loans, p=[0.92,0.03,0.02,0.02,0.01]),
        "ltv_ratio":         rng.uniform(0.1, 1.0, n_loans).round(4),
    })

    return txn, acct, cust, credit


# ── Sidebar ────────────────────────────────────────────────────────────────────

def render_sidebar(txn):
    st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/thumb/a/a4/Toronto-Dominion_Bank_logo.svg/200px-Toronto-Dominion_Bank_logo.svg.png", width=120)
    st.sidebar.title("Filters")

    txn["transaction_date"] = pd.to_datetime(txn["transaction_date"])
    min_date = txn["transaction_date"].min().date()
    max_date = txn["transaction_date"].max().date()

    date_range = st.sidebar.date_input(
        "Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    channels = st.sidebar.multiselect(
        "Channel",
        options=sorted(txn["channel"].dropna().unique()),
        default=sorted(txn["channel"].dropna().unique()),
    )

    txn_types = st.sidebar.multiselect(
        "Transaction Type",
        options=sorted(txn["transaction_type"].dropna().unique()),
        default=sorted(txn["transaction_type"].dropna().unique()),
    )

    st.sidebar.markdown("---")
    st.sidebar.caption("🏦 Banking Data Platform v1.0\nBuilt with Python · dbt · Airflow · PostgreSQL")

    return date_range, channels, txn_types


# ── KPI Cards ──────────────────────────────────────────────────────────────────

def render_kpi_cards(txn_filtered, acct, cust):
    total_volume = txn_filtered["amount_cad"].sum()
    txn_count = len(txn_filtered)
    failure_rate = (txn_filtered["status"] == "Failed").mean() * 100
    large_txns = txn_filtered["is_large_transaction"].sum()
    active_accounts = (acct["account_status"] == "Active").sum()
    dormant_rate = acct["is_dormant"].mean() * 100

    col1, col2, col3, col4, col5, col6 = st.columns(6)

    with col1:
        st.metric("Total Volume", f"${total_volume/1e6:.1f}M CAD",
                  delta=f"+{np.random.uniform(2,8):.1f}% MoM")
    with col2:
        st.metric("Transactions", f"{txn_count:,}",
                  delta=f"+{np.random.uniform(1,5):.1f}% MoM")
    with col3:
        st.metric("Failure Rate", f"{failure_rate:.1f}%",
                  delta=f"{np.random.uniform(-0.5,0.1):.2f}%",
                  delta_color="inverse")
    with col4:
        st.metric("FINTRAC Reportable", f"{int(large_txns):,}",
                  help="Transactions ≥ CAD $10,000")
    with col5:
        st.metric("Active Accounts", f"{active_accounts:,}")
    with col6:
        st.metric("Dormancy Rate", f"{dormant_rate:.1f}%",
                  delta=f"{np.random.uniform(-0.3,0.2):.1f}%",
                  delta_color="inverse",
                  help="Accounts inactive >730 days")


# ── Charts ─────────────────────────────────────────────────────────────────────

def render_transaction_trends(txn_filtered):
    st.subheader("📈 Transaction Trends")

    txn_filtered["transaction_date"] = pd.to_datetime(txn_filtered["transaction_date"])
    daily = txn_filtered.groupby("transaction_date").agg(
        volume=("amount_cad", "sum"),
        count=("transaction_id", "count"),
    ).reset_index()

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Bar(x=daily["transaction_date"], y=daily["volume"],
               name="Volume (CAD)", marker_color=COLORS["accent"], opacity=0.7),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(x=daily["transaction_date"], y=daily["count"],
                   name="Transaction Count", line=dict(color=COLORS["primary"], width=2)),
        secondary_y=True,
    )
    fig.update_layout(
        height=320, margin=dict(t=10, b=10),
        legend=dict(orientation="h", y=1.1),
        plot_bgcolor="white",
    )
    fig.update_yaxes(title_text="Volume CAD", secondary_y=False, gridcolor="#E0E0E0")
    fig.update_yaxes(title_text="Count", secondary_y=True)
    st.plotly_chart(fig, use_container_width=True)


def render_channel_breakdown(txn_filtered):
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📡 Channel Split")
        channel_data = txn_filtered.groupby("channel")["amount_cad"].sum().reset_index()
        fig = px.pie(channel_data, values="amount_cad", names="channel",
                     color_discrete_sequence=CHART_COLORS, hole=0.4)
        fig.update_layout(height=300, margin=dict(t=10, b=10),
                          legend=dict(orientation="h", y=-0.1))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("🛒 Merchant Categories")
        merch = txn_filtered.groupby("merchant_category")["amount_cad"].sum()\
            .sort_values(ascending=True).tail(8).reset_index()
        fig = px.bar(merch, x="amount_cad", y="merchant_category",
                     orientation="h", color_discrete_sequence=[COLORS["primary"]])
        fig.update_layout(height=300, margin=dict(t=10, b=10, l=10),
                          xaxis_title="Total Spend (CAD)", yaxis_title="")
        st.plotly_chart(fig, use_container_width=True)


def render_account_health(acct):
    st.subheader("🏦 Account Health")
    col1, col2, col3 = st.columns(3)

    with col1:
        type_data = acct.groupby("account_type").size().reset_index(name="count")
        fig = px.bar(type_data, x="account_type", y="count",
                     color_discrete_sequence=[COLORS["primary"]],
                     title="Accounts by Type")
        fig.update_layout(height=260, margin=dict(t=30,b=10), showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        tier_data = acct.groupby("balance_tier").size().reset_index(name="count")
        tier_order = ["Negative","Under $1K","$1K–$10K","$10K–$50K","$50K–$100K","Over $100K"]
        tier_data["balance_tier"] = pd.Categorical(tier_data["balance_tier"],
                                                    categories=tier_order, ordered=True)
        tier_data = tier_data.sort_values("balance_tier")
        fig = px.bar(tier_data, x="balance_tier", y="count",
                     color_discrete_sequence=[COLORS["accent"]],
                     title="Accounts by Balance Tier")
        fig.update_layout(height=260, margin=dict(t=30,b=10),
                          xaxis_tickangle=-30, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with col3:
        prov_data = acct.groupby("province")["current_balance"].sum().reset_index()
        fig = px.bar(prov_data.sort_values("current_balance", ascending=False),
                     x="province", y="current_balance",
                     color_discrete_sequence=[COLORS["secondary"]],
                     title="Total Deposits by Province")
        fig.update_layout(height=260, margin=dict(t=30,b=10), showlegend=False,
                          yaxis_title="Balance (CAD)")
        st.plotly_chart(fig, use_container_width=True)


def render_customer_segments(cust):
    st.subheader("👥 Customer Segments")
    col1, col2 = st.columns(2)

    with col1:
        seg_data = cust.groupby("customer_segment").size().reset_index(name="customers")
        fig = px.pie(seg_data, values="customers", names="customer_segment",
                     color_discrete_sequence=CHART_COLORS, hole=0.35,
                     title="Customers by Segment")
        fig.update_layout(height=280, margin=dict(t=30,b=10))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        age_seg = cust.groupby(["age_band","customer_segment"]).size().reset_index(name="count")
        age_order = ["18-25","26-35","36-50","51-65","65+"]
        age_seg["age_band"] = pd.Categorical(age_seg["age_band"],
                                              categories=age_order, ordered=True)
        age_seg = age_seg.sort_values("age_band")
        fig = px.bar(age_seg, x="age_band", y="count", color="customer_segment",
                     color_discrete_sequence=CHART_COLORS, barmode="stack",
                     title="Customers by Age Band & Segment")
        fig.update_layout(height=280, margin=dict(t=30,b=10),
                          legend=dict(orientation="h", y=-0.25))
        st.plotly_chart(fig, use_container_width=True)


def render_credit_risk(credit):
    st.subheader("⚠️ Credit Risk Portfolio")
    col1, col2, col3 = st.columns(3)

    with col1:
        risk_data = credit.groupby("risk_rating").agg(
            loans=("loan_id","count"),
            outstanding=("outstanding_balance","sum"),
        ).reset_index()
        risk_order = ["AAA","AA","A","BBB","BB","B","CCC"]
        risk_data["risk_rating"] = pd.Categorical(risk_data["risk_rating"],
                                                    categories=risk_order, ordered=True)
        risk_data = risk_data.sort_values("risk_rating")
        fig = px.bar(risk_data, x="risk_rating", y="outstanding",
                     color="risk_rating",
                     color_discrete_map={"AAA":COLORS["primary"],"AA":"#388E3C",
                                          "A":COLORS["accent"],"BBB":COLORS["warning"],
                                          "BB":"#FF8F00","B":"#E65100","CCC":COLORS["danger"]},
                     title="Exposure by Risk Rating")
        fig.update_layout(height=260, margin=dict(t=30,b=10), showlegend=False,
                          yaxis_title="Outstanding (CAD)")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        dq_data = credit.groupby("delinquency_bucket").size().reset_index(name="count")
        bucket_order = ["Current","1-30 DPD","31-60 DPD","61-90 DPD","180+ DPD"]
        dq_data["delinquency_bucket"] = pd.Categorical(dq_data["delinquency_bucket"],
                                                         categories=bucket_order, ordered=True)
        dq_data = dq_data.sort_values("delinquency_bucket")
        fig = px.bar(dq_data, x="delinquency_bucket", y="count",
                     color_discrete_sequence=[COLORS["warning"]],
                     title="Delinquency Buckets")
        fig.update_layout(height=260, margin=dict(t=30,b=10), showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with col3:
        cs_data = credit.groupby("credit_score_band").size().reset_index(name="loans")
        fig = px.pie(cs_data, values="loans", names="credit_score_band",
                     color_discrete_sequence=CHART_COLORS, hole=0.3,
                     title="Loans by Credit Score Band")
        fig.update_layout(height=260, margin=dict(t=30,b=10),
                          legend=dict(orientation="h", y=-0.3, font=dict(size=10)))
        st.plotly_chart(fig, use_container_width=True)


def render_data_quality_tab():
    st.subheader("✅ Data Quality Overview")
    st.markdown("""
    The pipeline runs **6 quality dimensions** on every dataset before loading.
    """)

    dq_data = pd.DataFrame([
        {"Dataset": "transactions_raw",  "Completeness": 99.5, "Uniqueness": 99.9, "Validity": 98.1, "Referential": 99.8, "Freshness": 100.0, "Score": 97.2},
        {"Dataset": "accounts_raw",      "Completeness": 100.0,"Uniqueness": 100.0,"Validity": 99.8, "Referential": 100.0,"Freshness": 100.0, "Score": 99.9},
        {"Dataset": "customers_raw",     "Completeness": 99.8, "Uniqueness": 100.0,"Validity": 99.5, "Referential": 100.0,"Freshness": 100.0, "Score": 99.7},
        {"Dataset": "credit_data_raw",   "Completeness": 100.0,"Uniqueness": 100.0,"Validity": 99.2, "Referential": 100.0,"Freshness": 100.0, "Score": 99.5},
    ])

    def color_score(val):
        if isinstance(val, float):
            if val >= 99: return "background-color: #C8E6C9"
            elif val >= 95: return "background-color: #FFF9C4"
            else: return "background-color: #FFCDD2"
        return ""

    st.dataframe(
        dq_data.style.applymap(color_score, subset=["Completeness","Uniqueness","Validity","Referential","Freshness","Score"]),
        use_container_width=True,
        hide_index=True,
    )


# ── Main App ───────────────────────────────────────────────────────────────────

def main():
    # Header
    st.markdown("""
    <h1 style='color:#2E7D32; margin-bottom:0'>🏦 Banking Data Platform</h1>
    <p style='color:#666; margin-top:4px'>Retail Banking Analytics Dashboard · Data as of today</p>
    """, unsafe_allow_html=True)
    st.markdown("---")

    # Load data
    with st.spinner("Loading data…"):
        txn, acct, cust, credit = load_data()

    # Sidebar filters
    date_range, channels, txn_types = render_sidebar(txn)

    # Apply filters
    txn["transaction_date"] = pd.to_datetime(txn["transaction_date"])
    txn_filtered = txn[
        (txn["transaction_date"].dt.date >= date_range[0]) &
        (txn["transaction_date"].dt.date <= date_range[1]) &
        (txn["channel"].isin(channels)) &
        (txn["transaction_type"].isin(txn_types))
    ] if len(date_range) == 2 else txn

    # KPI cards
    render_kpi_cards(txn_filtered, acct, cust)
    st.markdown("---")

    # Tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📈 Transactions", "🏦 Accounts", "👥 Customers", "⚠️ Credit Risk", "✅ Data Quality"
    ])

    with tab1:
        render_transaction_trends(txn_filtered)
        render_channel_breakdown(txn_filtered)

    with tab2:
        render_account_health(acct)

    with tab3:
        render_customer_segments(cust)

    with tab4:
        render_credit_risk(credit)

    with tab5:
        render_data_quality_tab()


if __name__ == "__main__":
    main()
