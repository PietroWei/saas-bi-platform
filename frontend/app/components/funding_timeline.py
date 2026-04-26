"""Funding timeline - scatter of rounds over time, sized by amount."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st


def render(rounds: list[dict]) -> None:
    if not rounds:
        st.info("No funding rounds on record.")
        return

    df = pd.DataFrame(rounds)
    df["announced_on"] = pd.to_datetime(df["announced_on"])
    df["amount_usd"] = pd.to_numeric(df["amount_usd"], errors="coerce").fillna(0)
    df["amount_m"] = (df["amount_usd"] / 1_000_000).round(2)

    fig = px.scatter(
        df,
        x="announced_on",
        y="amount_m",
        size=df["amount_m"].clip(lower=1),
        color="round_type",
        hover_data={
            "round_type":    True,
            "lead_investor": True,
            "investors":     True,
            "amount_usd":    ":,.0f",
            "amount_m":      False,
            "announced_on":  "|%Y-%m-%d",
        },
        labels={"announced_on": "Date", "amount_m": "Amount ($M)"},
        title="Funding timeline",
    )
    fig.update_layout(height=420, legend=dict(orientation="h", y=-0.2))
    st.plotly_chart(fig, use_container_width=True)

    total = df["amount_usd"].sum()
    st.metric("Total raised (tracked rounds)", f"${total/1_000_000:,.1f}M")
    st.dataframe(
        df[["announced_on", "round_type", "amount_usd", "lead_investor", "investors"]]
          .sort_values("announced_on", ascending=False),
        use_container_width=True, hide_index=True,
    )
