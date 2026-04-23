"""Monthly sentiment trend chart (plotly)."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st


def render(trend: list[dict]) -> None:
    if not trend:
        st.info("No sentiment history available yet.")
        return

    df = pd.DataFrame(trend)
    df["month_start"] = pd.to_datetime(df["month_start"])

    fig = px.line(
        df,
        x="month_start",
        y=["avg_sentiment", "sentiment_3mo_avg"],
        markers=True,
        labels={
            "month_start":   "Month",
            "value":         "Sentiment (-1 … +1)",
            "variable":      "Series",
        },
        title="Review sentiment over time",
    )
    fig.update_layout(hovermode="x unified", height=420,
                      legend=dict(orientation="h", y=-0.2))
    fig.update_yaxes(range=[-1, 1])
    st.plotly_chart(fig, use_container_width=True)

    if df["review_count"].sum() > 0:
        bar = px.bar(
            df, x="month_start", y="review_count",
            labels={"review_count": "Reviews", "month_start": "Month"},
            title="Review volume",
        )
        bar.update_layout(height=260)
        st.plotly_chart(bar, use_container_width=True)
