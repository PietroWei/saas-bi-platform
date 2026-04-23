"""Health score card — big gauge + component breakdown."""

from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st


def _gauge(value: float, title: str) -> go.Figure:
    fig = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=value,
            title={"text": title, "font": {"size": 16}},
            gauge={
                "axis": {"range": [0, 100]},
                "bar": {"color": "#2E86DE"},
                "steps": [
                    {"range": [0, 40],  "color": "#FADBD8"},
                    {"range": [40, 70], "color": "#FEF9E7"},
                    {"range": [70, 100],"color": "#D5F5E3"},
                ],
            },
        )
    )
    fig.update_layout(height=260, margin=dict(l=20, r=20, t=50, b=20))
    return fig


def render(score: dict) -> None:
    """Render an overall gauge and the three component gauges side by side."""
    st.plotly_chart(
        _gauge(float(score.get("health_score") or 0), "Overall Health Score"),
        use_container_width=True,
    )

    c1, c2, c3 = st.columns(3)
    with c1:
        st.plotly_chart(
            _gauge(float(score.get("sentiment_score_0_100") or 0), "Sentiment"),
            use_container_width=True,
        )
    with c2:
        st.plotly_chart(
            _gauge(float(score.get("funding_score_0_100") or 0), "Funding"),
            use_container_width=True,
        )
    with c3:
        st.plotly_chart(
            _gauge(float(score.get("github_score_0_100") or 0), "GitHub Activity"),
            use_container_width=True,
        )
