"""Compare View - side-by-side analysis of 2-3 companies.

Auto-discovered by Streamlit as a separate page in the sidebar.
The view is built around three artefacts:
  * a radar chart of the three component scores,
  * a sentiment-over-time overlay,
  * a tabular feature-by-feature comparison.
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from api_client import (
    compare_companies,
    get_sentiment_trend,
    list_companies,
    list_industries,
)
from theme import COMPARE_PALETTE, apply, hero, industry_pill_class

st.set_page_config(page_title="Compare View", page_icon="⚖️", layout="wide")
apply()


def _radar(breakdowns: list[dict]) -> go.Figure:
    categories = ["Sentiment", "Funding", "GitHub Activity"]
    fig = go.Figure()
    for i, b in enumerate(breakdowns):
        fig.add_trace(go.Scatterpolar(
            r=[
                b.get("sentiment_score_0_100") or 0,
                b.get("funding_score_0_100")   or 0,
                b.get("github_score_0_100")    or 0,
            ],
            theta=categories,
            fill="toself",
            name=b["company_name"],
            line=dict(color=COMPARE_PALETTE[i % len(COMPARE_PALETTE)]),
            opacity=0.55,
        ))
    fig.update_layout(
        polar=dict(radialaxis=dict(range=[0, 100], showline=False)),
        height=480,
        showlegend=True,
        legend=dict(orientation="h", y=-0.1),
        margin=dict(l=20, r=20, t=30, b=20),
        title="Component scores (0-100) - radar comparison",
    )
    return fig


def _sentiment_overlay(slugs: list[str], names: list[str]) -> None:
    frames: list[pd.DataFrame] = []
    for slug, name in zip(slugs, names):
        trend = get_sentiment_trend(slug)
        if not trend:
            continue
        df = pd.DataFrame(trend)
        df["company_name"] = name
        frames.append(df)
    if not frames:
        st.info("No sentiment trend data available for the selected companies.")
        return

    big = pd.concat(frames, ignore_index=True)
    big["month_start"] = pd.to_datetime(big["month_start"])
    fig = px.line(
        big,
        x="month_start", y="avg_sentiment",
        color="company_name", markers=True,
        color_discrete_sequence=COMPARE_PALETTE,
        labels={"month_start": "Month", "avg_sentiment": "Avg sentiment (-1 … +1)",
                "company_name": "Company"},
        title="Sentiment trend overlay",
    )
    fig.update_layout(hovermode="x unified", height=420,
                      legend=dict(orientation="h", y=-0.2))
    fig.update_yaxes(range=[-1, 1])
    st.plotly_chart(fig, use_container_width=True)


def _comparison_table(breakdowns: list[dict]) -> None:
    rows = []
    for b in breakdowns:
        rows.append({
            "Company":              b["company_name"],
            "Industry":             b.get("industry") or "-",
            "Country":              b.get("country") or "-",
            "Founded":              b.get("founded_year") or "-",
            "Health score":         f"{b['health_score']:.1f}",
            "Sentiment / 100":      f"{b['sentiment_score_0_100']:.1f}",
            "Funding / 100":        f"{b['funding_score_0_100']:.1f}",
            "GitHub / 100":         f"{b['github_score_0_100']:.1f}",
            "HN mentions (180d)":   b.get("mention_count_180d") or 0,
            "Total raised (M$)":    f"{(b.get('total_raised_usd') or 0) / 1_000_000:,.1f}",
            "Last round":           b.get("last_round_date") or "-",
            "GitHub stars":         b.get("total_stars") or 0,
            "Commits (30d)":        b.get("total_commits_30d") or 0,
            "Contributors (30d)":   b.get("total_contributors_30d") or 0,
        })
    df = pd.DataFrame(rows).set_index("Company").T
    st.dataframe(df, use_container_width=True)


def _winner_callout(breakdowns: list[dict]) -> None:
    """Tiny qualitative summary - who wins on each axis?"""
    if len(breakdowns) < 2:
        return

    def _best(key: str) -> dict:
        return max(breakdowns, key=lambda b: (b.get(key) or 0))

    overall   = _best("health_score")
    sentiment = _best("sentiment_score_0_100")
    funding   = _best("funding_score_0_100")
    github    = _best("github_score_0_100")

    cols = st.columns(4)
    cols[0].metric("🏆 Overall",   overall["company_name"],   f"{overall['health_score']:.1f}")
    cols[1].metric("💬 Sentiment", sentiment["company_name"], f"{sentiment['sentiment_score_0_100']:.1f}")
    cols[2].metric("💰 Funding",   funding["company_name"],   f"{funding['funding_score_0_100']:.1f}")
    cols[3].metric("⚙️ GitHub",    github["company_name"],    f"{github['github_score_0_100']:.1f}")


def render() -> None:
    hero(
        "⚖️ Compare View",
        "Pick 2-3 companies - typically a target and its peers - and inspect "
        "where each one wins or loses on the three component signals.",
    )

    industries = ["All industries"] + list_industries()
    industry_choice = st.selectbox(
        "Filter peer pool by industry",
        industries,
        index=0,
        help="Optional - narrow the picker so the radar overlays stay readable.",
    )
    industry_filter = None if industry_choice == "All industries" else industry_choice

    pool = list_companies(industry=industry_filter, limit=500)
    if not pool:
        st.warning(
            "No companies indexed yet - trigger the DAGs and run "
            "`docker compose run --rm dbt-runner dbt build`."
        )
        return

    label_map: dict[str, str] = {
        f"{c['company_name']}  ·  {c.get('industry') or '-'}": c["company_slug"]
        for c in pool
    }
    default_picks = list(label_map.keys())[: min(3, len(label_map))]
    picked_labels = st.multiselect(
        "Pick 2 to 5 companies",
        list(label_map.keys()),
        default=default_picks,
        max_selections=5,
    )

    if len(picked_labels) < 2:
        st.info("Pick at least two companies to start the comparison.")
        return

    slugs = [label_map[l] for l in picked_labels]
    breakdowns = compare_companies(slugs)
    if not breakdowns:
        st.error("Could not load comparison data.")
        return

    # --- Industry pills strip
    pill_html = " ".join(
        f"<span class='{industry_pill_class(b.get('industry'))}'>"
        f"{b['company_name']} · {b.get('industry') or 'n/a'}</span>"
        for b in breakdowns
    )
    st.markdown(pill_html, unsafe_allow_html=True)
    st.write("")

    # --- Winner callouts
    _winner_callout(breakdowns)

    st.divider()
    # --- Radar
    st.plotly_chart(_radar(breakdowns), use_container_width=True)

    st.divider()
    # --- Sentiment overlay
    st.subheader("📈 Sentiment over time")
    _sentiment_overlay(slugs, [b["company_name"] for b in breakdowns])

    st.divider()
    # --- Detailed comparison table
    st.subheader("🔍 Detailed breakdown")
    _comparison_table(breakdowns)


render()
