"""Company View (lato azienda) - perception of your own company.

Auto-discovered by Streamlit as a separate page.
"""

from __future__ import annotations

from statistics import mean

import pandas as pd
import streamlit as st

from api_client import (
    get_health_score,
    get_reviews,
    get_sentiment_trend,
    list_companies,
)
from components import health_score_card, sentiment_chart
from theme import apply, hero, industry_pill_class

st.set_page_config(page_title="Company View", page_icon="🏢", layout="wide")
apply()


def _category_average(exclude_slug: str) -> float | None:
    """Mean health score across the other tracked companies - used as a baseline."""
    peers = [c for c in list_companies(limit=500) if c["company_slug"] != exclude_slug]
    scores = [c["health_score"] for c in peers if c.get("health_score") is not None]
    return mean(scores) if scores else None


def render() -> None:
    hero(
        "🏢 Company View",
        "How is your company perceived in the market right now?",
    )

    companies = list_companies(limit=500)
    if not companies:
        st.warning("No companies indexed yet - run DAGs + `dbt build` first.")
        return

    names = [c["company_name"] for c in companies]
    choice = st.selectbox("Select your company", names, index=0)
    slug = next(c["company_slug"] for c in companies if c["company_name"] == choice)

    score = get_health_score(slug)
    if score is None:
        st.error(f"No health score for {choice} yet.")
        return

    pill = (
        f"<span class='{industry_pill_class(score.get('industry'))}'>"
        f"{score.get('industry') or 'n/a'}</span>"
    )
    country = score.get("country") or ""
    founded = score.get("founded_year")
    sub = f"{country}" + (f" · founded {founded}" if founded else "")
    st.markdown(
        f"### {score['company_name']} &nbsp; {pill}<br>"
        f"<span class='bi-rank-meta'>{sub}</span>",
        unsafe_allow_html=True,
    )
    health_score_card.render(score)

    baseline = _category_average(slug)
    if baseline is not None:
        delta = round(score["health_score"] - baseline, 1)
        st.metric(
            "vs. category average",
            f"{score['health_score']:.1f}",
            delta=f"{delta:+.1f} pts",
        )

    st.divider()
    st.subheader("Sentiment over time")
    sentiment_chart.render(get_sentiment_trend(slug))

    st.divider()
    st.subheader("Recent reviews")
    reviews = get_reviews(slug, limit=20)
    if not reviews:
        st.info("No reviews on record.")
        return

    df = pd.DataFrame(reviews)
    if "review_date" in df.columns:
        df = df.sort_values("review_date", ascending=False, na_position="last")

    for _, row in df.head(10).iterrows():
        with st.container(border=True):
            cols = st.columns([3, 1])
            with cols[0]:
                st.markdown(f"**{row.get('review_title') or '(no title)'}**")
                st.write(row.get("review_body") or "")
            with cols[1]:
                st.metric("Rating", row.get("rating") or "-")
                st.metric("Sentiment", f"{(row.get('sentiment_score') or 0):+.2f}")
                if row.get("review_date"):
                    st.caption(str(row["review_date"]))
                if row.get("reviewer_role"):
                    st.caption(row["reviewer_role"])


render()
