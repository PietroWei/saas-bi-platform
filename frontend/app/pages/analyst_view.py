"""Analyst View (lato analyst) — due-diligence dashboard for any tracked company.

Auto-discovered by Streamlit as a separate page.
"""

from __future__ import annotations

import streamlit as st

from api_client import (
    get_funding,
    get_health_score,
    get_reviews,
    get_sentiment_trend,
    search_companies,
)
from components import funding_timeline, health_score_card, sentiment_chart

st.set_page_config(page_title="Analyst View", page_icon="🔎", layout="wide")


def _red_flags(score: dict, reviews: list[dict], funding: list[dict]) -> list[str]:
    """Compute ad-hoc warnings an analyst would care about."""
    flags: list[str] = []
    if score.get("health_score") is not None and score["health_score"] < 40:
        flags.append("Overall health score below 40/100 — broadly unhealthy.")
    if score.get("sentiment_score_0_100") is not None and score["sentiment_score_0_100"] < 45:
        flags.append("Review sentiment below neutral — customers unhappy.")
    if score.get("github_score_0_100") is not None and score["github_score_0_100"] < 20:
        flags.append("GitHub activity very low — engineering output may be stalling.")
    if score.get("last_round_date") is None:
        flags.append("No funding rounds in the tracked recency window.")
    if reviews and len(reviews) < 5:
        flags.append(f"Only {len(reviews)} reviews captured — low confidence.")
    if funding:
        latest = max(r["announced_on"] for r in funding if r.get("announced_on"))
        if str(latest) < "2022-01-01":
            flags.append(f"Last funding round is old ({latest}).")
    return flags


def render() -> None:
    st.title("🔎 Analyst View")
    st.caption("Due-diligence on any SaaS company tracked by the platform.")

    query = st.text_input("Search companies", value="", placeholder="e.g. notion, slack…")
    if not query:
        st.info("Type at least one character to search.")
        return

    results = search_companies(query, limit=20)
    if not results:
        st.warning("No matches. Try a different substring.")
        return

    label_map = {f"{r['company_name']} ({r['company_slug']})": r["company_slug"] for r in results}
    picked = st.selectbox("Pick a company", list(label_map.keys()))
    slug = label_map[picked]

    score = get_health_score(slug)
    if score is None:
        st.error("No health score for this company yet.")
        return

    st.subheader(f"{score['company_name']} — analyst dossier")
    health_score_card.render(score)

    c1, c2, c3 = st.columns(3)
    c1.metric("Reviews (180d)", score.get("review_count_180d") or 0)
    c2.metric("Total raised (3y)", f"${(score.get('total_raised_usd') or 0)/1_000_000:.1f}M")
    c3.metric("GitHub stars", score.get("total_stars") or 0)

    st.divider()
    st.subheader("Funding timeline")
    funding = get_funding(slug)
    funding_timeline.render(funding)

    st.divider()
    st.subheader("Review sentiment trend")
    sentiment_chart.render(get_sentiment_trend(slug))

    st.divider()
    st.subheader("GitHub activity (latest snapshot)")
    gh_c1, gh_c2, gh_c3 = st.columns(3)
    gh_c1.metric("Commits (30d)", score.get("total_commits_30d") or 0)
    gh_c2.metric("Contributors (30d)", score.get("total_contributors_30d") or 0)
    gh_c3.metric("Stars", score.get("total_stars") or 0)

    st.divider()
    st.subheader("🚩 Red flags")
    flags = _red_flags(score, get_reviews(slug, limit=100), funding)
    if not flags:
        st.success("No red flags surfaced by the current signals.")
    else:
        for f in flags:
            st.warning(f)


render()
