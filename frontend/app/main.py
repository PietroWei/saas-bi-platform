"""Streamlit entrypoint - landing page.

Streamlit auto-discovers files under ``pages/`` and exposes them as
additional pages in the sidebar, so this script only needs to render
the home/landing content.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from api_client import BACKEND_URL, list_companies, list_industries
from theme import apply, hero, industry_pill_class

st.set_page_config(
    page_title="SaaS BI Platform",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)
apply()


def _rank_card(rank: int, c: dict) -> str:
    pill = (
        f"<span class='{industry_pill_class(c.get('industry'))}'>"
        f"{c.get('industry') or 'n/a'}</span>"
    )
    score = (
        f"{c['health_score']:.1f}"
        if c.get("health_score") is not None
        else "-"
    )
    return f"""
        <div class='bi-rank-card'>
            <div class='bi-rank-meta'>#{rank}</div>
            <h3>{c['company_name']}</h3>
            <div>{pill}</div>
            <div class='bi-rank-score'>{score}</div>
            <div class='bi-rank-meta'>{c.get('country') or ''}</div>
        </div>
    """


def main() -> None:
    hero(
        "📊 SaaS BI Platform",
        "Public-signal analytics for SaaS, fintech and capital-markets vendors - "
        "HackerNews mentions, funding rounds and engineering activity blended into a single "
        "<b>0-100 health score</b>.",
    )

    industries = ["All industries"] + list_industries()
    cols = st.columns([2, 1])
    with cols[0]:
        choice = st.selectbox(
            "Filter by industry",
            industries,
            index=0,
            label_visibility="collapsed",
        )
    industry_filter = None if choice == "All industries" else choice

    companies = list_companies(industry=industry_filter, limit=500)
    if not companies:
        st.info(
            "No data yet. In Airflow, trigger the three ingestion DAGs, "
            "then run `docker compose run --rm dbt-runner dbt build`."
        )
        return

    # --- Top 3 ranked cards
    st.subheader("🏆 Top performers")
    top3 = companies[:3]
    card_cols = st.columns(len(top3))
    for i, (col, c) in enumerate(zip(card_cols, top3), start=1):
        with col:
            st.markdown(_rank_card(i, c), unsafe_allow_html=True)

    st.divider()

    # --- Full leaderboard
    st.subheader("📋 Leaderboard")
    df = pd.DataFrame(companies)[
        ["company_name", "industry", "country", "health_score", "company_slug"]
    ].rename(columns={
        "company_name": "Company",
        "industry":     "Industry",
        "country":      "Country",
        "health_score": "Health score",
        "company_slug": "Slug",
    })
    st.dataframe(df, use_container_width=True, hide_index=True)

    st.divider()
    st.markdown(
        "**Where to next?**\n\n"
        "- **🏢 Company View** - perception of your own company.\n"
        "- **🔎 Analyst View** - due-diligence on any tracked company.\n"
        "- **⚖️ Compare View** - side-by-side analysis across 2-5 peers.\n\n"
        "Use the sidebar to switch pages."
    )

    st.sidebar.caption("Backend:")
    st.sidebar.code(BACKEND_URL, language="text")
    st.sidebar.caption(f"{len(companies)} companies indexed")


if __name__ == "__main__":
    main()
