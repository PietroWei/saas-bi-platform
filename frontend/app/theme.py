"""Shared visual theme - CSS injection + plotly colour palette.

Every page calls ``apply()`` once at the top. Keeping the styling in one file
avoids drift between Company / Analyst / Compare views.
"""

from __future__ import annotations

import streamlit as st

# Industry colour palette - picked to read clearly on both light & dark themes.
INDUSTRY_COLORS: dict[str, str] = {
    "Workplace SaaS":              "#2E86DE",
    "Fintech / Payments":          "#27AE60",
    "Capital Markets / Treasury":  "#8E44AD",
}

# Per-company colours used in the Compare view (radar / line overlays).
COMPARE_PALETTE: list[str] = [
    "#2E86DE",  # blue
    "#E67E22",  # orange
    "#27AE60",  # green
    "#8E44AD",  # purple
    "#C0392B",  # red
]

_CSS = """
<style>
  /* ---- Layout polish ---- */
  .block-container { padding-top: 1.5rem; padding-bottom: 3rem; }

  /* ---- Hero / page title ---- */
  .bi-hero {
      padding: 1.2rem 1.4rem;
      border-radius: 14px;
      background: linear-gradient(120deg, #1f3a93 0%, #2E86DE 60%, #27AE60 100%);
      color: white;
      margin-bottom: 1rem;
      box-shadow: 0 6px 18px rgba(0,0,0,0.12);
  }
  .bi-hero h1 { color: white; margin: 0 0 .25rem 0; font-size: 1.7rem; }
  .bi-hero p  { color: rgba(255,255,255,0.92); margin: 0; font-size: .95rem; }

  /* ---- Score pills ---- */
  .bi-pill {
      display: inline-block; padding: .15rem .55rem; border-radius: 999px;
      font-size: .78rem; font-weight: 600; margin-right: .35rem;
  }
  .bi-pill-fintech { background: #d5f5e3; color: #145a32; }
  .bi-pill-saas    { background: #d6eaf8; color: #1b4f72; }
  .bi-pill-cm      { background: #ebdef0; color: #4a235a; }
  .bi-pill-other   { background: #eaeded; color: #2c3e50; }

  /* ---- Top card grid ---- */
  .bi-rank-card {
      border: 1px solid rgba(120,120,120,0.18);
      border-radius: 12px;
      padding: 1rem 1.1rem;
      background: var(--background-color, #ffffff);
      box-shadow: 0 1px 3px rgba(0,0,0,0.04);
      height: 100%;
  }
  .bi-rank-card h3 { margin: 0 0 .25rem 0; font-size: 1.05rem; }
  .bi-rank-score   { font-size: 2rem; font-weight: 700; color: #2E86DE; margin: .25rem 0; }
  .bi-rank-meta    { color: #7f8c8d; font-size: .82rem; }

  /* ---- Tighten Streamlit metrics ---- */
  div[data-testid="stMetric"] { padding-top: 0.25rem; }
</style>
"""


def industry_pill_class(industry: str | None) -> str:
    if not industry:
        return "bi-pill bi-pill-other"
    if "Fintech" in industry:
        return "bi-pill bi-pill-fintech"
    if "Capital" in industry:
        return "bi-pill bi-pill-cm"
    if "SaaS" in industry:
        return "bi-pill bi-pill-saas"
    return "bi-pill bi-pill-other"


def hero(title: str, subtitle: str) -> None:
    """Render the gradient page header."""
    st.markdown(
        f"<div class='bi-hero'><h1>{title}</h1><p>{subtitle}</p></div>",
        unsafe_allow_html=True,
    )


def apply() -> None:
    """Inject the shared CSS - call once per page."""
    st.markdown(_CSS, unsafe_allow_html=True)
