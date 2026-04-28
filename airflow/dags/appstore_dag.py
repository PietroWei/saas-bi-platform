"""Mobile app reviews + ratings ingestion DAG.

For every tracked company that ships a consumer app we fetch:

* iOS reviews + aggregate ratings via ``app-store-scraper``
* Android reviews + aggregate ratings via ``google-play-scraper``

Per-review rows are loaded into ``raw.app_reviews``, daily aggregates
into ``raw.app_ratings``. Companies without a consumer app (B2B vendors
like Murex, Bloomberg, ...) are skipped with an info log.

If both scrapers fail for a company we fall back to ``SAMPLE_DATA`` so
the demo pipeline always produces rows.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from tenacity import retry, stop_after_attempt, wait_exponential

log = logging.getLogger(__name__)

TRACKED_COMPANIES: list[dict[str, str]] = [
    # Workplace SaaS
    {"name": "Slack",     "slug": "slack"},
    {"name": "Notion",    "slug": "notion"},
    {"name": "Figma",     "slug": "figma"},
    {"name": "Asana",     "slug": "asana"},
    {"name": "Airtable",  "slug": "airtable"},
    # Fintech / payments
    {"name": "Satispay",  "slug": "satispay"},
    {"name": "Nexi",      "slug": "nexi"},
    {"name": "Revolut",   "slug": "revolut"},
    {"name": "Klarna",    "slug": "klarna"},
    {"name": "N26",       "slug": "n26"},
    # Capital markets / treasury
    {"name": "Murex",     "slug": "murex"},
    {"name": "Finastra",  "slug": "finastra"},
    {"name": "Calypso",   "slug": "calypso"},
    {"name": "FIS",       "slug": "fis"},
    {"name": "Bloomberg", "slug": "bloomberg"},
]

# App identifiers per platform. ``ios`` is the numeric App Store ID
# (without the leading ``id``); ``android`` is the Play Store package id.
# Companies absent from this dict are skipped (B2B with no consumer app).
COMPANY_APPS: dict[str, dict[str, str]] = {
    "satispay": {
        "ios":     "1066132396",                   # it.satispay.satispayapp
        "android": "com.satispay.customer",
    },
    "revolut": {
        "ios":     "990487637",
        "android": "com.revolut.revolut",
    },
    "n26": {
        "ios":     "1174616992",
        "android": "de.number26.android",
    },
    "klarna": {
        "ios":     "1115120118",
        "android": "com.myklarna.android",
    },
    "nexi": {
        # Nexi Pay (consumer wallet)
        "ios":     "1437114124",
        "android": "it.nexi.android.nexipay",
    },
}

REVIEWS_PER_APP = 100  # cap for both scrapers, keeps the run bounded

SAMPLE_DATA: dict[str, dict[str, Any]] = {
    "satispay": {
        "ios": {
            "avg_rating": 4.7, "total_ratings": 182_400,
            "rating_distribution": {"1": 4500, "2": 1800, "3": 4200, "4": 19_000, "5": 152_900},
            "reviews": [
                {"review_id": "ios_satispay_1", "rating": 5, "title": "Indispensable",
                 "content": "I use it daily, transfers are instant and the UI is clean.",
                 "author": "marco_mi", "review_date": "2026-04-10T08:12:00"},
                {"review_id": "ios_satispay_2", "rating": 4, "title": "Great app, minor bugs",
                 "content": "Occasionally fails to load history. Overall reliable.",
                 "author": "lucia_rm", "review_date": "2026-03-22T19:44:00"},
                {"review_id": "ios_satispay_3", "rating": 5, "title": "Best fintech app in Italy",
                 "content": "Customer support replied within minutes. Very pleased.",
                 "author": "andrea_to", "review_date": "2026-04-01T11:05:00"},
            ],
        },
        "android": {
            "avg_rating": 4.5, "total_ratings": 156_300,
            "rating_distribution": {"1": 8200, "2": 3100, "3": 6700, "4": 22_000, "5": 116_300},
            "reviews": [
                {"review_id": "and_satispay_1", "rating": 5, "title": "Smooth",
                 "content": "Setup took two minutes. KYC was painless.",
                 "author": "Giulia P.", "review_date": "2026-04-12T07:00:00"},
                {"review_id": "and_satispay_2", "rating": 3, "title": "Push notifications flaky",
                 "content": "Sometimes I receive payment alerts hours late.",
                 "author": "Davide R.", "review_date": "2026-03-30T14:18:00"},
                {"review_id": "and_satispay_3", "rating": 5, "title": "Switched from Postepay",
                 "content": "Lower fees, faster transfers. Very happy.",
                 "author": "Sara F.", "review_date": "2026-04-05T20:33:00"},
            ],
        },
    },
    "revolut": {
        "ios": {
            "avg_rating": 4.8, "total_ratings": 4_120_000,
            "rating_distribution": {"1": 80_000, "2": 40_000, "3": 100_000, "4": 600_000, "5": 3_300_000},
            "reviews": [
                {"review_id": "ios_revolut_1", "rating": 5, "title": "Best for travel",
                 "content": "Multi-currency wallet plus cheap FX is unbeatable.",
                 "author": "traveler_uk", "review_date": "2026-04-09T16:22:00"},
                {"review_id": "ios_revolut_2", "rating": 2, "title": "Account frozen for a week",
                 "content": "Compliance check took forever and support kept copy-pasting answers.",
                 "author": "frustrated_user", "review_date": "2026-03-15T09:48:00"},
                {"review_id": "ios_revolut_3", "rating": 5, "title": "Powerful budgeting",
                 "content": "Categorisation and analytics are spot on.",
                 "author": "saver_de", "review_date": "2026-04-02T18:10:00"},
            ],
        },
        "android": {
            "avg_rating": 4.6, "total_ratings": 3_540_000,
            "rating_distribution": {"1": 220_000, "2": 80_000, "3": 200_000, "4": 540_000, "5": 2_500_000},
            "reviews": [
                {"review_id": "and_revolut_1", "rating": 5, "title": "Daily driver",
                 "content": "Replaced my high-street bank entirely.",
                 "author": "Tom W.", "review_date": "2026-04-11T07:55:00"},
                {"review_id": "and_revolut_2", "rating": 3, "title": "Crypto fees high",
                 "content": "Spreads are way wider than dedicated exchanges.",
                 "author": "Ana K.", "review_date": "2026-03-28T22:01:00"},
                {"review_id": "and_revolut_3", "rating": 5, "title": "Premium worth it",
                 "content": "Lounge passes and travel insurance pay for the subscription.",
                 "author": "Luca B.", "review_date": "2026-04-06T12:29:00"},
            ],
        },
    },
    "n26": {
        "ios": {
            "avg_rating": 4.4, "total_ratings": 612_000,
            "rating_distribution": {"1": 24_000, "2": 12_000, "3": 28_000, "4": 88_000, "5": 460_000},
            "reviews": [
                {"review_id": "ios_n26_1", "rating": 5, "title": "Clean UI",
                 "content": "Onboarding is fast, statements are clear.",
                 "author": "berlin_user", "review_date": "2026-04-08T10:11:00"},
                {"review_id": "ios_n26_2", "rating": 2, "title": "Frozen randomly",
                 "content": "Account got blocked, took ten days to resolve.",
                 "author": "freelance_de", "review_date": "2026-03-19T08:40:00"},
                {"review_id": "ios_n26_3", "rating": 4, "title": "Solid daily account",
                 "content": "Works fine for European transfers. Wish I could earn interest.",
                 "author": "saver_es", "review_date": "2026-04-02T15:00:00"},
            ],
        },
        "android": {
            "avg_rating": 4.3, "total_ratings": 540_000,
            "rating_distribution": {"1": 32_000, "2": 18_000, "3": 30_000, "4": 80_000, "5": 380_000},
            "reviews": [
                {"review_id": "and_n26_1", "rating": 5, "title": "Modern banking",
                 "content": "Clean app, good analytics, no branch needed.",
                 "author": "Klaus M.", "review_date": "2026-04-09T09:20:00"},
                {"review_id": "and_n26_2", "rating": 1, "title": "Support nonexistent",
                 "content": "Took two weeks to get a reply for a card issue.",
                 "author": "Marta L.", "review_date": "2026-03-25T19:50:00"},
                {"review_id": "and_n26_3", "rating": 4, "title": "Decent",
                 "content": "Good for travel, weaker for credit products.",
                 "author": "Henri D.", "review_date": "2026-04-04T13:42:00"},
            ],
        },
    },
}


def _engine() -> Engine:
    """Build a SQLAlchemy engine from DATABASE_URL (Supabase)."""
    url = os.environ.get("DATABASE_URL") or os.environ.get("APP_POSTGRES_URL")
    if not url:
        raise RuntimeError("DATABASE_URL env var is not set")
    return create_engine(url, pool_pre_ping=True, future=True)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=10))
def _fetch_ios(slug: str, app_id: str) -> dict[str, Any] | None:
    """Pull iOS reviews + app metadata via app-store-scraper."""
    try:
        from app_store_scraper import AppStore  # type: ignore[import-not-found]
    except ImportError:
        log.warning("app-store-scraper not installed, skipping iOS for %s", slug)
        return None
    try:
        scraper = AppStore(country="it", app_name=slug, app_id=int(app_id))
        scraper.review(how_many=REVIEWS_PER_APP)
        reviews = scraper.reviews or []
    except Exception as exc:
        log.warning("iOS scrape failed for %s (%s): %s", slug, app_id, exc)
        return None

    distribution = {str(i): 0 for i in range(1, 6)}
    total_rating = 0.0
    out_reviews: list[dict[str, Any]] = []
    for r in reviews:
        rating = int(r.get("rating") or 0)
        if 1 <= rating <= 5:
            distribution[str(rating)] += 1
            total_rating += rating
        review_date = r.get("date")
        if isinstance(review_date, datetime):
            review_date = review_date.replace(tzinfo=None)
        out_reviews.append({
            "review_id":   str(r.get("review") or r.get("userName", "")) + "_" + str(review_date),
            "rating":      rating if 1 <= rating <= 5 else None,
            "title":       (r.get("title") or "").strip() or None,
            "content":     (r.get("review") or "").strip() or None,
            "author":      (r.get("userName") or "").strip() or None,
            "review_date": review_date,
        })
    counted = sum(distribution.values())
    avg = round(total_rating / counted, 2) if counted else None
    return {
        "avg_rating": avg,
        "total_ratings": counted,
        "rating_distribution": distribution,
        "reviews": out_reviews,
    }


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=10))
def _fetch_android(slug: str, package: str) -> dict[str, Any] | None:
    """Pull Android reviews + aggregate ratings via google-play-scraper."""
    try:
        from google_play_scraper import app, reviews, Sort  # type: ignore[import-not-found]
    except ImportError:
        log.warning("google-play-scraper not installed, skipping Android for %s", slug)
        return None
    try:
        meta = app(package, lang="en", country="us")
        result, _ = reviews(
            package, lang="en", country="us",
            sort=Sort.NEWEST, count=REVIEWS_PER_APP,
        )
    except Exception as exc:
        log.warning("Android scrape failed for %s (%s): %s", slug, package, exc)
        return None

    histogram = meta.get("histogram") or [0, 0, 0, 0, 0]
    distribution = {str(i + 1): int(histogram[i] or 0) for i in range(5)}
    out_reviews: list[dict[str, Any]] = []
    for r in result or []:
        rating = int(r.get("score") or 0)
        review_date = r.get("at")
        if isinstance(review_date, datetime):
            review_date = review_date.replace(tzinfo=None)
        out_reviews.append({
            "review_id":   str(r.get("reviewId") or ""),
            "rating":      rating if 1 <= rating <= 5 else None,
            "title":       None,  # Play Store reviews have no title field
            "content":     (r.get("content") or "").strip() or None,
            "author":      (r.get("userName") or "").strip() or None,
            "review_date": review_date,
        })
    return {
        "avg_rating":          float(meta.get("score") or 0) or None,
        "total_ratings":       int(meta.get("ratings") or 0),
        "rating_distribution": distribution,
        "reviews":             out_reviews,
    }


def _bundled(slug: str, platform: str) -> dict[str, Any] | None:
    return SAMPLE_DATA.get(slug, {}).get(platform)


def _upsert_reviews(engine: Engine, slug: str, platform: str,
                    reviews: list[dict[str, Any]]) -> int:
    if not reviews:
        return 0
    rows = [{
        "company_slug": slug, "platform": platform,
        "review_id":   r["review_id"],
        "rating":      r.get("rating"),
        "title":       r.get("title"),
        "content":     r.get("content"),
        "author":      r.get("author"),
        "review_date": r.get("review_date"),
    } for r in reviews if r.get("review_id")]
    if not rows:
        return 0
    sql = text("""
        INSERT INTO raw.app_reviews (
            company_slug, platform, review_id, rating, title,
            content, author, review_date
        )
        VALUES (
            :company_slug, :platform, :review_id, :rating, :title,
            :content, :author, :review_date
        )
        ON CONFLICT (platform, review_id) DO NOTHING
    """)
    with engine.begin() as conn:
        conn.execute(sql, rows)
    return len(rows)


def _upsert_rating(engine: Engine, slug: str, platform: str,
                   payload: dict[str, Any]) -> int:
    distribution = payload.get("rating_distribution") or {}
    row = {
        "company_slug":  slug,
        "platform":      platform,
        "avg_rating":    payload.get("avg_rating"),
        "total_ratings": payload.get("total_ratings"),
        "rating_1":      int(distribution.get("1") or 0),
        "rating_2":      int(distribution.get("2") or 0),
        "rating_3":      int(distribution.get("3") or 0),
        "rating_4":      int(distribution.get("4") or 0),
        "rating_5":      int(distribution.get("5") or 0),
    }
    sql = text("""
        INSERT INTO raw.app_ratings (
            company_slug, platform, avg_rating, total_ratings,
            rating_1, rating_2, rating_3, rating_4, rating_5
        )
        VALUES (
            :company_slug, :platform, :avg_rating, :total_ratings,
            :rating_1, :rating_2, :rating_3, :rating_4, :rating_5
        )
        ON CONFLICT (company_slug, platform, snapshot_date) DO NOTHING
    """)
    with engine.begin() as conn:
        conn.execute(sql, [row])
    return 1


def _process_company(engine: Engine, company: dict[str, str]) -> tuple[int, int]:
    """Fetch + load both platforms for one company. Returns (reviews, aggregates)."""
    slug = company["slug"]
    apps = COMPANY_APPS.get(slug)
    if not apps:
        log.info("No consumer app for %s, skipping", slug)
        return 0, 0

    reviews_loaded = 0
    aggs_loaded = 0
    for platform, fetcher, identifier in (
        ("ios",     _fetch_ios,     apps.get("ios")),
        ("android", _fetch_android, apps.get("android")),
    ):
        if not identifier:
            continue
        payload: dict[str, Any] | None = None
        try:
            payload = fetcher(slug, identifier)
        except Exception as exc:
            log.warning("%s scrape failed for %s: %s", platform, slug, exc)

        if not payload:
            payload = _bundled(slug, platform)
            if payload:
                log.info("Using bundled %s sample for %s", platform, slug)

        if not payload:
            continue

        reviews_loaded += _upsert_reviews(engine, slug, platform,
                                          payload.get("reviews") or [])
        aggs_loaded += _upsert_rating(engine, slug, platform, payload)
    return reviews_loaded, aggs_loaded


def ingest_app_reviews(**_: Any) -> None:
    """DAG entrypoint: iterate companies, load reviews + aggregate ratings."""
    engine = _engine()
    total_reviews = 0
    total_aggs = 0
    for company in TRACKED_COMPANIES:
        try:
            reviews, aggs = _process_company(engine, company)
        except Exception as exc:
            log.exception("Hard failure for %s: %s", company["slug"], exc)
            continue
        total_reviews += reviews
        total_aggs += aggs

    log.info("App store ingestion complete: %d reviews, %d rating snapshots",
             total_reviews, total_aggs)


default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="appstore_dag",
    description="Ingest mobile app reviews + ratings (iOS + Android) for tracked companies.",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["saas-bi", "ingest", "appstore"],
) as dag:
    PythonOperator(
        task_id="ingest_app_reviews",
        python_callable=ingest_app_reviews,
    )
