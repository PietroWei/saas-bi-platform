"""Pydantic response schemas shared by the routers."""

from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, Field


class Company(BaseModel):
    company_slug: str
    company_name: str
    industry: Optional[str] = None
    country: Optional[str] = None
    health_score: Optional[float] = None


class HealthScoreBreakdown(BaseModel):
    company_slug: str
    company_name: str
    industry: Optional[str] = None
    country: Optional[str] = None
    founded_year: Optional[int] = None
    health_score: float = Field(..., ge=0, le=100)
    sentiment_score_0_100: float
    funding_score_0_100: float
    github_score_0_100: float
    mention_count_180d: Optional[int] = None
    total_raised_usd: Optional[float] = None
    last_round_date: Optional[date] = None
    total_stars: Optional[int] = None
    total_commits_30d: Optional[int] = None
    total_contributors_30d: Optional[int] = None
    computed_at: Optional[datetime] = None


class Mention(BaseModel):
    mention_id: str
    mention_type: str
    title: Optional[str]
    body: Optional[str]
    points: int
    author: Optional[str]
    mention_date: Optional[date]
    sentiment_score: Optional[float]


class FundingRound(BaseModel):
    round_id: str
    round_type: Optional[str]
    announced_on: date
    amount_usd: Optional[float]
    lead_investor: Optional[str]
    investors: Optional[str]
    currency: str


class SentimentTrendPoint(BaseModel):
    month_start: date
    avg_sentiment: Optional[float]
    sentiment_3mo_avg: Optional[float]
    mention_count: int
    avg_points: Optional[float]
