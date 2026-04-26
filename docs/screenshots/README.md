# Dashboard screenshots

Drop the PNGs referenced from the project root README here. Suggested names
(used as-is by the README image links):

| File                  | Page it captures                                  |
|-----------------------|---------------------------------------------------|
| `landing.png`         | Landing - top performers + leaderboard            |
| `company_view.png`    | Company View - gauge + sentiment trend            |
| `analyst_view.png`    | Analyst View - funding timeline + red flags       |
| `compare_view.png`    | Compare View - radar chart + sentiment overlay    |

To take a screenshot:

1. Stack up: `docker compose up --build`
2. Trigger the 3 Airflow DAGs and run `docker compose run --rm dbt-runner dbt build`
3. Open `http://localhost:8501`, navigate to each page, capture full-window PNGs
4. Save them in this folder with the names above and commit
