# Feed Watch: pipeline observability for real public data

[![ci](https://github.com/AshokReddy010/pipeline-observability/actions/workflows/ci.yml/badge.svg)](https://github.com/AshokReddy010/pipeline-observability/actions/workflows/ci.yml)
[![monitor](https://github.com/AshokReddy010/pipeline-observability/actions/workflows/monitor.yml/badge.svg)](https://github.com/AshokReddy010/pipeline-observability/actions/workflows/monitor.yml)

**▶ [Live status page](https://ashokreddy010.github.io/pipeline-observability/)**

A small data-observability system that runs for real: every 3 hours a scheduled workflow fetches three public datasets, profiles each batch, compares it with that feed's own history, commits the results, and opens or closes GitHub issues for incidents. The status page reads the committed status document directly, so what you see is the latest real run.

| Feed | Shape | Cadence | Freshness SLA |
|---|---|---|---|
| [USGS earthquakes](https://earthquake.usgs.gov/earthquakes/feed/), last 24 h | GeoJSON, ~200–400 events | continuous | 2 h |
| [Wikipedia top 1,000 articles](https://wikimedia.org/api/rest_v1/) | REST JSON, 1,000 rows/day | daily | 54 h |
| [Chicago reported crimes](https://data.cityofchicago.org/d/ijzp-q8t2) | Socrata SODA, ~600–800 rows/day | daily, ~7 days behind | 10 days |

Three deliberately different feeds: a rolling real-time window, a fixed-size daily ranking, and a lagging municipal dataset with a weekly rhythm. One set of monitors has to work for all of them.

## The monitors

| Monitor | Fails when | Method |
|---|---|---|
| availability | the fetch fails after 3 retries | exponential backoff; 404s are not retried |
| freshness | the newest record is older than the SLA | checked at fetch time *and* when the status is built, so a feed that silently stops publishing still goes red |
| volume | row count is far from normal | robust z-score: median and MAD of the last 28 healthy runs; daily feeds compare with the same weekday once 4 are known; fails at 4.5, warns at 3 |
| schema | a column is removed or changes type (a new column warns) | column/type map diffed against the previous run; all-blank columns are left to completeness |
| completeness | a column's null rate jumps | +10 points warns, +30 fails, against the column's median null rate |
| uniqueness | ids repeat inside a batch | |
| distribution | warns only | population stability index on fixed histogram bins and category shares vs the last 14 runs; PSI ≥ 0.2 warns |

Design choices worth noting:

- **Baselines exclude bad runs.** Runs that failed volume or schema are left out of future baselines, so one outage does not teach the monitor that outages are normal.
- **Weekly rhythm is learned, not assumed.** Chicago reports far fewer crimes at weekends. Until four same-weekday runs exist, volume can warn but never fail, so the monitor does not page on its own ignorance.
- **Top-k category profiles are compared fairly.** Profiles keep the 30 largest categories. A category is compared individually only when every profile in the comparison recorded it; the rest are pooled into one bucket on both sides. Without this, a category dropping out of the top-k looked like a huge distribution shift (a bug the tests now pin down).
- **Drift warns, never fails.** The world changes (a big news day reshapes Wikipedia traffic); drift is a signal for a human, not an outage.
- **History, not raw data.** Each run is reduced to a few-KB profile (row count, schema, null rates, histograms, category shares, freshness) appended to `history/<feed>.jsonl`. Months of history fit in git and every monitor runs on history alone.

## Incidents and alerting

An incident is a streak of consecutive runs where the same monitor failed for the same feed. `pipeobs alert` keeps exactly one GitHub issue per open incident (labelled `incident`), adds a reminder every 8 failing runs, and closes the issue with a note when the check passes. Repository watchers get the usual GitHub email and mobile notifications. No extra secrets are needed: the workflow's own token is enough.

The status page shows, per feed, a ribbon of the last 120 runs, the row-count trend, live freshness, 7-day share of runs without a failure, and the latest checks; plus all incidents with duration and the 30-day mean time to recover.

When monitoring starts, each feed's archive is replayed (30 days) so the baselines exist from day one. Replayed runs are marked in the history, faded on the page, and never open GitHub issues.

## Run it locally

```bash
pip install -e ".[dev]"
pipeobs run --backfill-if-empty 30     # fetch every feed (replay 30 days the first time)
pipeobs status -o status/latest.json   # build the status document
python -m http.server -d site          # open http://localhost:8000/?src=../status/latest.json
```

## Layout

```
src/pipeobs/
  sources.py    the three feeds: fetch (network, retries) and parse (pure, tested with fixtures)
  profile.py    batch -> compact profile
  monitors.py   the seven monitors and their baselines
  runner.py     one monitoring pass; fetch failures become availability records, never crashes
  store.py      JSON Lines history, capped and written atomically
  status.py     incidents, uptime, MTTR, live freshness -> status/latest.json
  alert.py      GitHub issue sync (planned as data, then applied)
  cli.py        pipeobs run | status | alert
.github/workflows/monitor.yml   every 3 hours: run, commit, alert
site/index.html                 the status page
tests/                          fixtures shaped like each API's real payload; 40 tests, ~94% coverage
```

## Limits

This is a portfolio-scale system on free infrastructure: GitHub's scheduler can delay runs by several minutes or more, the cadence is 3 hours rather than continuous, and alerts go to GitHub issues rather than a pager. The monitors are statistical and tuned on these three feeds; another feed would need its own SLA and histogram bins. Data belongs to its publishers (USGS, Wikimedia Foundation, City of Chicago).

## License

MIT
