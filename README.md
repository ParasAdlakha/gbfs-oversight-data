# GBFS oversight data

A longitudinal record of what public bike-share and micromobility station feeds actually report.

Every 15 minutes, a scheduled GitHub Actions job polls every key-free system in the [MobilityData GBFS catalogue](https://github.com/MobilityData/gbfs/blob/master/systems.csv). It stores two things exactly as each operator published them:

- the feed's health: whether it was reachable, the HTTP status, latency, the feed's own `last_updated`, and how many stations it listed;
- every station's `station_status` row.

The rows are not cleaned or corrected, so data-quality defects such as stale or impossible timestamps, counts above capacity, and silent stations are preserved for study. Each station's `station_information` record (location, capacity) is stored once per UTC day.

## Where the data is

Snapshots are attached to one GitHub Release per UTC day, tagged `data-YYYY-MM-DD`. The repository itself only holds the collector.

| Asset | One row per | Columns |
|---|---|---|
| `raw_YYYY_MM_DD_HHMM_feeds.csv.gz` | system | `run_ts, system_id, country, outcome, http_status, latency_ms, gbfs_version, feed_last_updated, ttl, n_stations, error` |
| `raw_YYYY_MM_DD_HHMM_status.csv.gz` | station | `run_ts, system_id, station_id, last_reported, num_bikes_available, num_ebikes_available, num_bikes_disabled, num_docks_available, num_docks_disabled, is_installed, is_renting, is_returning` |
| `static_YYYY-MM-DD_stations.csv.gz` | station (daily) | `system_id, station_id, name, lat, lon, capacity, is_virtual_station` |

Notes on the data:

- Times are POSIX seconds (UTC). GBFS 3.x RFC 3339 timestamps are converted to POSIX seconds.
- `outcome` takes one of these values: `ok`, `http_error`, `unreachable`, `malformed`, or `no_station_status` (a dock-less system that publishes no station feed).
- The catalogue is frozen at the start of the study (`collector/systems.csv`), so the population of systems doesn't drift during it.
- A first snapshot on 14 Nov 2026 polled 1,274 systems: 845 healthy, 99 returning HTTP errors, 4 unreachable and 325 dock-less. Together they listed 203,463 stations.

## Run it

```bash
python -I collector/collect.py --out data            # one snapshot, standard library only
python -I collector/collect.py --out data --limit 20  # quick test
```

## Licence and data terms

The code is MIT-licensed. The feed contents belong to their operators and are redistributed here for research under each system's published GBFS licence (see each system's `system_information` / `gbfs.json`). If you are an operator and want your system excluded, open an issue.
