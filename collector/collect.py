"""Longitudinal GBFS collector (stdlib only).

One run = one snapshot of every public, key-free system in the MobilityData
catalogue. Writes:
  data/raw/YYYY/MM/DD/HHMM_feeds.csv.gz     one row per system (feed-level health)
  data/raw/YYYY/MM/DD/HHMM_status.csv.gz    one row per station (station_status)
  data/static/YYYY-MM-DD/stations.csv.gz    station_information, once per UTC day
State (discovered feed URLs) is cached in data/state/discovery.json.

Raw values are stored as published: no cleaning happens here, so the data
contract in analysis/ is applied to exactly what each operator served.
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import csv
import datetime as dt
import gzip
import io
import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path

UA = "gbfs-oversight-study/1.0 (academic research; contact via repository issues)"
TIMEOUT = 20
ROOT = Path(__file__).resolve().parent.parent

FEED_COLS = ["run_ts", "system_id", "country", "outcome", "http_status", "latency_ms",
             "gbfs_version", "feed_last_updated", "ttl", "n_stations", "error"]
STATUS_COLS = ["run_ts", "system_id", "station_id", "last_reported", "num_bikes_available",
               "num_ebikes_available", "num_bikes_disabled", "num_docks_available",
               "num_docks_disabled", "is_installed", "is_renting", "is_returning"]
STATIC_COLS = ["system_id", "station_id", "name", "lat", "lon", "capacity", "is_virtual_station"]


def fetch_json(url: str) -> tuple[dict, int, int]:
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    t0 = time.monotonic()
    with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
        body = r.read()
        status = r.status
    return json.loads(body), status, int((time.monotonic() - t0) * 1000)


def to_epoch(v):
    """GBFS <=2.x uses POSIX seconds; 3.x uses RFC 3339 strings."""
    if v is None or v == "":
        return ""
    if isinstance(v, (int, float)):
        return int(v)
    try:
        return int(dt.datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp())
    except ValueError:
        return f"unparsed:{v}"


def text(v):
    """GBFS 3.x localises names as [{text, language}]; take the first."""
    if isinstance(v, list) and v:
        return v[0].get("text", "")
    return v if v is not None else ""


def discover(url: str) -> tuple[dict[str, str], str]:
    d, _, _ = fetch_json(url)
    data = d.get("data", {})
    feeds = data.get("feeds")
    if feeds is None:  # v1/v2: {"en": {"feeds": [...]}}
        langs = [k for k in data if isinstance(data[k], dict) and "feeds" in data[k]]
        lang = "en" if "en" in langs else (langs[0] if langs else None)
        feeds = data[lang]["feeds"] if lang else []
    return {f["name"]: f["url"] for f in feeds if "name" in f and "url" in f}, str(d.get("version", ""))


def ebikes(s: dict):
    # 3.x reports e-bikes only via vehicle_types_available, which needs vehicle_types.json
    # to interpret; left blank there rather than guessed.
    return s.get("num_ebikes_available", "")


def poll_system(sysrow: dict, disc: dict, run_ts: int, want_static: bool):
    sid = sysrow["System ID"]
    feed = dict(run_ts=run_ts, system_id=sid, country=sysrow["Country Code"], outcome="ok",
                http_status="", latency_ms="", gbfs_version="", feed_last_updated="", ttl="",
                n_stations="", error="")
    rows, static = [], []
    try:
        entry = disc.get(sid)
        if entry is None or entry.get("day") != run_day(run_ts):
            urls, ver = discover(sysrow["Auto-Discovery URL"])
            entry = {"urls": urls, "version": ver, "day": run_day(run_ts)}
            disc[sid] = entry
        urls = entry["urls"]
        feed["gbfs_version"] = entry["version"]
        if "station_status" not in urls:
            feed["outcome"] = "no_station_status"
            return feed, rows, static
        d, status, ms = fetch_json(urls["station_status"])
        feed.update(http_status=status, latency_ms=ms, feed_last_updated=to_epoch(d.get("last_updated")),
                    ttl=d.get("ttl", ""), gbfs_version=d.get("version", entry["version"]))
        stations = d.get("data", {}).get("stations", [])
        feed["n_stations"] = len(stations)
        for s in stations:
            rows.append(dict(
                run_ts=run_ts, system_id=sid, station_id=s.get("station_id", ""),
                last_reported=to_epoch(s.get("last_reported")),
                num_bikes_available=s.get("num_bikes_available", s.get("num_vehicles_available", "")),
                num_ebikes_available=ebikes(s), num_bikes_disabled=s.get("num_bikes_disabled",
                                                                       s.get("num_vehicles_disabled", "")),
                num_docks_available=s.get("num_docks_available", ""),
                num_docks_disabled=s.get("num_docks_disabled", ""),
                is_installed=s.get("is_installed", ""), is_renting=s.get("is_renting", ""),
                is_returning=s.get("is_returning", "")))
        if want_static and "station_information" in urls:
            info, _, _ = fetch_json(urls["station_information"])
            for s in info.get("data", {}).get("stations", []):
                static.append(dict(system_id=sid, station_id=s.get("station_id", ""), name=text(s.get("name")),
                                   lat=s.get("lat", ""), lon=s.get("lon", ""), capacity=s.get("capacity", ""),
                                   is_virtual_station=s.get("is_virtual_station", "")))
    except urllib.error.HTTPError as e:
        feed.update(outcome="http_error", http_status=e.code, error=str(e)[:200])
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        feed.update(outcome="unreachable", error=f"{type(e).__name__}: {e}"[:200])
    except (ValueError, KeyError, TypeError, AttributeError) as e:
        feed.update(outcome="malformed", error=f"{type(e).__name__}: {e}"[:200])
    return feed, rows, static


def run_day(ts: int) -> str:
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).strftime("%Y-%m-%d")


def write_gz(path: Path, cols: list[str], rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=cols, lineterminator="\n")
    w.writeheader()
    w.writerows(rows)
    with gzip.open(path, "wt", encoding="utf-8", compresslevel=9) as f:
        f.write(buf.getvalue())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalogue", default=str(ROOT / "collector" / "systems.csv"))
    ap.add_argument("--out", default=str(ROOT / "data"))
    ap.add_argument("--workers", type=int, default=48)
    ap.add_argument("--limit", type=int, default=0, help="poll only the first N systems (testing)")
    ap.add_argument("--skip-static", action="store_true",
                    help="today's station_information is already stored elsewhere")
    args = ap.parse_args()

    out = Path(args.out)
    systems = [r for r in csv.DictReader(open(args.catalogue, encoding="utf-8"))
               if not r["Authentication Type"].strip() and r["Auto-Discovery URL"].strip()]
    if args.limit:
        systems = systems[: args.limit]
    state_path = out / "state" / "discovery.json"
    disc = json.loads(state_path.read_text()) if state_path.exists() else {}

    run_ts = int(time.time())
    day = run_day(run_ts)
    static_dir = out / "static" / day
    want_static = not args.skip_static and not (static_dir / "stations.csv.gz").exists()

    t0 = time.monotonic()
    feeds, status, static = [], [], []
    with cf.ThreadPoolExecutor(args.workers) as ex:
        for f, rows, st in ex.map(lambda s: poll_system(s, disc, run_ts, want_static), systems):
            feeds.append(f)
            status.extend(rows)
            static.extend(st)

    stamp = dt.datetime.fromtimestamp(run_ts, dt.timezone.utc)
    base = out / "raw" / stamp.strftime("%Y/%m/%d") / stamp.strftime("%H%M")
    write_gz(Path(f"{base}_feeds.csv.gz"), FEED_COLS, feeds)
    write_gz(Path(f"{base}_status.csv.gz"), STATUS_COLS, status)
    if want_static:
        write_gz(static_dir / "stations.csv.gz", STATIC_COLS, static)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(disc, sort_keys=True))

    from collections import Counter
    print(json.dumps({"run_ts": run_ts, "systems": len(systems), "stations": len(status),
                      "outcomes": Counter(f["outcome"] for f in feeds),
                      "static_rows": len(static), "seconds": round(time.monotonic() - t0, 1)}))


if __name__ == "__main__":
    main()
