#!/usr/bin/env python3
"""
Emas 10g — background gold alert worker (Telegram + email)
==========================================================

Checks the live 10g 999 gold price in MYR every 15 minutes and sends alerts when:
  1. the price falls to/below your target (re-arms after it climbs back 0.3% above), or
  2. the composite of the 5 market drivers shifts strongly bullish or bearish, or
  3. the price moves sharply (default ±1.5%) between two checks.

Pure Python standard library — no pip installs needed. Python 3.8+.

Usage
-----
  python3 alerts.py            # loop forever, checking every CHECK_INTERVAL_MIN minutes
  python3 alerts.py --once     # single check, then exit (use this with cron / GitHub Actions)
  python3 alerts.py --test     # send a test message to every configured channel
  python3 alerts.py --dry-run  # run a check and print what would be sent, send nothing

Configuration
-------------
Set environment variables, or put KEY=value lines in a `.env` file next to this script.
Only TARGET_10G_MYR plus one channel (Telegram or email) are required.

  TARGET_10G_MYR=4800            # alert when 10g price <= this (MYR)
  TARGET_BASIS=spot              # spot | retail  (retail = spot + RETAIL_SPREAD_PCT)

  TELEGRAM_BOT_TOKEN=123456:ABC...   # from @BotFather
  TELEGRAM_CHAT_ID=987654321         # your chat id (see README)

  SMTP_HOST=smtp.gmail.com  SMTP_PORT=465  SMTP_USER=you@gmail.com
  SMTP_PASS=<16-char app password>   EMAIL_TO=you@gmail.com   EMAIL_FROM=(defaults to SMTP_USER)

  TWELVEDATA_API_KEY=            # optional live XAU/USD (else keyless gold-api.com)
  METALS_DEV_API_KEY=            # optional alternative
  FRED_API_KEY=                  # optional: auto-pull Fed target rate + CPI from FRED

  FED_BIAS=hike                  # hike | hawkish | neutral | dovish | cut | auto (auto needs FRED)
  CPI_LATEST=3.7  CPI_PRIOR=3.4  # US CPI y/y %, ignored when FRED_API_KEY is set
  GEO_LEVEL=elevated             # calm | moderate | elevated | crisis
  SUPPORTS=3700,3500             # USD/oz support floors
  RETAIL_SPREAD_PCT=7  BUYBACK_SPREAD_PCT=2
  CHECK_INTERVAL_MIN=15  SIGNAL_THRESHOLD=0.35  SHARP_MOVE_PCT=1.5
  STATE_FILE=alerts_state.json
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import smtplib
import ssl
import sys
import time
import urllib.parse
import urllib.request
from email.message import EmailMessage
from pathlib import Path

OZ = 31.1034768
SIGMA_WEEK = 0.019
HERE = Path(__file__).resolve().parent
MYT = dt.timezone(dt.timedelta(hours=8))

# Reference levels (news reports, Nov 2026) used until the worker has 7 days of its own readings.
BASELINE_DXY = 102.41      # 11 Nov 2026
BASELINE_USDMYR = 4.0828   # 12 Nov 2026


# --------------------------------------------------------------------------- config
def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        v = re.split(r"\s+#", v, maxsplit=1)[0]  # drop inline comments
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def env(key: str, default: str = "") -> str:
    return os.environ.get(key, default).strip()


def env_f(key: str, default: float) -> float:
    try:
        return float(env(key, str(default)))
    except ValueError:
        return default


class Config:
    def __init__(self) -> None:
        load_dotenv(HERE / ".env")
        self.target = env_f("TARGET_10G_MYR", 0.0)
        self.basis = env("TARGET_BASIS", "spot").lower()
        self.tg_token = env("TELEGRAM_BOT_TOKEN")
        self.tg_chat = env("TELEGRAM_CHAT_ID")
        self.smtp_host = env("SMTP_HOST")
        self.smtp_port = int(env_f("SMTP_PORT", 465))
        self.smtp_user = env("SMTP_USER")
        self.smtp_pass = env("SMTP_PASS")
        self.email_to = env("EMAIL_TO")
        self.email_from = env("EMAIL_FROM") or self.smtp_user
        self.td_key = env("TWELVEDATA_API_KEY")
        self.md_key = env("METALS_DEV_API_KEY")
        self.fred_key = env("FRED_API_KEY")
        self.fed_bias = env("FED_BIAS", "hike").lower()
        self.fed_rate = env("FED_RATE", "3.75–4.00%")
        self.cpi = env_f("CPI_LATEST", 3.7)
        self.cpi_prev = env_f("CPI_PRIOR", 3.4)
        self.geo = env("GEO_LEVEL", "elevated").lower()
        self.supports = sorted((float(x) for x in env("SUPPORTS", "3700,3500").split(",") if x.strip()), reverse=True)
        self.retail = env_f("RETAIL_SPREAD_PCT", 7.0)
        self.buyback = env_f("BUYBACK_SPREAD_PCT", 2.0)
        self.interval = env_f("CHECK_INTERVAL_MIN", 15.0)
        self.threshold = env_f("SIGNAL_THRESHOLD", 0.35)
        self.sharp = env_f("SHARP_MOVE_PCT", 1.5)
        sf = Path(env("STATE_FILE", "alerts_state.json"))
        self.state_file = sf if sf.is_absolute() else HERE / sf


# --------------------------------------------------------------------------- http
def get_json(url: str, headers: dict | None = None, timeout: int = 12) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "emas10g-alerts/1.0", **(headers or {})})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def post_json(url: str, payload: dict, timeout: int = 12) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


# --------------------------------------------------------------------------- data sources
def fetch_fx() -> tuple[float, float | None, str]:
    """Returns (USD/MYR, DXY or None, source name)."""
    try:
        j = get_json("https://open.er-api.com/v6/latest/USD")
        r = j["rates"]
        dxy = (50.14348112 * (1 / r["EUR"]) ** -0.576 * r["JPY"] ** 0.136 * (1 / r["GBP"]) ** -0.119
               * r["CAD"] ** 0.091 * r["SEK"] ** 0.042 * r["CHF"] ** 0.036)
        return float(r["MYR"]), dxy, "ExchangeRate-API"
    except Exception as e:  # noqa: BLE001
        log(f"er-api failed: {e}")
    j = get_json("https://api.bnm.gov.my/public/exchange-rate/USD",
                 headers={"Accept": "application/vnd.BNM.API.v1+json"})
    return float(j["data"]["rate"]["middle_rate"]), None, "Bank Negara Malaysia"


def fetch_gold(cfg: Config) -> tuple[float, str]:
    errors = []
    if cfg.td_key:
        try:
            j = get_json(f"https://api.twelvedata.com/price?symbol=XAU/USD&apikey={urllib.parse.quote(cfg.td_key)}")
            if "price" in j:
                return float(j["price"]), "Twelve Data"
            errors.append(f"TwelveData: {j.get('message')}")
        except Exception as e:  # noqa: BLE001
            errors.append(f"TwelveData: {e}")
    try:
        j = get_json("https://api.gold-api.com/price/XAU")
        return float(j["price"]), "gold-api.com"
    except Exception as e:  # noqa: BLE001
        errors.append(f"gold-api: {e}")
    if cfg.md_key:
        try:
            j = get_json(f"https://api.metals.dev/v1/latest?api_key={urllib.parse.quote(cfg.md_key)}&currency=USD&unit=toz")
            return float(j["metals"]["gold"]), "Metals.dev"
        except Exception as e:  # noqa: BLE001
            errors.append(f"metals.dev: {e}")
    raise RuntimeError("All gold sources failed: " + "; ".join(errors))


def fred_series(key: str, series: str, limit: int) -> list[tuple[str, float]]:
    url = ("https://api.stlouisfed.org/fred/series/observations?"
           + urllib.parse.urlencode({"series_id": series, "api_key": key, "file_type": "json",
                                     "sort_order": "desc", "limit": limit}))
    obs = get_json(url)["observations"]
    return [(o["date"], float(o["value"])) for o in obs if o["value"] not in (".", "")]


def apply_fred(cfg: Config) -> None:
    """Overrides Fed rate / CPI config with the latest FRED data when a key is set."""
    if not cfg.fred_key:
        return
    try:
        up = fred_series(cfg.fred_key, "DFEDTARU", 200)   # daily upper bound, newest first
        lo = fred_series(cfg.fred_key, "DFEDTARL", 1)
        cfg.fed_rate = f"{lo[0][1]:.2f}–{up[0][1]:.2f}%"
        if cfg.fed_bias == "auto":
            older = up[-1][1]  # ~6+ months of business days back
            cfg.fed_bias = "hawkish" if up[0][1] > older else "dovish" if up[0][1] < older else "neutral"
        cpi = fred_series(cfg.fred_key, "CPIAUCSL", 14)    # monthly index, newest first
        cfg.cpi = (cpi[0][1] / cpi[12][1] - 1) * 100
        cfg.cpi_prev = (cpi[1][1] / cpi[13][1] - 1) * 100
    except Exception as e:  # noqa: BLE001
        log(f"FRED failed, using config values: {e}")
    if cfg.fed_bias == "auto":
        cfg.fed_bias = "neutral"


# --------------------------------------------------------------------------- model
def clamp(v: float, a: float, b: float) -> float:
    return max(a, min(b, v))


def reading_days_ago(hist: dict, days: int) -> tuple[str, float] | None:
    cutoff = (dt.datetime.now(MYT) - dt.timedelta(days=days)).date().isoformat()
    keys = sorted(k for k in hist if k <= cutoff)
    return (keys[-1], hist[keys[-1]]) if keys else None


def drivers(cfg: Config, xau: float, fx: float, dxy: float | None, st: dict) -> list[dict]:
    out = []
    fed_map = {"hike": -0.8, "hawkish": -0.4, "neutral": 0.0, "dovish": 0.4, "cut": 0.8}
    out.append({"id": "fed", "name": "Fed rates", "score": fed_map.get(cfg.fed_bias, 0.0),
                "reading": f"{cfg.fed_rate} · {cfg.fed_bias}"})

    dc = cfg.cpi - cfg.cpi_prev
    cs = clamp(-dc * 1.2, -0.8, 0.8)
    if cfg.cpi > 3 and dc >= 0:
        cs = min(cs, -0.3)
    out.append({"id": "cpi", "name": "US CPI", "score": cs, "reading": f"{cfg.cpi:.1f}% y/y (prior {cfg.cpi_prev:.1f}%)"})

    ref = reading_days_ago(st.get("dxy_hist", {}), 7) or ("baseline", BASELINE_DXY)
    if dxy:
        ch = (dxy / ref[1] - 1) * 100
        out.append({"id": "dxy", "name": "DXY", "score": clamp(-ch, -1, 1), "reading": f"{dxy:.2f} ({ch:+.2f}% vs {ref[0]})"})
    else:
        out.append({"id": "dxy", "name": "DXY", "score": 0.0, "reading": "unavailable"})

    ref = reading_days_ago(st.get("fx_hist", {}), 7) or ("baseline", BASELINE_USDMYR)
    ch = (fx / ref[1] - 1) * 100
    out.append({"id": "myr", "name": "USD/MYR", "score": clamp(ch, -1, 1), "reading": f"{fx:.4f} ({ch:+.2f}% vs {ref[0]})"})

    geo_map = {"calm": -0.1, "moderate": 0.1, "elevated": 0.3, "crisis": 0.6}
    held = [s for s in cfg.supports if xau >= s]
    if held:
        dist = (xau / held[0] - 1) * 100
        ts = 0.3 if dist < 1.5 else 0.1
        if len(held) < len(cfg.supports):
            ts -= 0.2
        txt = f"{dist:.1f}% above ${held[0]:,.0f}"
    else:
        ts, txt = -0.6, "below all supports"
    out.append({"id": "geo", "name": "Geopolitics/support", "score": clamp(ts + geo_map.get(cfg.geo, 0.1), -1, 1),
                "reading": f"{cfg.geo} · {txt}"})
    return out


WEIGHTS = {"fed": 0.25, "cpi": 0.15, "dxy": 0.25, "myr": 0.15, "geo": 0.20}


def composite(ds: list[dict]) -> tuple[float, int]:
    s = sum(WEIGHTS[d["id"]] * d["score"] for d in ds)
    a = sum(WEIGHTS[d["id"]] * abs(d["score"]) for d in ds)
    s = clamp(s, -1, 1)
    agree = abs(s) / a if a else 0
    strength = clamp(abs(s) / 0.6, 0, 1)
    prob = round(55 + 23 * clamp(0.5 * strength + 0.5 * agree * strength ** 0.5, 0, 1))
    return s, prob


def regime(s: float, th: float) -> str:
    return "bullish" if s >= th else "bearish" if s <= -th else "neutral"


# --------------------------------------------------------------------------- notify
def send_telegram(cfg: Config, text: str) -> bool:
    if not (cfg.tg_token and cfg.tg_chat):
        return False
    r = post_json(f"https://api.telegram.org/bot{cfg.tg_token}/sendMessage",
                  {"chat_id": cfg.tg_chat, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True})
    if not r.get("ok"):
        raise RuntimeError(f"Telegram error: {r}")
    return True


def send_email(cfg: Config, subject: str, text: str) -> bool:
    if not (cfg.smtp_host and cfg.smtp_user and cfg.smtp_pass and cfg.email_to):
        return False
    msg = EmailMessage()
    msg["Subject"], msg["From"], msg["To"] = subject, cfg.email_from, cfg.email_to
    msg.set_content(strip_html(text))
    ctx = ssl.create_default_context()
    if cfg.smtp_port == 465:
        with smtplib.SMTP_SSL(cfg.smtp_host, cfg.smtp_port, context=ctx, timeout=20) as s:
            s.login(cfg.smtp_user, cfg.smtp_pass)
            s.send_message(msg)
    else:
        with smtplib.SMTP(cfg.smtp_host, cfg.smtp_port, timeout=20) as s:
            s.starttls(context=ctx)
            s.login(cfg.smtp_user, cfg.smtp_pass)
            s.send_message(msg)
    return True


def strip_html(t: str) -> str:
    for tag in ("<b>", "</b>", "<i>", "</i>", "<code>", "</code>"):
        t = t.replace(tag, "")
    return t


def notify(cfg: Config, subject: str, text: str, dry: bool = False) -> None:
    if dry:
        print(f"\n--- WOULD SEND: {subject} ---\n{strip_html(text)}\n")
        return
    sent = []
    for name, fn in (("telegram", lambda: send_telegram(cfg, text)), ("email", lambda: send_email(cfg, subject, text))):
        try:
            if fn():
                sent.append(name)
        except Exception as e:  # noqa: BLE001
            log(f"{name} send failed: {e}")
    log(f"alert '{subject}' → {', '.join(sent) or 'NO CHANNEL CONFIGURED'}")


# --------------------------------------------------------------------------- state
def load_state(cfg: Config) -> dict:
    try:
        return json.loads(cfg.state_file.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return {}


def save_state(cfg: Config, st: dict) -> None:
    for k in ("dxy_hist", "fx_hist"):
        h = st.get(k, {})
        for old in sorted(h)[:-60]:
            del h[old]
    cfg.state_file.write_text(json.dumps(st, indent=2), encoding="utf-8")


def log(msg: str) -> None:
    print(f"[{dt.datetime.now(MYT):%Y-%m-%d %H:%M:%S} MYT] {msg}", flush=True)


def rm(v: float, dp: int = 2) -> str:
    return f"RM {v:,.{dp}f}"


# --------------------------------------------------------------------------- main check
def check(cfg: Config, dry: bool = False) -> None:
    apply_fred(cfg)
    st = load_state(cfg)
    fx, dxy, fx_src = fetch_fx()
    xau, gold_src = fetch_gold(cfg)

    g10 = xau / OZ * fx * 10
    retail, buyback, j916 = g10 * (1 + cfg.retail / 100), g10 * (1 - cfg.buyback / 100), g10 * 0.916
    ds = drivers(cfg, xau, fx, dxy, st)
    s, prob = composite(ds)
    mid = g10 * (1 + s * SIGMA_WEEK * 0.8)
    lo, hi = mid * (1 - SIGMA_WEEK), mid * (1 + SIGMA_WEEK)
    reg = regime(s, cfg.threshold)
    price = retail if cfg.basis == "retail" else g10

    summary = (f"10g spot <b>{rm(g10)}</b> · 1g {rm(g10 / 10)}\n"
               f"XAU/USD ${xau:,.2f} · USD/MYR {fx:.4f}\n"
               f"Bar retail ~{rm(retail)} · 916 value {rm(j916)} · buyback ~{rm(buyback)}\n"
               f"7-day range {rm(lo, 0)} – {rm(hi, 0)} (mid {rm(mid, 0)})\n"
               f"Drivers {s:+.2f} ({reg}, {prob}% alignment)")
    driver_lines = "\n".join(f"• {d['name']}: {d['reading']} [{d['score']:+.1f}]" for d in ds)

    # 1) target
    armed = st.get("armed", True)
    if cfg.target > 0:
        if armed and price <= cfg.target:
            notify(cfg, f"Gold 10g below RM {cfg.target:,.0f}",
                   f"🎯 <b>Target reached</b>\n{'Bar retail est.' if cfg.basis == 'retail' else 'Spot'} {rm(price)} ≤ target {rm(cfg.target)}\n\n{summary}",
                   dry)
            armed = False
        elif not armed and price > cfg.target * 1.003:
            armed = True
    # 2) driver regime shift (skip on first ever run)
    prev_reg = st.get("regime")
    if prev_reg and reg != prev_reg and reg != "neutral":
        icon = "🟢" if reg == "bullish" else "🔴"
        meaning = ("Prices more likely to rise — waiting may cost more; DCA now."
                   if reg == "bullish" else "Prices more likely to soften — stage buys toward the lower band.")
        notify(cfg, f"Gold drivers turned {reg}",
               f"{icon} <b>Market drivers shifted {reg}</b> (was {prev_reg})\n{meaning}\n\n{summary}\n\n{driver_lines}", dry)
    # 3) sharp move
    last = st.get("last_g10")
    if last:
        mv = (g10 / last - 1) * 100
        if abs(mv) >= cfg.sharp:
            notify(cfg, f"Gold 10g moved {mv:+.1f}%",
                   f"{'📈' if mv > 0 else '📉'} <b>Sharp move {mv:+.2f}%</b> since last check ({rm(last)} → {rm(g10)})\n\n{summary}", dry)

    today = dt.datetime.now(MYT).date().isoformat()
    st.setdefault("fx_hist", {})[today] = fx
    if dxy:
        st.setdefault("dxy_hist", {})[today] = dxy
    st.update({"armed": armed, "regime": reg, "last_g10": g10, "last_check": dt.datetime.now(MYT).isoformat()})
    if not dry:
        save_state(cfg, st)
    log(f"10g {rm(g10)} | XAU ${xau:,.2f} ({gold_src}) | USDMYR {fx:.4f} ({fx_src}) | score {s:+.2f} {reg} | "
        f"target {rm(cfg.target) if cfg.target else '—'} {'armed' if armed else 'triggered'}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Emas 10g gold alert worker")
    ap.add_argument("--once", action="store_true", help="run one check and exit (cron mode)")
    ap.add_argument("--test", action="store_true", help="send a test message to all channels")
    ap.add_argument("--dry-run", action="store_true", help="print alerts instead of sending")
    a = ap.parse_args()
    cfg = Config()

    if a.test:
        notify(cfg, "Emas 10g test alert", "✅ <b>Emas 10g alerts are working.</b>\nYou'll get target, driver-shift and sharp-move alerts here.")
        return
    if not (cfg.tg_token and cfg.tg_chat) and not cfg.smtp_host and not a.dry_run:
        log("WARNING: no Telegram or email configured — alerts will only be logged.")

    if a.once or a.dry_run:
        try:
            check(cfg, dry=a.dry_run)
        except Exception as e:  # noqa: BLE001
            log(f"check failed: {e}")
            sys.exit(1)
        return

    log(f"Starting loop: every {cfg.interval:g} min. Ctrl+C to stop.")
    while True:
        try:
            check(Config())  # re-read .env each cycle so edits apply without restart
        except Exception as e:  # noqa: BLE001
            log(f"check failed: {e}")
        time.sleep(cfg.interval * 60)


if __name__ == "__main__":
    main()
