#!/usr/bin/env python3
"""Builds cash_symbols.txt for SignalDesk: every Nifty 500 share that is NOT in NSE's current F&O list.

Run it on a PC that can reach nseindia.com (NSE blocks most servers), then put cash_symbols.txt next to signal_dashboard.py
(the Dockerfile copies it when it is there) and restart. Without this file the app uses the list built into signal_dashboard.py.

    python make_cash_list.py            # downloads both lists from NSE
    python make_cash_list.py nifty500.csv fo_mktlot.csv   # or use files you saved yourself
"""
import csv
import io
import sys

NIFTY500 = "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv"
FO_LOTS = "https://nsearchives.nseindia.com/content/fo/fo_mktlot.csv"
SECTOR = {   # NSE "Industry" -> the sector names the dashboard uses
    "Financial Services": "Financial Services", "Healthcare": "Pharma & Healthcare", "Fast Moving Consumer Goods": "FMCG",
    "Capital Goods": "Capital Goods & Defence", "Automobile and Auto Components": "Auto", "Information Technology": "IT",
    "Construction Materials": "Cement & Building", "Chemicals": "Chemicals & Paints", "Realty": "Realty", "Metals & Mining": "Metals & Mining",
    "Oil Gas & Consumable Fuels": "Oil, Gas & Energy", "Power": "Power", "Services": "Infra & Logistics", "Consumer Durables": "Consumer Durables",
    "Consumer Services": "Retail & Consumer", "Construction": "Infra & Logistics", "Telecommunication": "Telecom", "Textiles": "Textiles",
    "Media Entertainment & Publication": "Retail & Consumer", "Forest Materials": "Paper & Packaging", "Diversified": "Diversified",
}


def fo_symbols(text):
    rows = list(csv.reader(io.StringIO(text)))
    head = next(i for i, r in enumerate(rows) if any(c.strip().upper() == "SYMBOL" for c in r))
    col = [c.strip().upper() for c in rows[head]].index("SYMBOL")
    return {r[col].strip().upper() for r in rows[head + 1:] if len(r) > col and r[col].strip()}


def build(nifty_text, fo_text):
    fo = fo_symbols(fo_text)
    by = {}
    for r in csv.DictReader(io.StringIO(nifty_text)):
        sym = (r.get("Symbol") or "").strip().upper()
        if sym and sym not in fo and (r.get("Series") or "EQ").strip().upper() == "EQ":
            by.setdefault(SECTOR.get((r.get("Industry") or "").strip(), "Others"), []).append(sym)
    return "\n".join(f"{sec}: {' '.join(sorted(v))}" for sec, v in sorted(by.items())) + "\n", sum(len(v) for v in by.values())


def main(argv):
    if len(argv) == 3:
        n, f = open(argv[1]).read(), open(argv[2]).read()
    else:
        import requests
        s = requests.Session()
        s.headers["User-Agent"] = "Mozilla/5.0"
        s.get("https://www.nseindia.com", timeout=15)
        n, f = (s.get(u, timeout=30).text for u in (NIFTY500, FO_LOTS))
    text, count = build(n, f)
    with open("cash_symbols.txt", "w") as out:
        out.write(text)
    print(f"cash_symbols.txt written: {count} cash stocks (Nifty 500 minus F&O).")


if __name__ == "__main__":
    main(sys.argv)
