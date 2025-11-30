import argparse
import logging
import os
import tempfile
from datetime import datetime, date
from typing import Optional, Set, Dict, List, Tuple

import pandas as pd
import requests

# Import edgar with error handling
try:
    from edgar import set_identity, Company
    from edgar.sgml import FilingSGML

    HAS_EDGAR = True
except ImportError as e:
    HAS_EDGAR = False
    print(f"Warning: Could not import edgar library: {e}")

# Reduce noise from edgar package
if HAS_EDGAR:
    logging.getLogger("edgar").setLevel(logging.WARNING)

# Import our new parser
try:
    from parse_13f_table import parse_13f_table

    HAS_PARSER = True
except ImportError as e:
    HAS_PARSER = False
    print(f"Warning: Could not import parse_13f_table.py parser: {e}")


# ... existing utility functions (parse_date, quarter_to_period_end, etc.) ...
def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def quarter_to_period_end(yq: str) -> str:
    yq = yq.strip().upper()
    if not (len(yq) == 6 and yq[4] == "Q" and yq[5] in "1234"):
        raise ValueError(f"Invalid quarter token: {yq}. Use YYYYQn (e.g., 2013Q1)")
    y, q = yq[:4], yq[5]
    end = {"1": "03-31", "2": "06-30", "3": "09-30", "4": "12-31"}[q]
    return f"{y}-{end}"


def period_in_years(period: str, years: Set[str]) -> bool:
    return bool(period) and period[:4] in years


def period_in_quarters(period: str, quarter_periods: Set[str]) -> bool:
    return period in quarter_periods


def period_in_range(period: str, dfrom: Optional[date], dto: Optional[date]) -> bool:
    if not period:
        return False
    p = parse_date(period)
    if dfrom and p < dfrom:
        return False
    if dto and p > dto:
        return False
    return True


def pick_latest_for_period(filings_for_period: List):
    """Pick the final filing for a quarter (prefer HR/A amendments)"""
    filings_for_period = sorted(
        filings_for_period,
        key=lambda f: (
            (getattr(f, "filing_date", "") or ""),
            (getattr(f, "accession_number", "") or ""),
        ),
    )
    hra = [
        f
        for f in filings_for_period
        if (getattr(f, "form", "") or "").upper() in ("13F-HR/A", "13F-HR/A ")
    ]
    if hra:
        return hra[-1]
    hr = [
        f
        for f in filings_for_period
        if (getattr(f, "form", "") or "").upper().strip() == "13F-HR"
    ]
    return hr[-1] if hr else filings_for_period[-1]


def _sec_txt_url(cik: str, accession_number: str) -> str:
    cik_nolead = str(int(cik))
    acc = accession_number
    acc_nodash = acc.replace("-", "")
    return (
        f"https://www.sec.gov/Archives/edgar/data/{cik_nolead}/{acc_nodash}/{acc}.txt"
    )


def _fetch_submission_txt(filing, identity: str) -> str:
    url = _sec_txt_url(str(filing.cik), filing.accession_number)
    r = requests.get(url, headers={"User-Agent": identity}, timeout=60)
    r.raise_for_status()
    return r.content.decode("latin-1", errors="ignore")


def _extract_info_table_block(txt: str) -> str:
    t_lower = txt.lower()
    start_key = "form 13f information table"
    end_key = "grand total"
    i = t_lower.find(start_key)
    if i == -1:
        return txt
    j = t_lower.find(end_key, i)
    return txt[i : (j + len(end_key))] if j != -1 else txt[i:]


def _find_header_positions(lines: List[str]) -> Dict:
    """Locate header line and column positions"""
    header_idx = None
    for idx, ln in enumerate(lines):
        s = ln.strip().upper()
        if "NAME OF ISSUER" in s and "CUSIP" in s and "VALUE" in s:
            header_idx = idx
            break
    if header_idx is None:
        raise RuntimeError("Could not locate header line.")

    hdr = lines[header_idx]
    nxt = lines[header_idx + 1] if header_idx + 1 < len(lines) else ""

    def pos(hay, needle):
        p = hay.upper().find(needle)
        return p if p != -1 else None

    positions = {
        "name": pos(hdr, "NAME OF ISSUER"),
        "title": pos(hdr, "TITLE OF"),
        "cusip": pos(hdr, "CUSIP"),
        "value": pos(hdr, "VALUE"),
        "shares": pos(hdr, "SHRS OR PRN AMT") or pos(hdr, "SHRS OR"),
        "sh_prn": pos(hdr, "SH/"),
        "putcall": pos(hdr, "PUT/CALL"),
        "discr": pos(hdr, "INVESTMENT DISCRETION"),
        "mgrs": pos(hdr, "OTHER MANAGERS"),
    }

    va_block = pos(hdr, "VOTING AUTHORITY") or pos(nxt, "VOTING AUTHORITY")
    if va_block is not None:
        positions["v_sole"] = pos(nxt, "SOLE") or va_block
        positions["v_shared"] = pos(nxt, "SHARED") or (
            positions["v_sole"] + 10 if positions["v_sole"] is not None else None
        )
        positions["v_none"] = pos(nxt, "NONE") or (
            positions["v_shared"] + 10
            if positions.get("v_shared") is not None
            else None
        )

    return {"header_idx": header_idx, "pos": positions}


def _slice(line: str, start: Optional[int], end: Optional[int]) -> str:
    if start is None:
        return ""
    return (line[start:end] if end is not None else line[start:]).rstrip()


def _parse_fixed_width_table(block: str) -> pd.DataFrame:
    """Parse fixed-width 13F information table"""
    try:
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(block, "html.parser")
        tables = soup.find_all("table")

        for table in tables:
            table_text = table.get_text().lower()
            if "information table" in table_text or "name of issuer" in table_text:
                try:
                    import io

                    html_str = str(table)
                    dfs = pd.read_html(io.StringIO(html_str), flavor="html5lib")
                    if dfs and len(dfs) > 0:
                        df = dfs[0]
                        df.columns = [str(col).strip() for col in df.columns]
                        mask = df.astype(str).apply(
                            lambda x: x.str.contains(
                                "Name of Issuer|Title|CUSIP|Market Value",
                                case=False,
                                na=False,
                            ).any(),
                            axis=1,
                        )
                        if mask.any():
                            df = df[~mask]
                        df = df[
                            df.astype(str).apply(lambda x: x.str.strip()).any(axis=1)
                        ]
                        df = df[
                            ~df.astype(str)
                            .apply(lambda x: x.str.contains("^[-=_]+$", na=False))
                            .any(axis=1)
                        ]

                        if len(df) > 0 and len(df.columns) > 0:
                            return df
                except Exception:
                    pass
    except Exception:
        pass

    # Fallback to fixed-width parsing
    raw_lines = block.splitlines()
    lines = [
        ln.rstrip("\r\n")
        for ln in raw_lines
        if not set(ln.strip()) <= {"-", "=", "<", ">", "_"}
    ]

    info = _find_header_positions(lines)
    hdr_idx, pos = info["header_idx"], info["pos"]

    keys = [
        "name",
        "title",
        "cusip",
        "value",
        "shares",
        "sh_prn",
        "putcall",
        "discr",
        "mgrs",
        "v_sole",
        "v_shared",
        "v_none",
    ]
    ordered = sorted(
        [(k, pos[k]) for k in keys if pos.get(k) is not None], key=lambda x: x[1]
    )
    spans = []
    for i, (k, start) in enumerate(ordered):
        end = ordered[i + 1][1] if i + 1 < len(ordered) else None
        spans.append((k, start, end))

    data_start = hdr_idx + 2
    rows = []
    for ln in lines[data_start:]:
        if not ln.strip():
            continue
        u = ln.upper()
        if "GRAND TOTAL" in u:
            break
        row = {k: _slice(ln, start, end).strip() for (k, start, end) in spans}
        if not row.get("cusip") and not row.get("value"):
            continue
        rows.append(row)

    df = pd.DataFrame(rows)

    def to_int(x):
        try:
            return int(str(x).replace(",", "").strip())
        except Exception:
            return pd.NA

    if "value" in df.columns:
        df["value_x1000"] = df["value"].apply(to_int)
        df.drop(columns=["value"], inplace=True)
    if "shares" in df.columns:
        df["shares"] = df["shares"].apply(to_int)
    for col in ("v_sole", "v_shared", "v_none"):
        if col in df.columns:
            df[col] = df[col].apply(to_int)

    rename = {
        "name": "name",
        "title": "title",
        "cusip": "cusip",
        "sh_prn": "share_unit",
        "putcall": "put_call",
        "discr": "discretion",
        "mgrs": "other_managers",
        "v_sole": "voting_sole",
        "v_shared": "voting_shared",
        "v_none": "voting_none",
    }
    df = df.rename(columns=rename)
    ordered_cols = [
        c
        for c in (
            "name",
            "title",
            "cusip",
            "value_x1000",
            "shares",
            "share_unit",
            "put_call",
            "discretion",
            "other_managers",
            "voting_sole",
            "voting_shared",
            "voting_none",
        )
        if c in df.columns
    ]
    return df[ordered_cols] if ordered_cols else df


def read_infotable_df(filing, identity: str) -> pd.DataFrame:
    """Read 13F information table via multiple methods"""
    # 1) XBRL/XML object
    try:
        obj = filing.obj()
        df = obj.infotable
        if df is not None and not df.empty:
            print(f"   [OK] Loaded via XBRL/XML object")
            return df
    except Exception:
        pass

    # 2) SGML -> XML
    try:
        sgml = FilingSGML.from_filing(filing)
        xml = sgml.xml()
        if xml is not None:
            for xp in (
                ".//informationTable/infoTable",
                ".//infoTable",
                ".//informationTable/informationTable/*",
            ):
                try:
                    df = pd.read_xml(xml, xpath=xp)
                    if df is not None and not df.empty:
                        print(f"   [OK] Loaded via SGML/XML")
                        return df
                except Exception:
                    continue
    except Exception:
        pass

    # 3) TXT fallback
    print(f"   [INFO] Falling back to TXT parsing")
    txt = _fetch_submission_txt(filing, identity)

    # Try new parser if available
    if HAS_PARSER:
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".txt", delete=False, encoding="latin-1"
            ) as tmp:
                tmp.write(txt)
                tmp_path = tmp.name

            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".csv", delete=False
            ) as out_tmp:
                out_path = out_tmp.name

            try:
                parse_13f_table(tmp_path, out_path)
                df = pd.read_csv(out_path)
                os.unlink(tmp_path)
                os.unlink(out_path)

                if df is not None and not df.empty:
                    print(f"   [OK] Loaded via parse_13f_table ({len(df)} rows)")
                    col_map = {
                        "Name of Issuer": "name",
                        "Title of Class": "title",
                        "CUSIP": "cusip",
                        "Value (thousands)": "value_x1000",
                        "Shares": "shares",
                        "Investment Discretion": "discretion",
                        "Put/Call": "put_call",
                        "Other Managers": "other_managers",
                        "Voting Sole": "voting_sole",
                        "Voting Shared": "voting_shared",
                        "Voting None": "voting_none",
                    }
                    df = df.rename(
                        columns={k: v for k, v in col_map.items() if k in df.columns}
                    )
                    return df
            except Exception as e:
                print(f"   [WARN] New parser failed, trying old parser: {e}")
                try:
                    os.unlink(tmp_path)
                    os.unlink(out_path)
                except:
                    pass
        except Exception as e:
            print(f"   [WARN] New parser failed: {e}")

    # 4) Old fixed-width parser
    block = _extract_info_table_block(txt)
    df = _parse_fixed_width_table(block)
    if df is None or df.empty:
        raise RuntimeError("TXT fallback produced no rows")
    print(f"   [OK] Loaded via fixed-width parser ({len(df)} rows)")
    return df


def main():
    ap = argparse.ArgumentParser(
        description="Download & merge SEC 13F filings by years / date range / quarters."
    )
    ap.add_argument("--company", required=True, help="CIK / ticker / company name")
    ap.add_argument(
        "--identity",
        required=True,
        help='SEC identity string, e.g. "Your Name your@email"',
    )
    ap.add_argument("--years", nargs="*", help="Specific years like: 2013 2023")
    ap.add_argument("--from", dest="date_from", help="From date YYYY-MM-DD")
    ap.add_argument("--to", dest="date_to", help="To date YYYY-MM-DD")
    ap.add_argument(
        "--quarters", nargs="*", help="Specific quarters (YYYYQn), e.g., 2013Q1 2023Q3"
    )
    ap.add_argument("--outdir", default="13f_outputs", help="Output directory")
    ap.add_argument(
        "--per-year-combined",
        action="store_true",
        help="Also write combined CSV per year",
    )
    ap.add_argument(
        "--master-combined", action="store_true", help="Also write a single master CSV"
    )
    args = ap.parse_args()

    if not HAS_EDGAR:
        print("[ERROR] edgar library required but not installed.")
        return

    set_identity(args.identity)
    os.makedirs(args.outdir, exist_ok=True)

    years_set: Optional[Set[str]] = set(args.years) if args.years else None
    dfrom: Optional[date] = parse_date(args.date_from) if args.date_from else None
    dto: Optional[date] = parse_date(args.date_to) if args.date_to else None
    quarters_set: Optional[Set[str]] = (
        {quarter_to_period_end(q) for q in args.quarters} if args.quarters else None
    )

    if not any([years_set, (dfrom or dto), quarters_set]):
        ap.error("Provide at least one filter: --years or --from/--to or --quarters")

    company = Company(args.company)
    filings = list(company.get_filings(form="13F-HR")) + list(
        company.get_filings(form="13F-HR/A")
    )

    buckets: Dict[str, List] = {}
    for f in filings:
        period = getattr(f, "period_of_report", None)
        if not period:
            continue

        ok = True
        if years_set and not period_in_years(period, years_set):
            ok = False
        if quarters_set and not period_in_quarters(period, quarters_set):
            ok = False
        if (dfrom or dto) and not period_in_range(period, dfrom, dto):
            ok = False

        if ok:
            buckets.setdefault(period, []).append(f)

    if not buckets:
        print("No filings matched the given filters.")
        return

    all_saved: List[pd.DataFrame] = []
    per_year_accumulator: Dict[str, List[pd.DataFrame]] = {}

    successful_periods: List[str] = []
    failed_periods: List[Tuple[str, str]] = []

    for period in sorted(buckets.keys()):
        print(f"Processing {period} ...")
        try:
            best_filing = pick_latest_for_period(buckets[period])
            df = read_infotable_df(best_filing, identity=args.identity)

            rename_map = {
                "nameOfIssuer": "name",
                "titleOfClass": "title",
                "cusip": "cusip",
                "value": "value_x1000",
                "sshPrnamt": "shares",
                "sshPrnamtType": "share_type",
                "investmentDiscretion": "discretion",
                "putCall": "put_call",
                "votingAuthoritySole": "voting_sole",
                "votingAuthorityShared": "voting_shared",
                "votingAuthorityNone": "voting_none",
            }
            df = df.rename(
                columns={k: v for k, v in rename_map.items() if k in df.columns}
            )
            if "value_x1000" in df.columns and df["value_x1000"].dtype == object:
                df["value_x1000"] = pd.to_numeric(df["value_x1000"], errors="coerce")

            df.insert(0, "period_of_report", period)

            company_name = (
                getattr(best_filing, "company", args.company)
                .replace(" ", "")
                .replace("&", "")
                .lower()
            )
            if len(company_name) > 15:
                company_name = company_name[:15]

            period_date = period.replace("-", "")
            period_obj = parse_date(period)
            month = period_obj.month
            quarter = (month - 1) // 3 + 1
            year = period_obj.year
            quarter_str = f"Q{quarter}{year}"

            filename = f"{company_name}_{quarter_str}_{period_date}"
            out_path = os.path.join(args.outdir, f"{filename}.csv")

            df.to_csv(out_path, index=False)
            print(f"[OK] Saved {out_path} ({len(df)} holdings)")

            successful_periods.append(period)
            all_saved.append(df)

            if args.per_year_combined:
                per_year_accumulator.setdefault(period[:4], []).append(df)

        except Exception as e:
            print(f"[WARN] Skipping {period}: {e}")
            failed_periods.append((period, str(e)))

            try:
                failed_dir = os.path.join(args.outdir, "failed")
                os.makedirs(failed_dir, exist_ok=True)
                txt = _fetch_submission_txt(best_filing, identity=args.identity)
                failed_filename = f"{args.company}_{period}.txt".replace("/", "-")
                failed_path = os.path.join(failed_dir, failed_filename)
                with open(failed_path, "w", encoding="latin-1", errors="ignore") as f:
                    f.write(txt)
                print(f"[DEBUG] Saved failed filing to: {failed_path}")
            except Exception as save_error:
                print(f"[DEBUG] Could not save failed filing: {save_error}")

    if args.per_year_combined and per_year_accumulator:
        for yr, dfs in per_year_accumulator.items():
            if not dfs:
                continue
            year_df = pd.concat(dfs, ignore_index=True)
            out_year = os.path.join(
                args.outdir, f"{args.company}_{yr}.csv".replace("/", "-")
            )
            year_df.to_csv(out_year, index=False)
            print(f"[INFO] Year combined saved: {out_year} ({len(year_df)} rows)")

    if args.master_combined and all_saved:
        master_df = pd.concat(all_saved, ignore_index=True)
        out_master = os.path.join(
            args.outdir, f"{args.company}_MASTER.csv".replace("/", "-")
        )
        master_df.to_csv(out_master, index=False)
        print(f"[INFO] Master combined saved: {out_master} ({len(master_df)} rows)")

    report_lines = []
    report_lines.append("=" * 80)
    report_lines.append("13F FILINGS PROCESSING REPORT")
    report_lines.append("=" * 80)
    report_lines.append("")
    report_lines.append(f"Company: {args.company}")
    report_lines.append(
        f"Total periods processed: {len(successful_periods) + len(failed_periods)}"
    )
    report_lines.append(f"Successful: {len(successful_periods)} quarterly filings")
    report_lines.append(f"Failed: {len(failed_periods)} quarterly filings")
    report_lines.append("")

    if successful_periods:
        report_lines.append("SUCCESSFUL PERIODS:")
        report_lines.append("-" * 80)
        for period in sorted(successful_periods):
            filename = f"{args.company}_{period}.csv".replace("/", "-")
            report_lines.append(f"  [OK] {period}")
        report_lines.append("")

    if failed_periods:
        report_lines.append("FAILED PERIODS:")
        report_lines.append("-" * 80)
        for period, reason in sorted(failed_periods):
            report_lines.append(f"  [FAIL] {period}: {reason}")
            failed_filename = f"{args.company}_{period}.txt".replace("/", "-")
            failed_path = os.path.join(args.outdir, "failed", failed_filename)
            if os.path.exists(failed_path):
                report_lines.append(f"         Saved to: failed/{failed_filename}")
        report_lines.append("")

    report_lines.append("=" * 80)
    report_text = "\n".join(report_lines)

    print("\n" + report_text)

    report_file = os.path.join(args.outdir, f"{args.company}_REPORT.txt")
    with open(report_file, "w", encoding="utf-8") as f:
        f.write(report_text)
    print(f"\nReport saved to: {report_file}")

    print("Done.")


if __name__ == "__main__":
    main()
