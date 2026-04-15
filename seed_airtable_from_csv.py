#!/usr/bin/env python3
import argparse
import csv
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from sync_givingdata_airtable import AirtableClient, prune_empty_fields


DEFAULT_ORGANIZATIONS_CSV = "/Users/bbeisswenger/Downloads/[Data Sync] All Organizations All Time-2026-04-15-at-04-52-41.csv"
DEFAULT_REQUESTS_CSV = "/Users/bbeisswenger/Downloads/[Data Sync] All Requests All Time-2026-04-15-at-05-42-32.csv"
DEFAULT_PAYMENTS_CSV = "/Users/bbeisswenger/Downloads/[Sync] All Payments All time-2026-04-15-at-05-06-42.csv"
DEFAULT_SHAREPOINT_FOLDER = os.getenv("GD_SYNC_FOLDER", "")


def get_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def parse_money(value: str):
    if value is None:
        return None
    cleaned = value.replace("$", "").replace(",", "").strip()
    if not cleaned:
        return None
    amount = float(cleaned)
    return int(amount) if amount.is_integer() else amount


def build_record_index(records: List[Dict[str, object]], key_field: str) -> Dict[str, str]:
    index: Dict[str, str] = {}
    for record in records:
        record_id = record.get("id")
        fields = record.get("fields", {})
        if not record_id or not isinstance(fields, dict):
            continue
        key = fields.get(key_field)
        if key in (None, ""):
            continue
        index[str(key)] = str(record_id)
    return index


def newest_matching_csv(folder: Path, keywords: Tuple[str, ...]) -> Optional[Path]:
    candidates = []
    for path in folder.glob("*.csv"):
        lowered = path.name.lower()
        if all(keyword in lowered for keyword in keywords):
            candidates.append(path)
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def resolve_csv_paths(
    folder: Optional[str],
    organizations_csv: Optional[str],
    requests_csv: Optional[str],
    payments_csv: Optional[str],
) -> Tuple[Path, Path, Path]:
    if folder:
        folder_path = Path(folder).expanduser().resolve()
        if not folder_path.exists():
            raise SystemExit(f"Sync folder does not exist: {folder_path}")
        if not folder_path.is_dir():
            raise SystemExit(f"Sync folder is not a directory: {folder_path}")
        org_path = Path(organizations_csv).expanduser().resolve() if organizations_csv else newest_matching_csv(folder_path, ("organization",))
        request_path = Path(requests_csv).expanduser().resolve() if requests_csv else newest_matching_csv(folder_path, ("request",))
        payment_path = Path(payments_csv).expanduser().resolve() if payments_csv else newest_matching_csv(folder_path, ("payment",))
    else:
        org_path = Path(organizations_csv or DEFAULT_ORGANIZATIONS_CSV).expanduser().resolve()
        request_path = Path(requests_csv or DEFAULT_REQUESTS_CSV).expanduser().resolve()
        payment_path = Path(payments_csv or DEFAULT_PAYMENTS_CSV).expanduser().resolve()

    missing = [str(path) for path in (org_path, request_path, payment_path) if not path or not path.exists()]
    if missing:
        raise SystemExit("Missing CSV file(s): " + ", ".join(missing))

    return org_path, request_path, payment_path


def parse_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.reader(handle)
        rows = list(reader)
    if len(rows) < 3:
        return []
    header = rows[0]
    data_rows = rows[2:]
    parsed = []
    for row in data_rows:
        padded = row + [""] * (len(header) - len(row))
        parsed.append({header[index]: padded[index] for index in range(len(header))})
    return parsed


def parse_payment_rows(path: Path) -> List[Dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.reader(handle)
        rows = list(reader)
    if len(rows) < 3:
        return []
    data_rows = rows[2:]
    parsed = []
    for row in data_rows:
        padded = row + [""] * (7 - len(row))
        parsed.append(
            {
                "Request ID": padded[0],
                "Pmt ID": padded[1],
                "Organization Name": padded[2],
                "Payment Amount": padded[3],
                "Status Name": padded[4],
                "Scheduled Date": padded[5],
                "Payment Date": padded[6],
            }
        )
    return parsed


def normalize_org_rows(rows: List[Dict[str, str]]) -> List[Dict[str, object]]:
    normalized = []
    for row in rows:
        address = row.get("Full Address") or ", ".join(
            part for part in [row.get("Address1"), row.get("Address2"), row.get("City"), row.get("State"), row.get("Postal Code"), row.get("Country")] if part
        )
        normalized.append(
            {
                "GivingData Organization ID": row.get("ID"),
                "Name": row.get("Name"),
                "Legal Name": row.get("Legal Name"),
                "Tax ID or EIN": row.get("Tax ID"),
                "Website": row.get("Website"),
                "Primary Contact Full Name": row.get("Full Name"),
                "Primary Contact Email": row.get("Email"),
                "Mission": row.get("Mission"),
                "Primary Address": address or None,
                "Address Line 1": row.get("Address1"),
                "Address Line 2": row.get("Address2"),
                "City": row.get("City"),
                "State": row.get("State"),
                "Zip": row.get("Postal Code"),
                "Country": row.get("Country"),
                "Source JSON": json.dumps(row, ensure_ascii=True, separators=(",", ":")),
            }
        )
    return prune_empty_fields(normalized)


def normalize_request_rows(rows: List[Dict[str, str]], organization_links: Dict[str, str]) -> List[Dict[str, object]]:
    normalized = []
    for row in rows:
        organization_id = row.get("Organization ID")
        organization_link = organization_links.get(str(organization_id)) if organization_id else None
        normalized.append(
            {
                "GivingData Request ID": row.get("Request ID"),
                "Project": row.get("Project"),
                "Organization": row.get("Organization"),
                "Organization ID": [organization_link] if organization_link else None,
                "Status": row.get("Status"),
                "Disposition": row.get("Disposition Date"),
                "Grant Amount": parse_money(row.get("Grant Amount", "")),
                "Staff Person": row.get("Staff Person"),
                "Grant Fiscal Year": row.get("Grant Fiscal Year"),
                "Program Areas": row.get("Program Area"),
                "Big Ideas": row.get("Big Ideas"),
                "Description": row.get("Description"),
                "Source JSON": json.dumps(row, ensure_ascii=True, separators=(",", ":")),
            }
        )
    return prune_empty_fields(normalized)


def normalize_payment_rows(rows: List[Dict[str, str]], request_links: Dict[str, str]) -> List[Dict[str, object]]:
    normalized = []
    for row in rows:
        request_id = row.get("Request ID")
        request_link = request_links.get(str(request_id)) if request_id else None
        normalized.append(
            {
                "GivingData Payment ID": row.get("Pmt ID"),
                "Organization Name": row.get("Organization Name"),
                "Request GD ID": [request_link] if request_link else None,
                "Status": row.get("Status Name"),
                "Amount": parse_money(row.get("Payment Amount", "")),
                "Schedule Date": row.get("Scheduled Date"),
                "Payment Date": row.get("Payment Date"),
                "Source JSON": json.dumps(row, ensure_ascii=True, separators=(",", ":")),
            }
        )
    return prune_empty_fields(normalized)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed Airtable from existing GivingData CSV exports.")
    parser.add_argument("--folder", default=DEFAULT_SHAREPOINT_FOLDER, help="Folder containing GivingData CSV exports. If set, newest matching CSVs are picked automatically.")
    parser.add_argument("--organizations-csv", default="", help="Explicit Organizations CSV path. Overrides auto-discovery.")
    parser.add_argument("--requests-csv", default="", help="Explicit Requests CSV path. Overrides auto-discovery.")
    parser.add_argument("--payments-csv", default="", help="Explicit Payments CSV path. Overrides auto-discovery.")
    parser.add_argument("--organizations-only", action="store_true")
    parser.add_argument("--requests-only", action="store_true")
    parser.add_argument("--payments-only", action="store_true")
    parser.add_argument("--start-row", type=int, default=0, help="Zero-based start row for partial imports.")
    parser.add_argument("--max-rows", type=int, default=0, help="Maximum rows to import after start-row. 0 means all remaining rows.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    selected_modes = [args.organizations_only, args.requests_only, args.payments_only]
    if sum(1 for flag in selected_modes if flag) > 1:
        raise SystemExit("Choose only one of --organizations-only, --requests-only, or --payments-only.")

    airtable = AirtableClient(get_env("AIRTABLE_PAT"), get_env("AIRTABLE_BASE_ID"))
    row_slice = slice(args.start_row, None if args.max_rows == 0 else args.start_row + args.max_rows)
    organizations_csv, requests_csv, payments_csv = resolve_csv_paths(
        args.folder or None,
        args.organizations_csv or None,
        args.requests_csv or None,
        args.payments_csv or None,
    )

    seeded = {
        "folder": str(Path(args.folder).expanduser().resolve()) if args.folder else None,
        "organizations_csv": str(organizations_csv),
        "requests_csv": str(requests_csv),
        "payments_csv": str(payments_csv),
        "organizations_seeded": 0,
        "requests_seeded": 0,
        "payments_seeded": 0,
    }
    if not args.requests_only and not args.payments_only:
        org_rows = normalize_org_rows(parse_csv_rows(organizations_csv)[row_slice])
        airtable.upsert_records("Organizations", org_rows, "GivingData Organization ID")
        seeded["organizations_seeded"] = len(org_rows)

    if not args.organizations_only and not args.payments_only:
        organization_index = build_record_index(
            airtable.list_records("Organizations", fields=["GivingData Organization ID"]),
            "GivingData Organization ID",
        )
        request_rows = normalize_request_rows(parse_csv_rows(requests_csv)[row_slice], organization_index)
        airtable.upsert_records("Requests", request_rows, "GivingData Request ID")
        seeded["requests_seeded"] = len(request_rows)

    if not args.organizations_only and not args.requests_only:
        request_index = build_record_index(
            airtable.list_records("Requests", fields=["GivingData Request ID"]),
            "GivingData Request ID",
        )
        payment_rows = normalize_payment_rows(parse_payment_rows(payments_csv)[row_slice], request_index)
        airtable.upsert_records("Payments", payment_rows, "GivingData Payment ID")
        seeded["payments_seeded"] = len(payment_rows)

    print(json.dumps(seeded, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
