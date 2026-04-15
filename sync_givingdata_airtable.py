#!/usr/bin/env python3
import argparse
import json
import math
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple


DEFAULT_TABLE_CONFIG = {
    "Organizations": {
        "primaryFieldName": "GivingData ID",
        "fields": [
            {"name": "Name", "type": "singleLineText"},
            {"name": "Legal Name", "type": "singleLineText"},
            {"name": "AKA", "type": "singleLineText"},
            {"name": "Type", "type": "singleLineText"},
            {"name": "Vendor ID", "type": "singleLineText"},
            {"name": "Intacct Vendor ID", "type": "singleLineText"},
            {"name": "Tax ID", "type": "singleLineText"},
            {"name": "Website", "type": "url"},
            {"name": "Email", "type": "email"},
            {"name": "Office Phone", "type": "phoneNumber"},
            {"name": "Mission", "type": "multilineText"},
            {"name": "Notes", "type": "multilineText"},
            {"name": "Charity Status", "type": "singleLineText"},
            {"name": "Subsidiary Of", "type": "singleLineText"},
            {"name": "Year Founded", "type": "number", "options": {"precision": 0}},
            {"name": "Total Paid", "type": "currency", "options": {"precision": 2, "symbol": "$"}},
            {"name": "Approved Total", "type": "currency", "options": {"precision": 2, "symbol": "$"}},
            {"name": "Approved Count", "type": "number", "options": {"precision": 0}},
            {"name": "Last Payment Date", "type": "date", "options": {"dateFormat": {"name": "iso"}}},
            {"name": "Create Date", "type": "dateTime", "options": {"dateFormat": {"name": "iso"}, "timeFormat": {"name": "24hour"}, "timeZone": "utc"}},
            {"name": "Change Date", "type": "dateTime", "options": {"dateFormat": {"name": "iso"}, "timeFormat": {"name": "24hour"}, "timeZone": "utc"}},
            {"name": "Primary Address", "type": "multilineText"},
            {"name": "Custom Fields JSON", "type": "multilineText"},
            {"name": "Attribs JSON", "type": "multilineText"},
            {"name": "Source JSON", "type": "multilineText"},
        ],
    },
    "Requests": {
        "primaryFieldName": "Request ID",
        "fields": [
            {"name": "Project", "type": "singleLineText"},
            {"name": "Organization ID", "type": "singleLineText"},
            {"name": "Organization", "type": "singleLineText"},
            {"name": "Status", "type": "singleLineText"},
            {"name": "Disposition", "type": "singleLineText"},
            {"name": "Grant Amount", "type": "currency", "options": {"precision": 2, "symbol": "$"}},
            {"name": "Staff Person", "type": "singleLineText"},
            {"name": "Email", "type": "email"},
            {"name": "Grant Fiscal Year", "type": "number", "options": {"precision": 0}},
            {"name": "Grant Start", "type": "date", "options": {"dateFormat": {"name": "friendly"}}},
            {"name": "Grant End", "type": "date", "options": {"dateFormat": {"name": "friendly"}}},
            {"name": "Program Area", "type": "multilineText"},
            {"name": "Big Ideas", "type": "multilineText"},
            {"name": "Source JSON", "type": "multilineText"},
        ],
    },
    "Payments": {
        "primaryFieldName": "GivingData ID",
        "fields": [
            {"name": "Organization GD ID", "type": "singleLineText"},
            {"name": "Organization Name", "type": "singleLineText"},
            {"name": "Request GD ID", "type": "singleLineText"},
            {"name": "Request Title", "type": "singleLineText"},
            {"name": "Request Status", "type": "singleLineText"},
            {"name": "Status ID", "type": "singleLineText"},
            {"name": "Status Name", "type": "singleLineText"},
            {"name": "Payment Number", "type": "number", "options": {"precision": 0}},
            {"name": "Schedule Date", "type": "date", "options": {"dateFormat": {"name": "iso"}}},
            {"name": "Payment Date", "type": "date", "options": {"dateFormat": {"name": "iso"}}},
            {"name": "Amount", "type": "currency", "options": {"precision": 2, "symbol": "$"}},
            {"name": "Currency", "type": "singleLineText"},
            {"name": "Method Of Payment", "type": "singleLineText"},
            {"name": "General Ledger Account", "type": "singleLineText"},
            {"name": "Create Date", "type": "dateTime", "options": {"dateFormat": {"name": "iso"}, "timeFormat": {"name": "24hour"}, "timeZone": "utc"}},
            {"name": "Change Date", "type": "dateTime", "options": {"dateFormat": {"name": "iso"}, "timeFormat": {"name": "24hour"}, "timeZone": "utc"}},
            {"name": "Custom Fields JSON", "type": "multilineText"},
            {"name": "Attribs JSON", "type": "multilineText"},
            {"name": "Source JSON", "type": "multilineText"},
        ],
    },
}


def compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"))


def chunked(values: List[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    for index in range(0, len(values), size):
        yield values[index:index + size]


def normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def get_env(name: str, required: bool = True, default: Optional[str] = None) -> str:
    value = os.getenv(name, default)
    if required and not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value or ""


class JsonHttpClient:
    def __init__(self, default_headers: Optional[Dict[str, str]] = None, timeout: int = 60):
        self.default_headers = default_headers or {}
        self.timeout = timeout

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Optional[Dict[str, str]] = None,
        query: Optional[Dict[str, Any]] = None,
        body: Optional[Any] = None,
    ) -> Tuple[Any, Dict[str, str]]:
        if query:
            filtered = {k: v for k, v in query.items() if v is not None}
            url = f"{url}?{urllib.parse.urlencode(filtered, doseq=True)}"
        data = None
        final_headers = {"Accept": "application/json", **self.default_headers, **(headers or {})}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            final_headers["Content-Type"] = "application/json"
        request = urllib.request.Request(url, data=data, headers=final_headers, method=method.upper())
        for attempt in range(6):
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    raw = response.read()
                    response_headers = {k: v for k, v in response.headers.items()}
                    if not raw:
                        return None, response_headers
                    content_type = response.headers.get("Content-Type", "")
                    if "application/json" in content_type or "text/json" in content_type:
                        return json.loads(raw.decode("utf-8")), response_headers
                    return raw.decode("utf-8"), response_headers
            except urllib.error.HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="replace")
                if exc.code in (429, 500, 502, 503, 504) and attempt < 5:
                    retry_after = exc.headers.get("Retry-After")
                    if retry_after:
                        sleep_seconds = float(retry_after)
                    else:
                        sleep_seconds = min(8.0, math.pow(2, attempt) * 0.5)
                    time.sleep(sleep_seconds)
                    continue
                raise RuntimeError(f"{method.upper()} {url} failed: {exc.code} {detail}") from exc


class GivingDataClient:
    def __init__(self, base_url: str, api_key: str):
        cleaned_base_url = base_url.rstrip("/")
        self.base_url = f"{cleaned_base_url}/api"
        self.client = JsonHttpClient(default_headers={"X-Api-Key": api_key})

    def _get_paginated_array(self, path: str, params: Dict[str, Any], max_page_size: int) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        page_index = 1
        while True:
            payload, headers = self.client.request(
                "GET",
                f"{self.base_url}{path}",
                query={**params, "pageIndex": page_index, "pageSize": max_page_size},
            )
            if not isinstance(payload, list):
                raise RuntimeError(f"Expected list response from {path}, got {type(payload).__name__}")
            items.extend(payload)
            page_count = int(headers.get("X-Pagination-Count", "0") or 0)
            if page_count and page_index >= page_count:
                break
            if len(payload) < max_page_size:
                break
            page_index += 1
        return items

    def get_organizations(self) -> List[Dict[str, Any]]:
        return self._get_paginated_array("/public/organizations", {"orderBy": "+id"}, 1000)

    def get_payment_statuses(self) -> List[Dict[str, Any]]:
        payload, _headers = self.client.request("GET", f"{self.base_url}/public/payments/statuses")
        if isinstance(payload, dict):
            for key in ("statuses", "results", "items"):
                if isinstance(payload.get(key), list):
                    return payload[key]
        if not isinstance(payload, list):
            raise RuntimeError("Unexpected response from /public/payments/statuses")
        return payload

    def get_payments(self) -> List[Dict[str, Any]]:
        payments: List[Dict[str, Any]] = []
        seen_ids = set()
        for status in self.get_payment_statuses():
            status_id = status.get("id")
            if status_id is None:
                continue
            for payment in self._get_paginated_array(
                "/public/payments",
                {"statusId": status_id},
                100,
            ):
                payment_id = payment.get("id")
                if payment_id in seen_ids:
                    continue
                seen_ids.add(payment_id)
                payments.append(payment)
        return payments

    def get_requests(self) -> List[Dict[str, Any]]:
        payload, _headers = self.client.request(
            "GET",
            f"{self.base_url}/public/export/requests",
            query={"pageSize": -1, "pageIndex": 1},
        )
        return extract_dynamic_rows(payload)


class AirtableClient:
    def __init__(self, pat: str, base_id: str):
        self.base_id = base_id
        self.api_base = "https://api.airtable.com/v0"
        self.meta_base = "https://api.airtable.com/v0/meta"
        self.client = JsonHttpClient(default_headers={"Authorization": f"Bearer {pat}"}, timeout=120)

    def list_tables(self) -> List[Dict[str, Any]]:
        payload, _headers = self.client.request("GET", f"{self.meta_base}/bases/{self.base_id}/tables")
        return payload.get("tables", [])

    def list_records(self, table_name: str, fields: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        endpoint = f"{self.api_base}/{self.base_id}/{urllib.parse.quote(table_name)}"
        offset = None
        while True:
            query: Dict[str, Any] = {"pageSize": 100}
            if offset:
                query["offset"] = offset
            if fields:
                query["fields[]"] = fields
            payload, _headers = self.client.request("GET", endpoint, query=query)
            records.extend(payload.get("records", []))
            offset = payload.get("offset")
            if not offset:
                break
        return records

    def ensure_tables(self, table_config: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        existing = {table["name"]: table for table in self.list_tables()}
        for table_name, config in table_config.items():
            if table_name in existing:
                continue
            body = {
                "name": table_name,
                "description": f"Synced from GivingData {table_name.lower()}",
                "fields": [{"name": config["primaryFieldName"], "type": "singleLineText"}, *config["fields"]],
            }
            self.client.request("POST", f"{self.meta_base}/bases/{self.base_id}/tables", body=body)
        return {table["name"]: table for table in self.list_tables()}

    def upsert_records(self, table_name: str, records: List[Dict[str, Any]], merge_field: str) -> None:
        if not records:
            return
        endpoint = f"{self.api_base}/{self.base_id}/{urllib.parse.quote(table_name)}"
        for batch in chunked(records, 10):
            body = {
                "performUpsert": {"fieldsToMergeOn": [merge_field]},
                "records": [{"fields": record} for record in batch],
                "typecast": True,
            }
            self.client.request("PATCH", endpoint, body=body)


def extract_dynamic_rows(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if not isinstance(payload, dict):
        raise RuntimeError(f"Unexpected export payload type: {type(payload).__name__}")

    rows = None
    for key in ("results", "rows", "records", "items", "data"):
        if isinstance(payload.get(key), list):
            rows = payload[key]
            break
    if rows is None:
        raise RuntimeError("Could not find rows in GivingData export response. Expected one of results/rows/records/items/data.")

    if rows and isinstance(rows[0], dict):
        return rows

    columns = payload.get("columns") or payload.get("fields")
    if rows and isinstance(rows[0], list) and isinstance(columns, list):
        names = []
        for column in columns:
            if isinstance(column, dict):
                names.append(column.get("name") or column.get("field") or column.get("title") or "column")
            else:
                names.append(str(column))
        return [dict(zip(names, row)) for row in rows]

    raise RuntimeError("GivingData export rows were present but could not be normalized into objects.")


def lookup_alias(row: Dict[str, Any], *aliases: str) -> Any:
    normalized = {normalize_key(str(key)): value for key, value in row.items()}
    for alias in aliases:
        key = normalize_key(alias)
        if key in normalized:
            return normalized[key]
    return None


def iso_text(value: Any) -> Optional[str]:
    if value in (None, "", []):
        return None
    return str(value)


def number_or_none(value: Any) -> Optional[float]:
    if value in (None, "", []):
        return None
    if isinstance(value, (int, float)):
        return value
    cleaned = str(value).replace(",", "").replace("$", "").strip()
    if cleaned == "":
        return None
    try:
        number = float(cleaned)
    except ValueError:
        return None
    if number.is_integer():
        return int(number)
    return number


def render_primary_address(address: Optional[Dict[str, Any]]) -> Optional[str]:
    if not address:
        return None
    parts = [
        address.get("street1"),
        address.get("street2"),
        address.get("city"),
        address.get("state"),
        address.get("postalCode"),
        address.get("country"),
    ]
    rendered = ", ".join(str(part).strip() for part in parts if part)
    return rendered or None


def normalize_organization(record: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "GivingData ID": str(record.get("id")),
        "Name": record.get("name"),
        "Legal Name": record.get("legalName"),
        "AKA": record.get("aka"),
        "Type": record.get("type"),
        "Vendor ID": record.get("vendorId"),
        "Intacct Vendor ID": record.get("intacctVendorId"),
        "Tax ID": record.get("taxId"),
        "Website": record.get("website"),
        "Email": record.get("email"),
        "Office Phone": record.get("officePhoneNumber"),
        "Mission": record.get("mission"),
        "Notes": record.get("notes"),
        "Charity Status": record.get("charityStatus"),
        "Subsidiary Of": record.get("subsidiaryOf"),
        "Year Founded": number_or_none(record.get("yearFounded")),
        "Total Paid": number_or_none(record.get("totalPaid")),
        "Approved Total": number_or_none(record.get("approvedTotal")),
        "Approved Count": number_or_none(record.get("approvedCount")),
        "Last Payment Date": iso_text(record.get("lastPaymentDate")),
        "Create Date": iso_text(record.get("createDate")),
        "Change Date": iso_text(record.get("changeDate")),
        "Primary Address": render_primary_address(record.get("primaryAddress")),
        "Custom Fields JSON": compact_json(record.get("customFields") or []),
        "Attribs JSON": compact_json(record.get("attribs") or []),
        "Source JSON": compact_json(record),
    }


def normalize_request(row: Dict[str, Any]) -> Dict[str, Any]:
    request_id = lookup_alias(row, "id", "request id", "requestid")
    return {
        "Request ID": str(request_id),
        "Project": lookup_alias(row, "project", "project title", "projecttitle", "title"),
        "Organization ID": iso_text(lookup_alias(row, "organization id", "organizationid")),
        "Organization": lookup_alias(row, "organization", "organization name", "organizationname", "org"),
        "Status": lookup_alias(row, "status", "status name", "statusname", "request status"),
        "Disposition": lookup_alias(row, "disposition"),
        "Grant Amount": number_or_none(lookup_alias(row, "grant amount", "grantamount")),
        "Staff Person": lookup_alias(row, "staff person", "primary staff member", "primary staff", "primarystaffmember"),
        "Email": lookup_alias(row, "email"),
        "Grant Fiscal Year": number_or_none(lookup_alias(row, "grant fiscal year", "grantfiscalyear")),
        "Grant Start": iso_text(lookup_alias(row, "grant start", "project start date", "grantstart", "projectstartdate")),
        "Grant End": iso_text(lookup_alias(row, "grant end", "project end date", "grantend", "projectenddate")),
        "Program Area": lookup_alias(row, "program area", "programarea"),
        "Big Ideas": lookup_alias(row, "big ideas", "bigideas"),
        "Source JSON": compact_json(row),
    }


def normalize_payment(record: Dict[str, Any]) -> Dict[str, Any]:
    organization = record.get("organization") or {}
    request = record.get("request") or {}
    gl_account = record.get("generalLedgerAccount") or {}
    return {
        "GivingData ID": str(record.get("id")),
        "Organization GD ID": iso_text(organization.get("id")),
        "Organization Name": organization.get("name"),
        "Request GD ID": iso_text(request.get("id")),
        "Request Title": request.get("projectTitle"),
        "Request Status": request.get("statusName"),
        "Status ID": iso_text(record.get("statusId")),
        "Status Name": record.get("statusName"),
        "Payment Number": number_or_none(record.get("paymentNumber")),
        "Schedule Date": iso_text(record.get("scheduleDate")),
        "Payment Date": iso_text(record.get("paymentDate")),
        "Amount": number_or_none(record.get("amount")),
        "Currency": record.get("currency"),
        "Method Of Payment": record.get("methodOfPayment"),
        "General Ledger Account": gl_account.get("name"),
        "Create Date": iso_text(record.get("createDate")),
        "Change Date": iso_text(record.get("changeDate")),
        "Custom Fields JSON": compact_json(record.get("customFields") or []),
        "Attribs JSON": compact_json(record.get("attribs") or []),
        "Source JSON": compact_json(record),
    }


def prune_empty_fields(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    cleaned = []
    for record in records:
        cleaned.append({key: value for key, value in record.items() if value is not None})
    return cleaned


@dataclass
class SyncSummary:
    organizations: int = 0
    requests: int = 0
    payments: int = 0


def run_sync(sync_organizations: bool, sync_requests: bool, sync_payments: bool, schema_only: bool) -> SyncSummary:
    airtable = AirtableClient(
        pat=get_env("AIRTABLE_PAT"),
        base_id=get_env("AIRTABLE_BASE_ID"),
    )
    airtable.ensure_tables(DEFAULT_TABLE_CONFIG)

    summary = SyncSummary()
    if schema_only:
        return summary

    gd = GivingDataClient(
        base_url=get_env("GD_BASE_URL"),
        api_key=get_env("GD_API_KEY"),
    )

    if sync_organizations:
        organizations = prune_empty_fields([normalize_organization(row) for row in gd.get_organizations() if row.get("id") is not None])
        airtable.upsert_records("Organizations", organizations, "GivingData ID")
        summary.organizations = len(organizations)

    if sync_requests:
        requests = prune_empty_fields([normalize_request(row) for row in gd.get_requests() if lookup_alias(row, "id", "request id", "requestid") is not None])
        airtable.upsert_records("Requests", requests, "Request ID")
        summary.requests = len(requests)

    if sync_payments:
        payments = prune_empty_fields([normalize_payment(row) for row in gd.get_payments() if row.get("id") is not None])
        airtable.upsert_records("Payments", payments, "GivingData ID")
        summary.payments = len(payments)

    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync GivingData Organizations, Requests, and Payments into Airtable.")
    parser.add_argument("--schema-only", action="store_true", help="Only create the Airtable tables if they do not already exist.")
    parser.add_argument("--organizations-only", action="store_true", help="Sync only Organizations.")
    parser.add_argument("--requests-only", action="store_true", help="Sync only Requests.")
    parser.add_argument("--payments-only", action="store_true", help="Sync only Payments.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    selected = [args.organizations_only, args.requests_only, args.payments_only]
    if sum(1 for flag in selected if flag) > 1:
        raise SystemExit("Choose only one of --organizations-only, --requests-only, or --payments-only.")

    summary = run_sync(
        sync_organizations=not any(selected) or args.organizations_only,
        sync_requests=not any(selected) or args.requests_only,
        sync_payments=not any(selected) or args.payments_only,
        schema_only=args.schema_only,
    )
    print(
        json.dumps(
            {
                "schema_only": args.schema_only,
                "organizations_synced": summary.organizations,
                "requests_synced": summary.requests,
                "payments_synced": summary.payments,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
