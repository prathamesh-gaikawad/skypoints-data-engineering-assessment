"""Reference ETL for the SkyPoints assessment; uses only the Python standard library."""
from __future__ import annotations

import argparse
import csv
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable


TIER_CODES = {"GLD", "SLV", "PLT"}


@dataclass
class Member:
    member_id: str
    source_member_id: str
    member_name: str
    country: str
    enrollment_date: str
    last_flight_date: str
    tier_code: str
    date_of_birth: str
    age: int | None
    stale_member: bool | None
    source_updated_at: str
    source_file: str


class RowError(ValueError):
    def __init__(self, reasons: list[str]):
        self.reasons = reasons
        super().__init__("; ".join(reasons))


def _date(value: str | None, fmt: str, field: str, required: bool = False) -> date | None:
    text = (value or "").strip()
    if text.upper() in {"", "NULL", "NONE", "N/A"}:
        if required:
            raise ValueError(f"{field} is required")
        return None
    if fmt == "%m%d%Y":
        text = text.zfill(8)
    try:
        return datetime.strptime(text, fmt).date()
    except ValueError as exc:
        raise ValueError(f"{field} has invalid date {value!r}; expected {fmt}") from exc


def _age(dob: date | None, as_of: date) -> int | None:
    if dob is None:
        return None
    return as_of.year - dob.year - ((as_of.month, as_of.day) < (dob.month, dob.day))


def normalize_row(row: dict[str, str], country: str, source_file: str,
                  loaded_at: datetime, as_of: date) -> Member:
    """Normalize one row. Country must come from a validated feed scope or row."""
    country = country.upper().strip()
    if country not in {"AUS", "IND", "USA"}:
        raise RowError([f"unsupported country {country!r}"])

    # The source field names and date encodings differ by feed.
    if country == "AUS":
        raw_id, name = row.get("Unique ID"), row.get("Member Name")
        dob, enrolled, flight = row.get("Date of Birth"), row.get("Date of Enrollment"), row.get("Date of Flight")
        dob_fmt = enrolled_fmt = flight_fmt = "%Y-%m-%d"
        tier = row.get("Tier Type")
    elif country == "IND":
        raw_id, name = row.get("ID"), row.get("Name")
        dob, enrolled, flight = row.get("DOB"), row.get("EnrollmentDate"), row.get("Flight Date")
        dob_fmt = enrolled_fmt = flight_fmt = "%m/%d/%Y"
        tier = row.get("TierCode")
    else:
        raw_id, name = row.get("ID"), row.get("Name")
        dob, enrolled, flight = None, row.get("EnrollmentDate"), row.get("FlightDate")
        dob_fmt = "%m%d%Y"
        enrolled_fmt = flight_fmt = "%m%d%Y"
        tier = row.get("TierCode")

    reasons: list[str] = []
    raw_id = (raw_id or "").strip()
    name = (name or "").strip()
    if not raw_id:
        reasons.append("member ID is required")
    if len(raw_id) > 18:
        reasons.append("member ID exceeds 18 characters")
    if not name:
        reasons.append("member name is required")
    if len(name) > 255:
        reasons.append("member name exceeds 255 characters")

    try:
        enrollment_date = _date(enrolled, enrolled_fmt, "enrollment date", required=True)
    except ValueError as exc:
        enrollment_date = None
        reasons.append(str(exc))
    try:
        flight_date = _date(flight, flight_fmt, "last flight date")
    except ValueError as exc:
        flight_date = None
        reasons.append(str(exc))
    try:
        birth_date = _date(dob, dob_fmt, "date of birth")
    except ValueError as exc:
        birth_date = None
        reasons.append(str(exc))

    tier = (tier or "").strip().upper()
    if tier and tier not in TIER_CODES:
        reasons.append(f"unknown tier code {tier!r}")
    if enrollment_date and enrollment_date > as_of:
        reasons.append("enrollment date is in the future")
    if birth_date and birth_date > as_of:
        reasons.append("date of birth is in the future")
    if flight_date and enrollment_date and flight_date < enrollment_date:
        reasons.append("last flight date precedes enrollment date")
    if reasons:
        raise RowError(reasons)

    age = _age(birth_date, as_of)
    stale = (as_of - flight_date).days > 90 if flight_date else None
    return Member(
        member_id=f"{country}:{raw_id}", source_member_id=raw_id, member_name=name,
        country=country, enrollment_date=enrollment_date.isoformat(),
        last_flight_date=flight_date.isoformat() if flight_date else "",
        tier_code=tier, date_of_birth=birth_date.isoformat() if birth_date else "",
        age=age, stale_member=stale, source_updated_at=loaded_at.isoformat(),
        source_file=source_file,
    )


FEEDS = (("AUS", "AUS(Sheet1).csv"), ("IND", "IND(in).csv"), ("USA", "USA(in).csv"))


def load_member_files(files: Iterable[tuple[str, str | Path]], as_of: date,
                      loaded_at: datetime | None = None) -> tuple[list[Member], list[dict[str, str]]]:
    loaded_at = loaded_at or datetime.now().astimezone()
    accepted: list[Member] = []
    rejected: list[dict[str, str]] = []
    seen: dict[str, str] = {}
    for country, file_name in files:
        path = Path(file_name)
        with path.open("r", newline="", encoding="utf-8-sig") as handle:
            for row_number, row in enumerate(csv.DictReader(handle), start=2):
                try:
                    item = normalize_row(row, country, path.name, loaded_at, as_of)
                    if item.member_id in seen:
                        raise RowError([f"duplicate member key also present at {seen[item.member_id]}"])
                    seen[item.member_id] = f"{path.name}:{row_number}"
                    accepted.append(item)
                except RowError as exc:
                    rejected.append({"source_file": path.name, "row_number": str(row_number),
                                     "source_member_id": (row.get("ID") or row.get("Unique ID") or "").strip(),
                                     "reasons": " | ".join(exc.reasons),
                                     "raw_row": json.dumps(row, ensure_ascii=False)})
    return accepted, rejected


def flatten_redemptions(payload: dict) -> list[dict[str, str | int]]:
    member_id = str(payload.get("member_id", "")).strip()
    feed_date = str(payload.get("feed_date", "")).strip()
    if not member_id or not feed_date:
        raise ValueError("redemption feed requires member_id and feed_date")
    _date(feed_date, "%Y%m%d", "feed_date", required=True)
    out = []
    for txn in payload.get("redemptions", []):
        txn_id = str(txn.get("txn_id", "")).strip()
        if not txn_id:
            raise ValueError("each redemption requires txn_id")
        txn_date = _date(str(txn.get("txn_date", "")), "%Y%m%d", "txn_date", required=True)
        miles = txn.get("miles_redeemed")
        if not isinstance(miles, int) or miles < 0:
            raise ValueError(f"invalid miles_redeemed for {txn_id}")
        out.append({"member_id": member_id, "feed_date": feed_date, "txn_id": txn_id,
                    "txn_date": txn_date.isoformat(), "partner": str(txn.get("partner", "")),
                    "miles_redeemed": miles, "status": str(txn.get("status", "")).upper()})
    return out


def _write_csv(path: Path, rows: list[dict], fields: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--aus", required=True)
    parser.add_argument("--ind", required=True)
    parser.add_argument("--usa", required=True)
    parser.add_argument("--redemptions", help="newline-delimited JSON records or a JSON array")
    parser.add_argument("--as-of", default=date.today().isoformat(), help="YYYY-MM-DD; set explicitly for reproducibility")
    parser.add_argument("--out", default="output")
    args = parser.parse_args()
    as_of = date.fromisoformat(args.as_of)
    loaded_at = datetime.now().astimezone()
    members, rejected = load_member_files((("AUS", args.aus), ("IND", args.ind), ("USA", args.usa)), as_of, loaded_at)
    output = Path(args.out)
    output.mkdir(parents=True, exist_ok=True)
    _write_csv(output / "members_normalized.csv", [asdict(x) for x in members], list(Member.__dataclass_fields__))
    _write_csv(output / "rejections.csv", rejected, ["source_file", "row_number", "source_member_id", "reasons", "raw_row"])
    if args.redemptions:
        text = Path(args.redemptions).read_text(encoding="utf-8")
        try:
            parsed = json.loads(text)
            payloads = parsed if isinstance(parsed, list) else [parsed]
        except json.JSONDecodeError:
            payloads = [json.loads(line) for line in text.splitlines() if line.strip()]
        txns = [txn for payload in payloads for txn in flatten_redemptions(payload)]
        _write_csv(output / "redemptions_flat.csv", txns,
                   ["member_id", "feed_date", "txn_id", "txn_date", "partner", "miles_redeemed", "status"])
    print(f"accepted={len(members)} rejected={len(rejected)} out={output.resolve()}")


if __name__ == "__main__":
    main()
