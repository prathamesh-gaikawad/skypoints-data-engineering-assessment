import sys
import unittest
import os
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from skypoints_pipeline import RowError, flatten_redemptions, load_member_files, normalize_row


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.loaded = datetime(2026, 11, 24, 12, 0)
        self.as_of = date(2022, 9, 1)

    def test_australian_iso_dates_and_missing_dob(self):
        row = {"Unique ID": "1", "Member Name": "Mike", "Tier Type": "PLT",
               "Date of Birth": "NULL", "Date of Enrollment": "2022-05-11", "Date of Flight": "2022-08-01"}
        member = normalize_row(row, "AUS", "aus.csv", self.loaded, self.as_of)
        self.assertEqual(member.member_id, "AUS:1")
        self.assertIsNone(member.age)
        self.assertFalse(member.stale_member)

    def test_indian_month_day_year_and_age(self):
        row = {"ID": "1", "Name": "Vikas", "DOB": "12/1/1998", "TierCode": "SLV",
               "EnrollmentDate": "1/1/2022", "Flight Date": "6/15/2022"}
        member = normalize_row(row, "IND", "ind.csv", self.loaded, date(2022, 10, 1))
        self.assertEqual(member.date_of_birth, "1998-12-01")
        self.assertEqual(member.age, 23)
        self.assertTrue(member.stale_member)

    def test_unpadded_usa_date(self):
        row = {"ID": "1", "Name": "Sam", "TierCode": "PLT", "EnrollmentDate": "6152022", "FlightDate": "8202022"}
        member = normalize_row(row, "USA", "usa.csv", self.loaded, self.as_of)
        self.assertEqual(member.enrollment_date, "2022-06-15")
        self.assertEqual(member.last_flight_date, "2022-08-20")

    def test_impossible_date_is_quarantinable(self):
        row = {"Unique ID": "2", "Member Name": "Jonathan", "Tier Type": "GLD",
               "Date of Birth": "1997-12-13", "Date of Enrollment": "2021-13-13", "Date of Flight": "2022-01-05"}
        with self.assertRaises(RowError):
            normalize_row(row, "AUS", "aus.csv", self.loaded, self.as_of)

    def test_real_feeds_quarantine_bad_row_and_keep_ids_scoped(self):
        files = [
            ("AUS", Path(os.environ.get("SKYPOINTS_AUS_CSV", ""))),
            ("IND", Path(os.environ.get("SKYPOINTS_IND_CSV", ""))),
            ("USA", Path(os.environ.get("SKYPOINTS_USA_CSV", ""))),
        ]
        if not all(str(path) and path.exists() for _, path in files):
            self.skipTest("set SKYPOINTS_AUS_CSV, SKYPOINTS_IND_CSV, and SKYPOINTS_USA_CSV to run the feed integration check")
        members, rejected = load_member_files(files, date(2026, 11, 24), self.loaded)
        self.assertEqual(len(members), 8)
        self.assertEqual(len(rejected), 1)
        self.assertEqual({m.member_id for m in members if m.source_member_id == "1"}, {"AUS:1", "IND:1", "USA:1"})

    def test_json_redemptions_flatten(self):
        rows = flatten_redemptions({"member_id": "223457", "feed_date": "20240115", "redemptions": [
            {"txn_id": "RX10091", "txn_date": "20240110", "partner": "AeroLink", "miles_redeemed": 12000, "status": "COMPLETED"},
            {"txn_id": "RX10092", "txn_date": "20240113", "partner": "SkyPoints", "miles_redeemed": 5000, "status": "PENDING"}]})
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["txn_date"], "2024-01-10")
        self.assertEqual(rows[1]["miles_redeemed"], 5000)


if __name__ == "__main__":
    unittest.main()
