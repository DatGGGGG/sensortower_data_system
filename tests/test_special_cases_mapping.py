from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from lib.library_st_data_processing import adjust_ndjson_app_full_info, build_special_case_map


class SpecialCasesMappingTests(unittest.TestCase):
    def test_blank_multiplier_preserves_default_and_decimal_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            special_cases = root / "special_cases.csv"
            publisher_mapping = root / "publishers.csv"
            input_ndjson = root / "input.ndjson"
            output_ndjson = root / "output.ndjson"
            special_cases.write_text(
                "app_id,cleaned_publisher_name,revenue_multiplier\n"
                "first,,\n"
                "second,Special Publisher,1.25\n",
                encoding="utf-8",
            )
            publisher_mapping.write_text(
                "cleaned_publisher_name,publisher_id,publisher_name,revenue_multiplier\n"
                "Default Publisher,publisher-a,Publisher A,2\n",
                encoding="utf-8",
            )
            input_ndjson.write_text(
                "\n".join(
                    json.dumps(row)
                    for row in (
                        {"app_id": "first", "publisher_id": "publisher-a"},
                        {"app_id": "second", "publisher_id": "publisher-a"},
                    )
                )
                + "\n",
                encoding="utf-8",
            )

            adjust_ndjson_app_full_info(
                str(input_ndjson), str(output_ndjson), str(publisher_mapping), str(special_cases)
            )
            rows = [json.loads(line) for line in output_ndjson.read_text(encoding="utf-8").splitlines()]

            self.assertEqual(rows[0]["cleaned_publisher_name"], "")
            self.assertEqual(rows[0]["revenue_multiplier"], 2)
            self.assertEqual(rows[1]["cleaned_publisher_name"], "Special Publisher")
            self.assertEqual(rows[1]["revenue_multiplier"], 1.25)

    def test_special_case_map_rejects_invalid_multiplier_as_no_override(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "special_cases.csv"
            path.write_text(
                "app_id,cleaned_publisher_name,revenue_multiplier\napp,Publisher,not-a-number\n",
                encoding="utf-8",
            )
            self.assertIsNone(build_special_case_map(str(path))["app"]["revenue_multiplier"])


if __name__ == "__main__":
    unittest.main()
