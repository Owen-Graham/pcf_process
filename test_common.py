import unittest
from datetime import datetime
from common import get_tradingview_vix_contract_code, normalize_vix_ticker, MONTH_CODES

class TestCommon(unittest.TestCase):

    def test_normalize_vix_ticker(self):
        self.assertEqual(normalize_vix_ticker("VXM25"), "VXM5")
        self.assertEqual(normalize_vix_ticker("VXH00"), "VXH0") # Assuming year 2000, normalized to 0
        self.assertEqual(normalize_vix_ticker("VXF2025"), "VXF5")
        self.assertEqual(normalize_vix_ticker("VXM5"), "VXM5") # Already normalized
        with self.assertRaises(Exception): # Or specific InvalidDataError if defined and raised
            normalize_vix_ticker("")
        with self.assertRaises(Exception):
            normalize_vix_ticker(None)
        self.assertEqual(normalize_vix_ticker("VXU23"), "VXU3")

    def test_get_tradingview_vix_contract_code(self):
        # Test cases: (normalized_code, current_datetime, expected_tradingview_code)

        # --- Current Year: 2024 ---
        # Future month in current year's decade
        self.assertEqual(get_tradingview_vix_contract_code("VXM4", datetime(2024, 1, 1)), "VXM2024") # April 2024
        self.assertEqual(get_tradingview_vix_contract_code("VXZ4", datetime(2024, 1, 1)), "VXZ2024") # Dec 2024
        self.assertEqual(get_tradingview_vix_contract_code("VXF5", datetime(2024, 1, 1)), "VXF2025") # Jan 2025

        # Past month in current year's decade (should roll to next decade for that year digit)
        # Example: Current is Nov 2024.
        # VXM4 (April 2024) is in the past. If we ask for "VXM4" as a future, it means "VXM2034".
        self.assertEqual(get_tradingview_vix_contract_code("VXH4", datetime(2024, 11, 1)), "VXH2034") # March 2034 (H=March)
        self.assertEqual(get_tradingview_vix_contract_code("VXM4", datetime(2024, 11, 1)), "VXM2034") # June 2034 (M=June)
        # Current: Nov 2024. Contract: VX + F (Jan) + 4 -> VXF2034
        self.assertEqual(get_tradingview_vix_contract_code("VXF4", datetime(2024, 11, 1)), "VXF2034") # Jan 2034

        # Contract for current month and year
        self.assertEqual(get_tradingview_vix_contract_code("VXX4", datetime(2024, 11, 1)), "VXX2024") # Nov 2024 (X=Nov)

        # Contract for future month in current year
        self.assertEqual(get_tradingview_vix_contract_code("VXZ4", datetime(2024, 11, 1)), "VXZ2024") # Dec 2024 (Z=Dec)

        # Contract for future year
        self.assertEqual(get_tradingview_vix_contract_code("VXF5", datetime(2024, 1, 1)), "VXF2025") # Jan 2025
        self.assertEqual(get_tradingview_vix_contract_code("VXM5", datetime(2024, 1, 1)), "VXM2025") # June 2025

        # --- Decade Rollover ---
        # Current: Dec 2029
        self.assertEqual(get_tradingview_vix_contract_code("VXF0", datetime(2029, 12, 1)), "VXF2030") # Jan 2030
        self.assertEqual(get_tradingview_vix_contract_code("VXM0", datetime(2029, 12, 1)), "VXM2030") # June 2030
        # For "VXF9" in Dec 2029: Jan 2029 contract is past. The next F contract ending in 9 would be for Jan 2039.
        self.assertEqual(get_tradingview_vix_contract_code("VXF9", datetime(2029, 12, 1)), "VXF2039")


        # --- Current Year: 2025 ---
        self.assertEqual(get_tradingview_vix_contract_code("VXM5", datetime(2025, 1, 1)), "VXM2025") # June 2025
        self.assertEqual(get_tradingview_vix_contract_code("VXH5", datetime(2025, 4, 1)), "VXH2035") # March 2035 (H=March, past month in 2025)
        self.assertEqual(get_tradingview_vix_contract_code("VXF6", datetime(2025, 1, 1)), "VXF2026") # Jan 2026

        # --- Turn of the Century ---
        # Current: Dec 2099
        self.assertEqual(get_tradingview_vix_contract_code("VXF0", datetime(2099, 12, 1)), "VXF2100") # Jan 2100
        self.assertEqual(get_tradingview_vix_contract_code("VXM0", datetime(2099, 12, 1)), "VXM2100") # June 2100
        self.assertEqual(get_tradingview_vix_contract_code("VXH9", datetime(2099, 12, 1)), "VXH2109") # March 2109 (H=March, past month in 2099) -> This logic for past month seems to be what my function does.

        # --- Test current month behavior ---
        # Current: June 2024 (Month 6)
        self.assertEqual(get_tradingview_vix_contract_code("VXM4", datetime(2024, 6, 15)), "VXM2024") # June 2024
        self.assertEqual(get_tradingview_vix_contract_code("VXN4", datetime(2024, 6, 15)), "VXN2024") # July 2024
        # Test a contract month that has passed this year
        self.assertEqual(get_tradingview_vix_contract_code("VXK4", datetime(2024, 6, 15)), "VXK2034") # May 2034 (K=May, past month)
        # Test a contract month that is far in future this year
        self.assertEqual(get_tradingview_vix_contract_code("VXZ4", datetime(2024, 6, 15)), "VXZ2024") # Dec 2024

        # Invalid Inputs
        with self.assertRaises(ValueError):
            get_tradingview_vix_contract_code("VXM2025", datetime.now()) # Not normalized
        with self.assertRaises(ValueError):
            get_tradingview_vix_contract_code("VXMA", datetime.now()) # Invalid year digit
        with self.assertRaises(ValueError):
            get_tradingview_vix_contract_code("VX?5", datetime.now()) # Invalid month code
        with self.assertRaises(ValueError):
            get_tradingview_vix_contract_code("VXM", datetime.now()) # Too short
        with self.assertRaises(ValueError):
            get_tradingview_vix_contract_code("ABCDE", datetime.now()) # Incorrect format

if __name__ == '__main__':
    unittest.main()
