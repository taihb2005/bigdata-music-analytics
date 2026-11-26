from __future__ import annotations

import unittest

from stream.tools.find_active_users import latest_activity


class LatestActivityTests(unittest.TestCase):
    def test_skips_now_playing_and_reads_last_scrobble(self) -> None:
        payload = {"recenttracks": {"track": [
            {"name": "Live", "@attr": {"nowplaying": "true"}},
            {"name": "Done", "date": {"uts": "1800000000"}},
        ]}}
        self.assertEqual(latest_activity(payload), (1800000000, True))

    def test_single_track_object_and_empty_history(self) -> None:
        self.assertEqual(latest_activity({"recenttracks": {"track": {"date": {"uts": "5"}}}}), (5, False))
        self.assertEqual(latest_activity({"recenttracks": {"track": []}}), (None, False))
        self.assertEqual(latest_activity({}), (None, False))


if __name__ == "__main__":
    unittest.main()
