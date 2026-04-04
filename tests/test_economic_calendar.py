"""テスト: economic_calendar.py"""

import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from generators.collectors.economic_calendar import load_upcoming_events

CALENDAR_PATH = str(Path(__file__).parent.parent / "generators" / "config" / "economic_events.yaml")


class TestLoadUpcomingEvents:
    def test_returns_list(self):
        """戻り値がリストであること。"""
        events = load_upcoming_events(CALENDAR_PATH, today=date(2026, 4, 4))
        assert isinstance(events, list)

    def test_events_within_range(self):
        """全イベントが今日から14日以内であること。"""
        today = date(2026, 4, 4)
        events = load_upcoming_events(CALENDAR_PATH, today=today)
        for ev in events:
            assert 0 <= ev["days_until"] <= 14

    def test_event_fields(self):
        """必須フィールドが全て存在すること。"""
        today = date(2026, 4, 4)
        events = load_upcoming_events(CALENDAR_PATH, today=today)
        if events:
            ev = events[0]
            assert "date" in ev
            assert "event" in ev
            assert "country" in ev
            assert "importance" in ev
            assert "date_str" in ev
            assert "weekday" in ev
            assert "days_until" in ev

    def test_sorted_by_date(self):
        """日付順にソートされていること。"""
        today = date(2026, 4, 4)
        events = load_upcoming_events(CALENDAR_PATH, today=today)
        dates = [ev["date"] for ev in events]
        assert dates == sorted(dates)

    def test_max_events_respected(self):
        """display_max_events 件を超えないこと。"""
        today = date(2026, 4, 4)
        events = load_upcoming_events(CALENDAR_PATH, today=today)
        assert len(events) <= 5

    def test_past_events_excluded(self):
        """過去のイベントは含まれないこと。"""
        # 4/30 以降を基準日にすると 4/7-4/29 のイベントは除外される
        today = date(2026, 5, 1)
        events = load_upcoming_events(CALENDAR_PATH, today=today)
        for ev in events:
            assert ev["days_until"] >= 0

    def test_nonexistent_path_returns_empty(self):
        """存在しないパスは空リストを返すこと。"""
        events = load_upcoming_events("/nonexistent/path.yaml", today=date(2026, 4, 4))
        assert events == []
