"""テスト: technical.py の信号機ロジック"""

import sys
from pathlib import Path

import pytest
import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))

from generators.analyzers.technical import (
    _sma_signal,
    _rsi_signal,
    _macd_signal,
    _bb_signal,
    _volume_signal,
)


class TestSmaSignal:
    def test_perfect_order_bullish(self):
        """完全パーフェクトオーダー（上昇）は🟢。"""
        assert _sma_signal(5900, 5800, 5700, 5500) == "🟢"

    def test_perfect_order_bearish(self):
        """完全逆パーフェクトオーダー（下落）は🔴。"""
        assert _sma_signal(5300, 5400, 5500, 5700) == "🔴"

    def test_mixed_order_neutral(self):
        """順序が混在しているは🟡。"""
        assert _sma_signal(5600, 5700, 5500, 5800) == "🟡"

    def test_none_values(self):
        """NoneはMSA計算不可で⚪。"""
        assert _sma_signal(5600, None, 5700, 5500) == "⚪"


class TestRsiSignal:
    def test_overbought(self):
        """RSI >= 70 は🔴（買われすぎ）。"""
        assert _rsi_signal(70) == "🔴"
        assert _rsi_signal(80) == "🔴"

    def test_oversold(self):
        """RSI <= 30 は🟢（売られすぎ → 逆張り買い）。"""
        assert _rsi_signal(30) == "🟢"
        assert _rsi_signal(20) == "🟢"

    def test_neutral(self):
        """RSI 31-69 は🟡。"""
        assert _rsi_signal(50) == "🟡"
        assert _rsi_signal(60) == "🟡"

    def test_none(self):
        """None は⚪。"""
        assert _rsi_signal(None) == "⚪"


class TestMacdSignal:
    def test_macd_above_signal(self):
        """MACD > シグナルは🟢。"""
        assert _macd_signal(10.0, 8.0) == "🟢"

    def test_macd_below_signal(self):
        """MACD < シグナルは🔴。"""
        assert _macd_signal(5.0, 8.0) == "🔴"

    def test_macd_equal(self):
        """MACD == シグナルは🟡。"""
        assert _macd_signal(8.0, 8.0) == "🟡"

    def test_none_values(self):
        """Noneは⚪。"""
        assert _macd_signal(None, 8.0) == "⚪"
        assert _macd_signal(8.0, None) == "⚪"


class TestBbSignal:
    def test_above_upper_band(self):
        """上バンド突破は🔴（過熱）。"""
        assert _bb_signal(5950, 5900, 5700, 5800) == "🔴"

    def test_below_lower_band(self):
        """下バンド割れは🟢（売られすぎ）。"""
        assert _bb_signal(5650, 5900, 5700, 5800) == "🟢"

    def test_within_bands(self):
        """バンド内は🟡。"""
        assert _bb_signal(5800, 5900, 5700, 5800) == "🟡"

    def test_none_values(self):
        """Noneは⚪。"""
        assert _bb_signal(5800, None, 5700, 5800) == "⚪"


class TestVolumeSignal:
    def test_high_volume(self):
        """出来高 1.5倍以上は🟢。"""
        assert _volume_signal(1500000, 1000000) == "🟢"

    def test_low_volume(self):
        """出来高 0.5倍以下は🟡。"""
        assert _volume_signal(400000, 1000000) == "🟡"

    def test_normal_volume(self):
        """通常出来高は🟡。"""
        assert _volume_signal(1100000, 1000000) == "🟡"

    def test_zero_ma(self):
        """移動平均0はゼロ除算を防ぐ。"""
        result = _volume_signal(1000000, 0)
        assert result == "⚪"

    def test_none_values(self):
        """Noneは⚪。"""
        assert _volume_signal(None, 1000000) == "⚪"
        assert _volume_signal(1000000, None) == "⚪"
