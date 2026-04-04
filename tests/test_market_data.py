"""テスト: market_data.py — 価格不連続点検出・除去ロジック"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from generators.collectors.market_data import _trim_price_discontinuity


def _make_df(close_values: list[float]) -> pd.DataFrame:
    """テスト用DataFrameを生成する。"""
    dates = pd.date_range("2025-10-01", periods=len(close_values), freq="B")
    return pd.DataFrame({"Close": close_values}, index=dates)


class TestTrimPriceDiscontinuity:
    def test_no_discontinuity_returns_original(self):
        """不連続なければ元のDataFrameをそのまま返す。"""
        close = [100.0 + i * 0.1 for i in range(60)]
        df = _make_df(close)
        result = _trim_price_discontinuity(df)
        assert len(result) == len(df)
        assert result is df

    def test_detects_large_drop(self):
        """50%超の急落を不連続と判定し、以降のデータを返す。"""
        # 最初の30日: ~3700円台、その後~380円に急落（1306.T 相当）
        close_old = [3700.0 + i for i in range(30)]
        close_new = [380.0 + i * 0.1 for i in range(30)]
        df = _make_df(close_old + close_new)
        result = _trim_price_discontinuity(df)

        # 切り詰め後は直近の連続データのみ
        assert len(result) < len(df)
        # 直近データの終値は~382円付近（380 + 29*0.1 = 382.9）
        assert result["Close"].iloc[-1] < 400.0

    def test_detects_large_spike(self):
        """50%超の急騰も不連続と判定する（逆方向の不連続）。"""
        close_old = [100.0 + i * 0.1 for i in range(30)]
        close_new = [1000.0 + i * 0.1 for i in range(30)]  # 10倍に跳ね上がり
        df = _make_df(close_old + close_new)
        result = _trim_price_discontinuity(df)

        assert len(result) < len(df)
        assert result["Close"].iloc[-1] > 900.0

    def test_uses_last_discontinuity_when_multiple(self):
        """複数の不連続点がある場合は最後の不連続点以降を使う。"""
        # 30日: 3700 → 30日: 380 → 30日: 40 と2回不連続
        close_a = [3700.0 + i for i in range(30)]
        close_b = [380.0 + i * 0.1 for i in range(30)]
        close_c = [40.0 + i * 0.01 for i in range(30)]
        df = _make_df(close_a + close_b + close_c)
        result = _trim_price_discontinuity(df)

        # 最後の不連続（380→40）以降だけ残る
        assert len(result) <= 31  # 30行 + 不連続点の1行
        assert result["Close"].iloc[-1] < 50.0

    def test_small_fluctuations_not_trimmed(self):
        """30%以内の変動は不連続と判定しない。"""
        # ±20% の範囲で変動するデータ
        close = [100.0, 120.0, 90.0, 110.0, 95.0] * 12  # 60行
        df = _make_df(close)
        result = _trim_price_discontinuity(df, threshold=0.50)
        # 最大変動は (90-120)/120 = -25% < 50%なので切り詰めなし
        assert len(result) == len(df)

    def test_short_data_returned_unchanged(self):
        """10行未満のデータは検査せずそのまま返す。"""
        df = _make_df([100.0, 10.0])  # 90%下落でも行数が少なければそのまま
        result = _trim_price_discontinuity(df)
        assert len(result) == len(df)

    def test_scales_when_post_jump_data_insufficient(self):
        """不連続点後のデータが10行未満のとき、全データをスケール調整して返す。"""
        # 不連続点が終端近くにある場合（スプリット直後）: 後に8行しか残らない
        close_old = [3700.0 + i for i in range(60)]
        close_new = [380.0 + i * 0.1 for i in range(8)]  # 8行 < 10行
        df = _make_df(close_old + close_new)
        result = _trim_price_discontinuity(df)

        # スケール調整後は元と同じ行数
        assert len(result) == len(df)
        # 全データが新スケール（~380円台）に正規化される
        # 旧スケール最終値 3759 × (380/3759) ≈ 38.4 ではなく
        # 旧スケール（3700...3759）× ratio（380/3759）≈ 374...376 に調整
        assert result["Close"].max() < 500.0
        assert result["Close"].min() > 300.0

    def test_custom_threshold(self):
        """カスタムしきい値（30%）での不連続検出。"""
        # 40% 下落: デフォルト(50%)では検出されない、30% しきい値では検出される
        # close_old は 100.0 固定（最終値 = 100.0）
        close_old = [100.0] * 30
        # close_new は 60.0 から開始（(60-100)/100 = -40%: 50%未満、30%超）
        close_new = [60.0 + i * 0.1 for i in range(30)]
        df = _make_df(close_old + close_new)

        # デフォルト(50%)では 40% 下落は検出されず切り詰めなし
        result_default = _trim_price_discontinuity(df, threshold=0.50)
        assert len(result_default) == len(df)

        # 30% しきい値では 40% 下落が検出されて切り詰めあり
        result_custom = _trim_price_discontinuity(df, threshold=0.30)
        assert len(result_custom) < len(df)

    def test_realistic_topix_trim_scenario(self):
        """1306.T 相当シナリオ（スプリット2ヶ月前）: 不連続点後60日以上 → 切り捨て。"""
        # 前半60日: 3700-4100円台（旧価格）
        close_old = [3700.0 + i * 5 for i in range(60)]
        # 後半60日: 370-390円台（正常価格）
        close_new = [370.0 + i * 0.3 for i in range(60)]

        df = _make_df(close_old + close_new)
        result = _trim_price_discontinuity(df)

        # 不連続点以降60行のみが残る（切り捨て戦略）
        assert len(result) == 60
        # テクニカル分析用データが正常なスケール（370-390円台）
        assert result["Close"].min() > 300.0
        assert result["Close"].max() < 450.0

    def test_realistic_topix_scale_scenario(self):
        """1306.T 相当シナリオ（スプリット直後）: 不連続点後5日のみ → スケール調整。"""
        # 115日: 3700-4100円台（旧価格）
        close_old = [3700.0 + i * 5 for i in range(115)]
        # 直近5日: 370-388円台（スプリット後の正常価格）
        close_new = [370.0 + i * 4.5 for i in range(5)]

        df = _make_df(close_old + close_new)
        result = _trim_price_discontinuity(df)

        # スケール調整後は全行を保持
        assert len(result) == 120
        # 全データが新スケール（370-390円台）に正規化される
        assert result["Close"].max() < 500.0
        assert result["Close"].min() > 300.0
        # 最新値は元データと一致
        assert abs(result["Close"].iloc[-1] - close_new[-1]) < 0.01
