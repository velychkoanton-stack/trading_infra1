from __future__ import annotations

import time
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from Common.config.path_config import get_project_root
from Common.config.rules_loader import load_rules_file
from Common.db.db_execute import execute, execute_many, fetch_all
from Common.db.heartbeat_writer import write_heartbeat
from Common.exchange.bybit_client import (
    build_not_in_params,
    create_bybit_client,
    fetch_linear_perpetual_symbols,
    fetch_ohlcv_with_retry,
)
from Common.statistics.adf_test import run_adf_test_from_close_df
from Common.utils.cleanup import cleanup_objects, force_gc
from Common.utils.logger import setup_logger


VALID_SLEEP_STATUSES = {"SLEEP", "STOP", "SL_BLOCK"}


def load_text_file(file_path: Path) -> str:
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    return file_path.read_text(encoding="utf-8").strip()


def str_to_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def build_ohlcv_dataframe(rows: list[list[Any]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])


def write_selection_symbol_ohlcv_parquet(project_root: Path, symbol: str, df: pd.DataFrame) -> Path:
    """
    Selection-layer own parquet cache:
    data/parquet_db_select/<BASE>.parquet
    """
    safe_name = symbol.replace("/", "_").replace(":", "_")
    parquet_dir = project_root / "data" / "parquet_db_select"
    parquet_dir.mkdir(parents=True, exist_ok=True)

    file_path = parquet_dir / f"{safe_name}.parquet"
    df.to_parquet(file_path, index=False)
    return file_path


def compute_liquidity_metrics_from_1h(df: pd.DataFrame) -> tuple[float, float]:
    """
    Source dataframe is 1h OHLCV.

    liq_1h_mean:
        mean(close * volume) on 1h candles

    liq_5min_mean:
        approximate 5m notional as liq_1h_mean / 12
    """
    work_df = df.copy()

    work_df["notional"] = pd.to_numeric(work_df["close"], errors="coerce") * pd.to_numeric(
        work_df["volume"], errors="coerce"
    )

    liq_1h_mean = float(work_df["notional"].mean())
    liq_5min_mean = float(liq_1h_mean / 12.0)

    return liq_5min_mean, liq_1h_mean


class AssetWorker:
    def __init__(self) -> None:
        self.project_root = get_project_root()
        self.worker_dir = Path(__file__).resolve().parent
        self.rules_dir = self.worker_dir / "rules"
        self.sql_dir = self.worker_dir / "sql_queries"

        self.rules = load_rules_file(self.rules_dir / "asset_rules.txt")

        self.batch_size = int(self.rules["BATCH_SIZE"])
        self.timeframe = self.rules["TIMEFRAME"]              # now expected: 1h
        self.target_candles = int(self.rules["TARGET_CANDLES"])  # now expected: 1000
        self.skip_fresh_recheck_days = int(self.rules["SKIP_FRESH_RECHECK_DAYS"])
        self.tested_refresh_days = int(self.rules["TESTED_REFRESH_DAYS"])
        self.adf_alpha = float(self.rules["ADF_ALPHA"])
        self.adf_use_log = str_to_bool(self.rules["ADF_USE_LOG"])
        self.min_ohlcv_rows = int(self.rules["MIN_OHLCV_ROWS"])  # now expected: 1000
        self.bybit_api_file = self.rules["BYBIT_API_FILE"]
        self.mysql_api_file = self.rules["MYSQL_API_FILE"]

        self.worker_name = self.rules.get("WORKER_NAME", "asset_worker")
        self.timezone_name = self.rules.get("TIMEZONE", "Europe/Amsterdam")
        self.scheduler_sleep_check_sec = int(self.rules.get("SCHEDULER_SLEEP_CHECK_SEC", "3600"))

        self.tz = ZoneInfo(self.timezone_name)

        log_file = self.project_root / self.rules["LOG_FILE"]
        self.logger = setup_logger("asset_worker", log_file)

        self.sql_upsert_listed_assets = load_text_file(self.sql_dir / "upsert_listed_assets.txt")
        self.sql_mark_delisted_assets = load_text_file(self.sql_dir / "mark_delisted_assets.txt")
        self.sql_select_assets = load_text_file(self.sql_dir / "select_assets_for_processing.txt")
        self.sql_update_asset_skip_fresh = load_text_file(
            self.sql_dir / "update_asset_skip_fresh.txt"
        )
        self.sql_update_asset_tested = load_text_file(self.sql_dir / "update_asset_tested.txt")
        self.sql_get_scheduler_statuses = load_text_file(
            self.sql_dir / "get_scheduler_statuses.txt"
        )

        # worker runtime client for OHLCV fetching
        self.bybit_client = create_bybit_client(self.bybit_api_file)

    def sync_exchange_symbols(self) -> list[str]:
        # important: use fresh client for exchange market sync
        fresh_client = create_bybit_client(self.bybit_api_file)

        listed_symbols_raw = fetch_linear_perpetual_symbols(fresh_client)

        listed_symbols = sorted(
            symbol
            for symbol in listed_symbols_raw
            if str(symbol).endswith("/USDT:USDT")
        )

        self.logger.info(
            "CTSI present in fetched symbols = %s",
            "CTSI/USDT:USDT" in listed_symbols,
        )

        if not listed_symbols:
            raise RuntimeError("Bybit returned zero listed /USDT:USDT linear perpetual symbols. Sync aborted.")

        params_seq = [(symbol,) for symbol in listed_symbols]
        execute_many(
            sql=self.sql_upsert_listed_assets,
            api_file_name=self.mysql_api_file,
            params_seq=params_seq,
        )

        placeholders, params = build_not_in_params(listed_symbols)
        mark_delisted_sql = self.sql_mark_delisted_assets.format(
            listed_symbols_placeholders=placeholders
        )
        execute(
            sql=mark_delisted_sql,
            api_file_name=self.mysql_api_file,
            params=params,
        )

        self.logger.info(
            "Market sync completed | listed_symbols_total=%s | filtered_usdt_symbols=%s",
            len(listed_symbols_raw),
            len(listed_symbols),
        )
        return listed_symbols

    def select_assets_for_processing(self) -> list[dict[str, Any]]:
        sql = self.sql_select_assets.format(
            skip_fresh_recheck_days=self.skip_fresh_recheck_days,
            tested_refresh_days=self.tested_refresh_days,
            batch_size=self.batch_size,
        )

        rows = fetch_all(
            sql=sql,
            api_file_name=self.mysql_api_file,
        )

        self.logger.info("Selected assets for processing | count=%s", len(rows))
        return rows

    def mark_skip_fresh(self, symbol: str) -> None:
        execute(
            sql=self.sql_update_asset_skip_fresh,
            api_file_name=self.mysql_api_file,
            params=(symbol,),
        )

    def mark_tested(
        self,
        symbol: str,
        liq_5min_mean: float,
        liq_1h_mean: float,
        adf_stat: float,
        p_value: float,
        is_non_stationary: bool,
    ) -> None:
        execute(
            sql=self.sql_update_asset_tested,
            api_file_name=self.mysql_api_file,
            params=(
                liq_5min_mean,
                liq_1h_mean,
                adf_stat,
                p_value,
                int(is_non_stationary),
                symbol,
            ),
        )

    def process_one_asset(self, symbol: str) -> None:
        self.logger.info("Processing asset | symbol=%s", symbol)

        rows = fetch_ohlcv_with_retry(
            bybit_client=self.bybit_client,
            symbol=symbol,
            timeframe=self.timeframe,
            limit=self.target_candles,
        )

        row_count = len(rows)

        if row_count < self.min_ohlcv_rows:
            self.mark_skip_fresh(symbol)
            self.logger.info(
                "Asset marked skip_fresh | symbol=%s rows=%s target=%s",
                symbol,
                row_count,
                self.min_ohlcv_rows,
            )
            cleanup_objects(rows)
            force_gc()
            return

        df = build_ohlcv_dataframe(rows)

        parquet_path = write_selection_symbol_ohlcv_parquet(
            project_root=self.project_root,
            symbol=symbol,
            df=df,
        )

        liq_5min_mean, liq_1h_mean = compute_liquidity_metrics_from_1h(df)

        adf_result = run_adf_test_from_close_df(
            df=df,
            alpha=self.adf_alpha,
            use_log=self.adf_use_log,
            close_column="close",
        )

        self.mark_tested(
            symbol=symbol,
            liq_5min_mean=liq_5min_mean,
            liq_1h_mean=liq_1h_mean,
            adf_stat=float(adf_result["adf"]),
            p_value=float(adf_result["p_value"]),
            is_non_stationary=bool(adf_result["is_non_stationary"]),
        )

        self.logger.info(
            "Asset tested | symbol=%s rows=%s parquet=%s liq_5min_mean=%.4f liq_1h_mean=%.4f adf=%.6f p_value=%.6f non_stationary=%s",
            symbol,
            row_count,
            parquet_path,
            liq_5min_mean,
            liq_1h_mean,
            float(adf_result["adf"]),
            float(adf_result["p_value"]),
            bool(adf_result["is_non_stationary"]),
        )

        cleanup_objects(rows, df, adf_result)
        force_gc()

    def run_one_cycle(self) -> tuple[int, int]:
        self.logger.info("Asset worker daily cycle started")

        listed_symbols = self.sync_exchange_symbols()
        self.logger.info("Listed symbols synced | count=%s", len(listed_symbols))

        total_processed = 0
        batch_number = 0

        while True:
            assets = self.select_assets_for_processing()

            if not assets:
                self.logger.info(
                    "No more assets selected for processing | total_processed=%s batches=%s",
                    total_processed,
                    batch_number,
                )
                break

            batch_number += 1
            self.logger.info(
                "Starting batch | batch_number=%s batch_size=%s",
                batch_number,
                len(assets),
            )

            for asset in assets:
                symbol = asset["symbol"]

                try:
                    self.process_one_asset(symbol)
                    total_processed += 1
                except Exception:
                    self.logger.exception("Asset processing failed | symbol=%s", symbol)
                    force_gc()

            force_gc()

        self.logger.info(
            "Asset worker daily cycle finished | total_processed=%s batches=%s",
            total_processed,
            batch_number,
        )
        return total_processed, batch_number

    def _get_effective_control_status(self) -> tuple[str, str]:
        rows = fetch_all(
            sql=self.sql_get_scheduler_statuses,
            api_file_name=self.mysql_api_file,
            params={"worker_id": self.worker_name},
        )

        mapped = {row["worker_id"]: row for row in rows}
        global_row = mapped.get("GLOBAL")
        worker_row = mapped.get(self.worker_name)

        if global_row and str(global_row.get("control_status", "")).upper() in VALID_SLEEP_STATUSES:
            return str(global_row["control_status"]).upper(), str(global_row.get("comment") or "")

        if worker_row:
            return str(worker_row.get("control_status", "RUNNING")).upper(), str(worker_row.get("comment") or "")

        return "RUNNING", "default_missing_scheduler_row"

    def _safe_write_heartbeat(self, runtime_status: str, comment: str | None) -> None:
        try:
            write_heartbeat(
                worker_id=self.worker_name,
                runtime_status=runtime_status,
                comment=self._truncate_comment(comment),
                api_file_name=self.mysql_api_file,
            )
        except Exception:
            self.logger.exception(
                "Failed to write heartbeat | worker=%s | runtime_status=%s | comment=%s",
                self.worker_name,
                runtime_status,
                comment,
            )

    def _get_local_today_str(self) -> str:
        return datetime.now(self.tz).strftime("%Y-%m-%d")

    @staticmethod
    def _truncate_comment(comment: str | None, max_len: int = 64) -> str | None:
        if comment is None:
            return None
        return str(comment)[:max_len]

    def run_forever(self) -> None:
        self.logger.info(
            "Asset worker started | worker_name=%s | timezone=%s | scheduler_sleep_check_sec=%s",
            self.worker_name,
            self.timezone_name,
            self.scheduler_sleep_check_sec,
        )

        last_completed_local_day: str | None = None

        while True:
            try:
                control_status, control_comment = self._get_effective_control_status()
                today_local = self._get_local_today_str()

                if control_status in VALID_SLEEP_STATUSES:
                    self._safe_write_heartbeat(
                        runtime_status="SLEEPING",
                        comment=f"sleep_by_scheduler:{control_status}",
                    )
                    self.logger.info(
                        "Asset worker sleeping by scheduler | status=%s | comment=%s",
                        control_status,
                        control_comment,
                    )
                    time.sleep(self.scheduler_sleep_check_sec)
                    continue

                if last_completed_local_day == today_local:
                    self._safe_write_heartbeat(
                        runtime_status="SLEEPING",
                        comment="daily_cycle_already_done",
                    )
                    self.logger.info(
                        "Asset worker daily cycle already completed today | date=%s",
                        today_local,
                    )
                    time.sleep(self.scheduler_sleep_check_sec)
                    continue

                self._safe_write_heartbeat("RUNNING", "daily_cycle_started")
                total_processed, batch_number = self.run_one_cycle()
                last_completed_local_day = today_local

                self._safe_write_heartbeat(
                    "RUNNING",
                    f"daily_cycle_done:assets={total_processed},batches={batch_number}",
                )

                self.logger.info(
                    "Asset worker cycle complete | date=%s | total_processed=%s | batches=%s",
                    today_local,
                    total_processed,
                    batch_number,
                )

                time.sleep(self.scheduler_sleep_check_sec)

            except Exception as exc:
                self.logger.exception("Asset worker loop failed")
                self._safe_write_heartbeat(
                    runtime_status="ERROR",
                    comment=f"loop_error:{type(exc).__name__}",
                )
                time.sleep(10)

            finally:
                force_gc()


def main() -> None:
    worker = AssetWorker()
    worker.run_forever()


if __name__ == "__main__":
    main()