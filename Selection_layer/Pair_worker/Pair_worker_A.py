from __future__ import annotations

import itertools
import time
from pathlib import Path
from typing import Any

import pandas as pd

from Common.config.path_config import get_project_root
from Common.config.rules_loader import load_rules_file
from Common.db.db_execute import execute, execute_many, fetch_all
from Common.db.deadlock_retry import run_with_deadlock_retry
from Common.db.heartbeat_writer import write_heartbeat
from Common.exchange.bybit_client import create_bybit_client, fetch_ohlcv_with_retry
from Common.parquet.parquet_reader import read_symbol_ohlcv_parquet
from Common.parquet.parquet_updater import replace_symbol_ohlcv_parquet
from Common.statistics.adf_test import run_adf_test_from_series
from Common.statistics.beta_calc import build_spread_from_dfs, calculate_beta_from_dfs, normalize_beta
from Common.statistics.half_life import calculate_half_life
from Common.statistics.hurst import calculate_hurst_exponent
from Common.statistics.scoring import score_stat_test
from Common.statistics.spread_stats import calculate_spread_stats
from Common.utils.cleanup import cleanup_objects, force_gc
from Common.utils.logger import setup_logger
from Common.utils.sql_file_loader import load_sql_file


VALID_SLEEP_STATUSES = {"SLEEP", "STOP", "SL_BLOCK"}


def str_to_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_optional_float(value: str | None) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if text == "" or text.upper() == "NONE":
        return None
    return float(text)


def build_pair_uuid(asset_1: str, asset_2: str) -> str:
    def base_symbol(symbol: str) -> str:
        return symbol.split("/")[0].strip().upper()

    base_1 = base_symbol(asset_1)
    base_2 = base_symbol(asset_2)
    ordered = sorted([base_1, base_2])
    return f"{ordered[0]}_{ordered[1]}"


class PairWorkerA:
    def __init__(self) -> None:
        self.project_root = get_project_root()
        self.worker_dir = Path(__file__).resolve().parent
        self.rules_dir = self.worker_dir / "rules"
        self.sql_dir = self.worker_dir / "sql_queries"

        self.rules = self._load_rules()

        self.worker_name = self.rules.get("WORKER_A_NAME", "pair_worker_a")
        self.mysql_api_file = self.rules.get("MYSQL_API_FILE", "api_mysql_main.txt")
        self.bybit_api_file = self.rules.get("BYBIT_API_FILE", "api_bybit_main.txt")
        self.timeframe = self.rules.get("TIMEFRAME", "5m")
        self.target_candles = int(self.rules.get("TARGET_CANDLES", "10000"))
        self.scheduler_sleep_check_sec = int(self.rules.get("SCHEDULER_SLEEP_CHECK_SEC", "300"))

        self.target_working_pairs = int(self.rules.get("TARGET_WORKING_PAIRS", "1000"))
        self.target_candidate_buffer = int(self.rules.get("TARGET_CANDIDATE_BUFFER", "1200"))
        self.max_bt_pending_queue = int(self.rules.get("MAX_BT_PENDING_QUEUE", "10"))
        self.max_new_pairs_per_run = int(self.rules.get("MAX_NEW_PAIRS_PER_RUN", "300"))
        self.max_stat_tests_per_run = int(self.rules.get("MAX_STAT_TESTS_PER_RUN", "300"))
        self.pair_retention_days_forbidden_fail = int(
            self.rules.get("PAIR_RETENTION_DAYS_FORBIDDEN_FAIL", "30")
        )

        log_file = self.project_root / self.rules.get("LOG_FILE_A", "data/logs/pair_worker_a.log")
        self.logger = setup_logger("pair_worker_a", log_file)

        self.bybit_client = create_bybit_client(self.bybit_api_file)

        self.sql_get_scheduler_statuses = load_sql_file(self.sql_dir / "get_scheduler_statuses.txt")
        self.sql_select_control_counts = load_sql_file(self.sql_dir / "select_control_counts.txt")
        self.sql_reset_old_forbidden_fail_pairs = load_sql_file(
            self.sql_dir / "reset_old_forbidden_fail_pairs.txt"
        )
        self.sql_mark_invalid_pairs_removed = load_sql_file(
            self.sql_dir / "mark_invalid_pairs_removed.txt"
        )
        self.sql_select_asset_pool_primary = load_sql_file(
            self.sql_dir / "select_asset_pool_primary.txt"
        )
        self.sql_select_asset_pool_fallback = load_sql_file(
            self.sql_dir / "select_asset_pool_fallback.txt"
        )
        self.sql_insert_new_pairs_ignore = load_sql_file(
            self.sql_dir / "insert_new_pairs_ignore.txt"
        )
        self.sql_select_pairs_for_stat_test = load_sql_file(
            self.sql_dir / "select_pairs_for_stat_test.txt"
        )
        self.sql_update_pair_stat_state_running = load_sql_file(
            self.sql_dir / "update_pair_stat_state_running.txt"
        )
        self.sql_update_pair_stat_test_result = load_sql_file(
            self.sql_dir / "update_pair_stat_test_result.txt"
        )
        self.sql_update_pair_stat_test_fail = load_sql_file(
            self.sql_dir / "update_pair_stat_test_fail.txt"
        )

    def _load_rules(self) -> dict[str, str]:
        return load_rules_file(self.rules_dir / "pair_rules.txt")

    def reload_rules_for_loop(self) -> None:
        self.rules = self._load_rules()

        self.worker_name = self.rules.get("WORKER_A_NAME", "pair_worker_a")
        self.mysql_api_file = self.rules.get("MYSQL_API_FILE", "api_mysql_main.txt")
        self.bybit_api_file = self.rules.get("BYBIT_API_FILE", "api_bybit_main.txt")
        self.timeframe = self.rules.get("TIMEFRAME", "5m")
        self.target_candles = int(self.rules.get("TARGET_CANDLES", "10000"))
        self.scheduler_sleep_check_sec = int(self.rules.get("SCHEDULER_SLEEP_CHECK_SEC", "300"))

        self.target_working_pairs = int(self.rules.get("TARGET_WORKING_PAIRS", "1000"))
        self.target_candidate_buffer = int(self.rules.get("TARGET_CANDIDATE_BUFFER", "1200"))
        self.max_bt_pending_queue = int(self.rules.get("MAX_BT_PENDING_QUEUE", "10"))
        self.max_new_pairs_per_run = int(self.rules.get("MAX_NEW_PAIRS_PER_RUN", "300"))
        self.max_stat_tests_per_run = int(self.rules.get("MAX_STAT_TESTS_PER_RUN", "300"))
        self.pair_retention_days_forbidden_fail = int(
            self.rules.get("PAIR_RETENTION_DAYS_FORBIDDEN_FAIL", "30")
        )

    def _execute(self, sql: str, params: Any | None = None) -> Any:
        return run_with_deadlock_retry(
            lambda: execute(
                sql=sql,
                api_file_name=self.mysql_api_file,
                params=params,
            )
        )

    def _execute_many(self, sql: str, params_seq: list[Any]) -> Any:
        return run_with_deadlock_retry(
            lambda: execute_many(
                sql=sql,
                api_file_name=self.mysql_api_file,
                params_seq=params_seq,
            )
        )

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
            return str(worker_row.get("control_status", "RUNNING")).upper(), str(
                worker_row.get("comment") or ""
            )

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

    @staticmethod
    def _truncate_comment(comment: str | None, max_len: int = 64) -> str | None:
        if comment is None:
            return None
        return str(comment)[:max_len]

    def reset_old_forbidden_fail_pairs(self) -> int:
        sql = self.sql_reset_old_forbidden_fail_pairs.format(
            pair_retention_days_forbidden_fail=self.pair_retention_days_forbidden_fail
        )
        rows_affected = self._execute(sql=sql)
        self.logger.info("Reusable pair reset pass done | rows_affected=%s", rows_affected)
        return int(rows_affected or 0)

    def mark_invalid_pairs_removed(self) -> int:
        rows_affected = self._execute(sql=self.sql_mark_invalid_pairs_removed)
        self.logger.info("Invalid pair cleanup done | rows_affected=%s", rows_affected)
        return int(rows_affected or 0)

    def fetch_control_counts(self) -> dict[str, int]:
        rows = fetch_all(
            sql=self.sql_select_control_counts,
            api_file_name=self.mysql_api_file,
        )

        if not rows:
            return {
                "working_count": 0,
                "candidate_count": 0,
                "bt_pending_count": 0,
            }

        row = rows[0]
        return {
            "working_count": int(row.get("working_count") or 0),
            "candidate_count": int(row.get("candidate_count") or 0),
            "bt_pending_count": int(row.get("bt_pending_count") or 0),
        }

    def needs_supply(self, counts: dict[str, int]) -> bool:
        if counts["working_count"] < self.target_working_pairs:
            return True

        if counts["candidate_count"] < self.target_candidate_buffer:
            return True

        if counts["bt_pending_count"] < self.max_bt_pending_queue:
            return True

        return False

    def build_asset_filter_sql_parts(self) -> tuple[str, str]:
        asset_max_adf = parse_optional_float(self.rules.get("ASSET_MAX_ADF"))
        asset_max_pvalue = parse_optional_float(self.rules.get("ASSET_MAX_PVALUE"))

        asset_adf_filter_sql = ""
        asset_pvalue_filter_sql = ""

        if asset_max_adf is not None:
            asset_adf_filter_sql = f"AND a.adf <= {asset_max_adf}"

        if asset_max_pvalue is not None:
            asset_pvalue_filter_sql = f"AND a.p_value <= {asset_max_pvalue}"

        return asset_adf_filter_sql, asset_pvalue_filter_sql

    def select_asset_pool(self) -> list[dict[str, Any]]:
        asset_adf_filter_sql, asset_pvalue_filter_sql = self.build_asset_filter_sql_parts()

        max_presence = float(
            self.rules.get(
                "ASSET_MAX_ACTIVE_PRESENCE",
                self.rules.get("MAX_ASSET_PRESENCE", "0.05"),
            )
        )

        sql_primary = self.sql_select_asset_pool_primary.format(
            asset_require_non_stationary_status=int(
                self.rules["ASSET_REQUIRE_NON_STATIONARY_STATUS"]
            ),
            min_liq_5m=float(self.rules["MIN_LIQ_5M"]),
            min_liq_1h=float(self.rules["MIN_LIQ_1H"]),
            max_asset_presence=max_presence,
            asset_pool_size=int(self.rules["ASSET_POOL_SIZE"]),
            asset_adf_filter_sql=asset_adf_filter_sql,
            asset_pvalue_filter_sql=asset_pvalue_filter_sql,
        )

        rows = fetch_all(sql=sql_primary, api_file_name=self.mysql_api_file)

        if len(rows) >= int(self.rules["ASSET_POOL_SIZE"]):
            return rows

        sql_fallback = self.sql_select_asset_pool_fallback.format(
            asset_require_non_stationary_status=int(
                self.rules["ASSET_REQUIRE_NON_STATIONARY_STATUS"]
            ),
            min_liq_5m=float(self.rules["MIN_LIQ_5M"]),
            min_liq_1h=float(self.rules["MIN_LIQ_1H"]),
            asset_pool_size=int(self.rules["ASSET_POOL_SIZE"]),
            asset_adf_filter_sql=asset_adf_filter_sql,
            asset_pvalue_filter_sql=asset_pvalue_filter_sql,
        )

        return fetch_all(sql=sql_fallback, api_file_name=self.mysql_api_file)

    def refresh_pool_parquet_data(self, asset_pool: list[dict[str, Any]]) -> dict[str, pd.DataFrame]:
        asset_data: dict[str, pd.DataFrame] = {}

        for asset in asset_pool:
            symbol = asset["symbol"]

            try:
                self.logger.info("Refreshing parquet for stat worker | symbol=%s", symbol)
                rows = fetch_ohlcv_with_retry(
                    bybit_client=self.bybit_client,
                    symbol=symbol,
                    timeframe=self.timeframe,
                    limit=self.target_candles,
                )
                replace_symbol_ohlcv_parquet(symbol=symbol, rows=rows)

                df = read_symbol_ohlcv_parquet(symbol)
                if df is None or df.empty:
                    self.logger.warning("Parquet refresh returned empty dataframe | symbol=%s", symbol)
                    continue

                asset_data[symbol] = df

            except Exception:
                self.logger.exception("Failed refreshing parquet | symbol=%s", symbol)

        return asset_data

    def create_missing_pairs(self, asset_pool: list[dict[str, Any]]) -> int:
        if len(asset_pool) < 2:
            return 0

        pair_rows: list[tuple[Any, ...]] = []
        asset_info = {row["symbol"]: row for row in asset_pool}

        for asset_a, asset_b in itertools.combinations(sorted(asset_info.keys()), 2):
            asset_1, asset_2 = sorted([asset_a, asset_b])

            row_1 = asset_info[asset_1]
            row_2 = asset_info[asset_2]

            pair_uuid = build_pair_uuid(asset_1, asset_2)
            pair_liq_5m = float(min(row_1["liq_5min_mean"], row_2["liq_5min_mean"]))
            pair_liq_1h = float(min(row_1["liq_1h_mean"], row_2["liq_1h_mean"]))
            activity_score = pair_liq_1h

            pair_rows.append(
                (
                    pair_uuid,
                    asset_1,
                    asset_2,
                    pair_liq_5m,
                    pair_liq_1h,
                    activity_score,
                )
            )

        if not pair_rows:
            return 0

        pair_rows = pair_rows[: self.max_new_pairs_per_run]
        batch_size = int(self.rules.get("PAIR_BATCH_INSERT_SIZE", "500"))
        inserted_estimate = 0

        for start in range(0, len(pair_rows), batch_size):
            batch = pair_rows[start : start + batch_size]
            rows_inserted = self._execute_many(
                sql=self.sql_insert_new_pairs_ignore,
                params_seq=batch,
            )
            inserted_estimate += int(rows_inserted or 0)

        return inserted_estimate

    def select_pairs_for_stat_test(self, asset_symbols: list[str]) -> list[dict[str, Any]]:
        if not asset_symbols:
            return []

        placeholders = ",".join(["%s"] * len(asset_symbols))
        stat_batch_size = min(
            int(self.rules.get("STAT_BATCH_SIZE", "50")),
            self.max_stat_tests_per_run,
        )

        sql = self.sql_select_pairs_for_stat_test.format(
            asset_pool_placeholders=placeholders,
            allow_stat_retry=1 if str_to_bool(self.rules.get("ALLOW_STAT_RETRY", "true")) else 0,
            stat_retry_cooldown_hours=int(self.rules.get("STAT_RETRY_COOLDOWN_HOURS", "24")),
            stat_batch_size=stat_batch_size,
        )

        params = tuple(asset_symbols) + tuple(asset_symbols)

        return fetch_all(
            sql=sql,
            api_file_name=self.mysql_api_file,
            params=params,
        )

    def _passes_optional_min(self, value: float, rule_value: str | None) -> bool:
        threshold = parse_optional_float(rule_value)
        if threshold is None:
            return True
        return float(value) >= threshold

    def _passes_optional_max(self, value: float, rule_value: str | None) -> bool:
        threshold = parse_optional_float(rule_value)
        if threshold is None:
            return True
        return float(value) <= threshold

    def evaluate_stat_thresholds(
        self,
        adf: float,
        p_value: float,
        hurst: float,
        hl: float,
        spread_skew: float,
        spread_kurt: float,
        beta: float,
        beta_norm: float,
        stat_test_score: float,
    ) -> bool:
        checks = [
            self._passes_optional_min(adf, self.rules.get("STAT_MIN_ADF")),
            self._passes_optional_max(p_value, self.rules.get("STAT_MAX_PVALUE")),
            self._passes_optional_max(hurst, self.rules.get("STAT_MAX_HURST")),
            self._passes_optional_min(hl, self.rules.get("STAT_MIN_HL")),
            self._passes_optional_max(hl, self.rules.get("STAT_MAX_HL")),
            self._passes_optional_min(spread_skew, self.rules.get("STAT_MIN_SPREAD_SKEW")),
            self._passes_optional_max(spread_skew, self.rules.get("STAT_MAX_SPREAD_SKEW")),
            self._passes_optional_min(spread_kurt, self.rules.get("STAT_MIN_SPREAD_KURT")),
            self._passes_optional_max(spread_kurt, self.rules.get("STAT_MAX_SPREAD_KURT")),
            self._passes_optional_min(beta, self.rules.get("STAT_MIN_BETA")),
            self._passes_optional_max(beta, self.rules.get("STAT_MAX_BETA")),
            self._passes_optional_min(beta_norm, self.rules.get("STAT_MIN_BETA_NORM")),
            self._passes_optional_max(beta_norm, self.rules.get("STAT_MAX_BETA_NORM")),
        ]

        min_stat_score = parse_optional_float(self.rules.get("MIN_STAT_TEST_SCORE"))
        if min_stat_score is not None:
            checks.append(float(stat_test_score) >= min_stat_score)

        return all(checks)

    def run_stat_test_for_pair(
        self,
        pair_row: dict[str, Any],
        asset_data: dict[str, pd.DataFrame],
    ) -> bool:
        pair_id = pair_row["id"]
        asset_1 = pair_row["asset_1"]
        asset_2 = pair_row["asset_2"]

        self._execute(
            sql=self.sql_update_pair_stat_state_running,
            params=(pair_id,),
        )

        try:
            if asset_1 not in asset_data or asset_2 not in asset_data:
                raise ValueError(f"Missing parquet data for pair_id={pair_id}")

            df_1 = asset_data[asset_1]
            df_2 = asset_data[asset_2]

            beta = calculate_beta_from_dfs(df_1=df_1, df_2=df_2, use_log=True)
            beta_norm = normalize_beta(beta)

            spread_df = build_spread_from_dfs(
                df_1=df_1,
                df_2=df_2,
                beta=beta,
                use_log=True,
            )

            spread_series = spread_df["spread"]

            adf_result = run_adf_test_from_series(
                series=spread_series,
                alpha=0.05,
                use_log=False,
            )
            spread_stats = calculate_spread_stats(spread_series)
            hurst_value = calculate_hurst_exponent(spread_series)
            hl_value = calculate_half_life(spread_series)

            stat_test_score = score_stat_test(
                p_value=float(adf_result["p_value"]),
                hurst=float(hurst_value),
                half_life=float(hl_value),
            )

            stat_pass = self.evaluate_stat_thresholds(
                adf=float(adf_result["adf"]),
                p_value=float(adf_result["p_value"]),
                hurst=float(hurst_value),
                hl=float(hl_value),
                spread_skew=float(spread_stats["spread_skew"]),
                spread_kurt=float(spread_stats["spread_kurt"]),
                beta=float(beta),
                beta_norm=float(beta_norm),
                stat_test_score=float(stat_test_score),
            )

            new_bt_state = "pending" if stat_pass else "fail"

            self._execute(
                sql=self.sql_update_pair_stat_test_result,
                params=(
                    float(adf_result["adf"]),
                    float(adf_result["p_value"]),
                    float(hurst_value),
                    float(hl_value),
                    float(spread_stats["spread_skew"]),
                    float(spread_stats["spread_kurt"]),
                    float(beta),
                    float(beta_norm),
                    float(stat_test_score),
                    "done",
                    new_bt_state,
                    pair_id,
                ),
            )

            self.logger.info(
                "Stat test done | pair_id=%s %s | %s beta=%.4f beta_norm=%.4f adf=%.4f p=%.6f hurst=%.4f hl=%.2f score=%.4f bt_state=%s",
                pair_id,
                asset_1,
                asset_2,
                float(beta),
                float(beta_norm),
                float(adf_result["adf"]),
                float(adf_result["p_value"]),
                float(hurst_value),
                float(hl_value),
                float(stat_test_score),
                new_bt_state,
            )

            cleanup_objects(spread_df, spread_series, adf_result, spread_stats)
            return stat_pass

        except Exception:
            self._execute(
                sql=self.sql_update_pair_stat_test_fail,
                params=(pair_id,),
            )
            self.logger.exception(
                "Stat test failed | pair_id=%s asset_1=%s asset_2=%s",
                pair_id,
                asset_1,
                asset_2,
            )
            return False

    def run_once(self) -> None:
        self.reload_rules_for_loop()

        reset_count = self.reset_old_forbidden_fail_pairs()
        removed_count = self.mark_invalid_pairs_removed()

        counts = self.fetch_control_counts()
        self.logger.info(
            "Control counts | working=%s candidate=%s bt_pending=%s reset=%s removed=%s",
            counts["working_count"],
            counts["candidate_count"],
            counts["bt_pending_count"],
            reset_count,
            removed_count,
        )

        if not self.needs_supply(counts):
            self.logger.info("Supply targets already satisfied | no work needed")
            return

        asset_pool = self.select_asset_pool()
        if len(asset_pool) < 2:
            self.logger.info("Insufficient asset pool | count=%s", len(asset_pool))
            return

        asset_symbols = [row["symbol"] for row in asset_pool]
        self.logger.info("Selected asset pool | count=%s symbols=%s", len(asset_symbols), asset_symbols)

        asset_data = self.refresh_pool_parquet_data(asset_pool)
        available_symbols = sorted(asset_data.keys())

        if len(available_symbols) < 2:
            self.logger.info("Insufficient refreshed parquet-ready assets | count=%s", len(available_symbols))
            cleanup_objects(asset_pool, asset_data)
            force_gc()
            return

        parquet_asset_pool = [row for row in asset_pool if row["symbol"] in asset_data]
        created_pairs_estimate = self.create_missing_pairs(parquet_asset_pool)
        self.logger.info("Pair creation pass done | created_estimate=%s", created_pairs_estimate)

        total_stat_processed = 0
        total_stat_passed = 0

        while total_stat_processed < self.max_stat_tests_per_run:
            stat_pairs = self.select_pairs_for_stat_test(available_symbols)
            if not stat_pairs:
                break

            self.logger.info(
                "Pairs selected for stat test | batch_count=%s processed_so_far=%s max=%s",
                len(stat_pairs),
                total_stat_processed,
                self.max_stat_tests_per_run,
            )

            for row in stat_pairs:
                if total_stat_processed >= self.max_stat_tests_per_run:
                    break

                passed = self.run_stat_test_for_pair(row, asset_data)
                total_stat_processed += 1
                total_stat_passed += int(passed)
                force_gc()

        self.logger.info(
            "Pair worker A cycle complete | assets=%s created_pairs_estimate=%s stat_processed=%s stat_passed=%s",
            len(available_symbols),
            created_pairs_estimate,
            total_stat_processed,
            total_stat_passed,
        )

        cleanup_objects(asset_pool, asset_data, parquet_asset_pool)
        force_gc()

    def run_forever(self) -> None:
        self.logger.info(
            "Pair worker A started | target_working=%s target_candidate=%s max_bt_pending=%s max_new_pairs=%s max_stat_tests=%s",
            self.target_working_pairs,
            self.target_candidate_buffer,
            self.max_bt_pending_queue,
            self.max_new_pairs_per_run,
            self.max_stat_tests_per_run,
        )

        while True:
            try:
                control_status, control_comment = self._get_effective_control_status()

                if control_status in VALID_SLEEP_STATUSES:
                    self._safe_write_heartbeat(
                        runtime_status="SLEEPING",
                        comment=f"sleep_by_scheduler:{control_status}",
                    )
                    self.logger.info(
                        "Pair worker A sleeping by scheduler | status=%s | comment=%s",
                        control_status,
                        control_comment,
                    )
                    time.sleep(self.scheduler_sleep_check_sec)
                    continue

                loop_started = time.time()
                self._safe_write_heartbeat("RUNNING", "loop_started")

                self.run_once()

                self._safe_write_heartbeat("RUNNING", "loop_ok")
                elapsed = time.time() - loop_started

                self.logger.info("Pair worker A loop finished | elapsed_sec=%.2f", elapsed)
                time.sleep(max(1.0, self.scheduler_sleep_check_sec))

            except Exception as exc:
                self.logger.exception("Pair worker A loop failed")
                self._safe_write_heartbeat(
                    runtime_status="ERROR",
                    comment=self._truncate_comment(f"loop_error:{type(exc).__name__}"),
                )
                time.sleep(10)

            finally:
                force_gc()


def main() -> None:
    worker = PairWorkerA()
    worker.run_forever()


if __name__ == "__main__":
    main()