from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from Common.db.db_execute import fetch_all, execute
from Common.db.heartbeat_writer import write_heartbeat
from Common.utils.cleanup import force_gc
from Common.utils.logger import setup_logger
from Common.utils.sql_file_loader import load_sql_file
from Common.utils.telegram_sender import send_tg_message


VALID_CONTROL_STATUSES = {"RUNNING", "SLEEP", "SL_BLOCK", "STOP"}


@dataclass(frozen=True)
class WeekendWindow:
    start_weekday: int  # Monday=0 ... Sunday=6
    start_hour: int
    start_minute: int
    end_weekday: int
    end_hour: int
    end_minute: int


@dataclass(frozen=True)
class DailyWindow:
    start_hour: int
    start_minute: int
    end_hour: int
    end_minute: int


class SchedulerWorker:
    def __init__(self) -> None:
        self.scheduler_dir = Path(__file__).resolve().parent
        self.project_root = self.scheduler_dir.parents[1]

        self.rules_path = self.scheduler_dir / "rules" / "rules.txt"
        self.sql_dir = self.scheduler_dir / "sql_queries"
        self.log_path = self.project_root / "data" / "logs" / "Execution_layer" / "Scheduler" / "scheduler.log"

        self.logger = setup_logger(
            logger_name="ExecutionLayer.Scheduler",
            log_file_path=self.log_path,
        )

        self.rules = self._load_rules_file(self.rules_path)
        self.mysql_api_file = self.rules.get("mysql_api_file", "api_mysql_main.txt")
        self.telegram_api_file = self.rules.get("telegram_api_file", "api_telegram_main.txt")
        self.loop_sec = self._parse_positive_int(self.rules.get("scheduler_loop_sec", "60"), "scheduler_loop_sec")
        self.update_only_on_change = self._parse_bool(self.rules.get("update_only_on_change", "1"))
        self.timezone_name = self.rules.get("timezone", "Europe/Amsterdam")
        self.tz = ZoneInfo(self.timezone_name)

        self.heartbeat_monitor_enabled = self._parse_bool(
            self.rules.get("heartbeat_monitor_enabled", "1")
        )
        self.heartbeat_alert_resend_sec = self._parse_positive_int(
            self.rules.get("heartbeat_alert_resend_sec", "1800"),
            "heartbeat_alert_resend_sec",
        )
        self.heartbeat_send_recovery = self._parse_bool(
            self.rules.get("heartbeat_send_recovery", "1")
        )
        self.heartbeat_stale_multiplier = self._parse_positive_int(
            self.rules.get("heartbeat_stale_multiplier", "2"),
            "heartbeat_stale_multiplier",
        )
        self.heartbeat_stale_min_sec = self._parse_positive_int(
            self.rules.get("heartbeat_stale_min_sec", "120"),
            "heartbeat_stale_min_sec",
        )

        self.weekend_window = self._parse_weekend_window(
            self.rules["weekend_block_start"],
            self.rules["weekend_block_end"],
        )
        self.worker_matrix = self._build_worker_matrix(self.rules)

        self.select_scheduler_all_sql = load_sql_file(self.sql_dir / "select_scheduler_all.txt")
        self.upsert_scheduler_row_sql = load_sql_file(self.sql_dir / "upsert_scheduler_row.txt")
        self.select_bot_heartbeat_all_sql = load_sql_file(self.sql_dir / "select_bot_heartbeat_all.txt")

        self.active_alerts: dict[str, dict[str, Any]] = {}

    def run_forever(self) -> None:
        self.logger.info("Scheduler worker started. loop_sec=%s timezone=%s", self.loop_sec, self.timezone_name)

        self._safe_write_heartbeat(
            runtime_status="RUNNING",
            comment="scheduler_started",
        )

        while True:
            loop_started = time.time()

            try:
                self.run_once()

                self._safe_write_heartbeat(
                    runtime_status="RUNNING",
                    comment="loop_ok",
                )

            except Exception as exc:
                self.logger.exception("Scheduler loop failed")

                self._safe_write_heartbeat(
                    runtime_status="ERROR",
                    comment=self._truncate_comment(f"loop_error:{type(exc).__name__}"),
                )

            elapsed = time.time() - loop_started
            sleep_seconds = max(1.0, self.loop_sec - elapsed)
            time.sleep(sleep_seconds)
            force_gc()

    def run_once(self) -> None:
        now_local = datetime.now(self.tz)
        now_naive = now_local.replace(tzinfo=None)

        existing_rows = self._fetch_existing_scheduler_rows()
        desired_rows = self._build_desired_scheduler_rows(now_local, now_naive)

        changed_count = 0

        for worker_id, desired_row in desired_rows.items():
            current_row = existing_rows.get(worker_id)

            should_write = True
            if self.update_only_on_change and current_row is not None:
                should_write = not self._rows_equal(current_row, desired_row)

            if should_write:
                execute(
                    sql=self.upsert_scheduler_row_sql,
                    api_file_name=self.mysql_api_file,
                    params=desired_row,
                )
                changed_count += 1
                self.logger.info(
                    "scheduler updated | worker_id=%s | control_status=%s | comment=%s",
                    desired_row["worker_id"],
                    desired_row["control_status"],
                    desired_row["comment"],
                )

        self.logger.info(
            "scheduler loop complete | workers_total=%s | rows_changed=%s | now=%s",
            len(desired_rows),
            changed_count,
            now_local.strftime("%Y-%m-%d %H:%M:%S %Z"),
        )

        if self.heartbeat_monitor_enabled:
            self._monitor_scheduler_vs_heartbeat(
                desired_rows=desired_rows,
                now_naive=now_naive,
            )

    def _safe_write_heartbeat(self, runtime_status: str, comment: str | None) -> None:
        try:
            write_heartbeat(
                worker_id="scheduler",
                runtime_status=runtime_status,
                comment=comment,
                api_file_name=self.mysql_api_file,
            )
        except Exception:
            self.logger.exception(
                "Failed to write scheduler heartbeat | runtime_status=%s | comment=%s",
                runtime_status,
                comment,
            )

    def _fetch_existing_scheduler_rows(self) -> dict[str, dict[str, Any]]:
        rows = fetch_all(
            sql=self.select_scheduler_all_sql,
            api_file_name=self.mysql_api_file,
        )
        return {row["worker_id"]: row for row in rows}

    def _fetch_heartbeat_rows(self) -> dict[str, dict[str, Any]]:
        rows = fetch_all(
            sql=self.select_bot_heartbeat_all_sql,
            api_file_name=self.mysql_api_file,
        )
        return {row["worker_id"]: row for row in rows}

    def _build_desired_scheduler_rows(
        self,
        now_local: datetime,
        now_naive: datetime,
    ) -> dict[str, dict[str, Any]]:
        desired: dict[str, dict[str, Any]] = {}
        is_weekend_block = self._is_within_weekend_window(now_local, self.weekend_window)

        for worker_id, worker_rules in self.worker_matrix.items():
            if worker_id == "GLOBAL":
                continue

            control_status, comment = self._resolve_control_state(
                worker_id=worker_id,
                worker_rules=worker_rules,
                now_local=now_local,
                is_weekend_block=is_weekend_block,
            )

            desired[worker_id] = {
                "worker_id": worker_id,
                "control_status": control_status,
                "last_update_ts": now_naive,
                "comment": comment,
            }

        return desired

    def _resolve_control_state(
        self,
        worker_id: str,
        worker_rules: dict[str, str],
        now_local: datetime,
        is_weekend_block: bool,
    ) -> tuple[str, str]:
        enabled = self._parse_bool(worker_rules.get("enabled", "1"))
        default_status = worker_rules.get("default_status", "RUNNING").strip().upper()
        weekend_mode = worker_rules.get("weekend_mode", default_status).strip().upper()

        self._validate_control_status(default_status, worker_id, "default_status")
        self._validate_control_status(weekend_mode, worker_id, "weekend_mode")

        if not enabled:
            return "STOP", "disabled_in_rules"

        if self._worker_has_daily_window(worker_rules):
            daily_window = self._parse_daily_window(
                worker_rules["daily_window_start"],
                worker_rules["daily_window_end"],
            )
            daily_window_status = worker_rules.get("daily_window_status", "RUNNING").strip().upper()
            daily_outside_status = worker_rules.get("daily_outside_status", default_status).strip().upper()

            self._validate_control_status(daily_window_status, worker_id, "daily_window_status")
            self._validate_control_status(daily_outside_status, worker_id, "daily_outside_status")

            if self._is_within_daily_window(now_local, daily_window):
                return daily_window_status, "daily_window"

            return daily_outside_status, "outside_daily_window"

        if is_weekend_block:
            return weekend_mode, "weekend_schedule"

        return default_status, "default_status"

    def _monitor_scheduler_vs_heartbeat(
        self,
        desired_rows: dict[str, dict[str, Any]],
        now_naive: datetime,
    ) -> None:
        heartbeat_rows = self._fetch_heartbeat_rows()

        for worker_id, desired_row in desired_rows.items():
            issue_type, issue_message = self._detect_worker_issue(
                worker_id=worker_id,
                desired_row=desired_row,
                heartbeat_row=heartbeat_rows.get(worker_id),
                now_naive=now_naive,
            )

            alert_key = worker_id

            if issue_type is None:
                self._resolve_alert_if_needed(
                    alert_key=alert_key,
                    worker_id=worker_id,
                    now_naive=now_naive,
                )
                continue

            self._register_or_send_alert(
                alert_key=alert_key,
                worker_id=worker_id,
                issue_type=issue_type,
                issue_message=issue_message,
                now_naive=now_naive,
            )

    def _detect_worker_issue(
        self,
        worker_id: str,
        desired_row: dict[str, Any],
        heartbeat_row: dict[str, Any] | None,
        now_naive: datetime,
    ) -> tuple[str | None, str | None]:
        expected_control = str(desired_row.get("control_status", "RUNNING")).upper()
        expected_runtime_statuses = self._expected_runtime_statuses(expected_control)

        if heartbeat_row is None:
            return (
                "missing_heartbeat",
                self._build_alert_message(
                    worker_id=worker_id,
                    desired_control=expected_control,
                    desired_comment=str(desired_row.get("comment") or ""),
                    runtime_status="MISSING",
                    runtime_comment="",
                    heartbeat_age_sec=None,
                    issue_type="missing_heartbeat",
                ),
            )

        runtime_status = str(heartbeat_row.get("runtime_status", "")).upper()
        runtime_comment = str(heartbeat_row.get("comment") or "")
        heartbeat_ts = heartbeat_row.get("last_update_ts")

        if heartbeat_ts is None or not isinstance(heartbeat_ts, datetime):
            return (
                "invalid_heartbeat_ts",
                self._build_alert_message(
                    worker_id=worker_id,
                    desired_control=expected_control,
                    desired_comment=str(desired_row.get("comment") or ""),
                    runtime_status=runtime_status or "UNKNOWN",
                    runtime_comment=runtime_comment,
                    heartbeat_age_sec=None,
                    issue_type="invalid_heartbeat_ts",
                ),
            )

        heartbeat_age_sec = max(0.0, (now_naive - heartbeat_ts).total_seconds())
        stale_threshold_sec = self._get_stale_threshold_sec(worker_id)

        if heartbeat_age_sec > stale_threshold_sec:
            return (
                "stale_heartbeat",
                self._build_alert_message(
                    worker_id=worker_id,
                    desired_control=expected_control,
                    desired_comment=str(desired_row.get("comment") or ""),
                    runtime_status=runtime_status,
                    runtime_comment=runtime_comment,
                    heartbeat_age_sec=heartbeat_age_sec,
                    issue_type="stale_heartbeat",
                ),
            )

        if runtime_status not in expected_runtime_statuses:
            return (
                "runtime_mismatch",
                self._build_alert_message(
                    worker_id=worker_id,
                    desired_control=expected_control,
                    desired_comment=str(desired_row.get("comment") or ""),
                    runtime_status=runtime_status,
                    runtime_comment=runtime_comment,
                    heartbeat_age_sec=heartbeat_age_sec,
                    issue_type="runtime_mismatch",
                ),
            )

        return None, None

    def _register_or_send_alert(
        self,
        alert_key: str,
        worker_id: str,
        issue_type: str,
        issue_message: str,
        now_naive: datetime,
    ) -> None:
        state = self.active_alerts.get(alert_key)

        if state is None or state["issue_type"] != issue_type:
            self.active_alerts[alert_key] = {
                "worker_id": worker_id,
                "issue_type": issue_type,
                "first_seen_ts": now_naive,
                "last_sent_ts": None,
            }
            return

        first_seen_ts = state["first_seen_ts"]
        issue_age_sec = max(0.0, (now_naive - first_seen_ts).total_seconds())
        stale_threshold_sec = self._get_stale_threshold_sec(worker_id)
        last_sent_ts = state["last_sent_ts"]

        if issue_age_sec < stale_threshold_sec:
            return

        if last_sent_ts is not None:
            resend_age_sec = max(0.0, (now_naive - last_sent_ts).total_seconds())
            if resend_age_sec < self.heartbeat_alert_resend_sec:
                return

        send_ok = send_tg_message(
            text=issue_message,
            api_file_name=self.telegram_api_file,
        )

        if send_ok:
            self.logger.warning("Heartbeat monitor alert sent | worker_id=%s | issue=%s", worker_id, issue_type)
            state["last_sent_ts"] = now_naive
        else:
            self.logger.error("Heartbeat monitor alert failed to send | worker_id=%s | issue=%s", worker_id, issue_type)

    def _resolve_alert_if_needed(
        self,
        alert_key: str,
        worker_id: str,
        now_naive: datetime,
    ) -> None:
        state = self.active_alerts.get(alert_key)
        if state is None:
            return

        last_sent_ts = state.get("last_sent_ts")
        issue_type = state.get("issue_type", "unknown")

        if self.heartbeat_send_recovery and last_sent_ts is not None:
            recovery_message = (
                "✅ Scheduler heartbeat recovery\n"
                f"worker={worker_id}\n"
                f"issue_cleared={issue_type}\n"
                f"time={now_naive.strftime('%Y-%m-%d %H:%M:%S')}"
            )

            send_ok = send_tg_message(
                text=recovery_message,
                api_file_name=self.telegram_api_file,
            )

            if send_ok:
                self.logger.info("Heartbeat monitor recovery sent | worker_id=%s | issue=%s", worker_id, issue_type)
            else:
                self.logger.error("Heartbeat monitor recovery failed to send | worker_id=%s | issue=%s", worker_id, issue_type)

        self.active_alerts.pop(alert_key, None)

    def _get_stale_threshold_sec(self, worker_id: str) -> int:
        worker_rules = self.worker_matrix.get(worker_id, {})
        heartbeat_sec = int(worker_rules.get("heartbeat_sec", self.loop_sec))
        return max(self.heartbeat_stale_min_sec, heartbeat_sec * self.heartbeat_stale_multiplier)

    @staticmethod
    def _expected_runtime_statuses(control_status: str) -> set[str]:
        control_status = str(control_status).upper()

        if control_status == "RUNNING":
            return {"RUNNING"}

        if control_status in {"SLEEP", "SL_BLOCK"}:
            return {"SLEEPING"}

        if control_status == "STOP":
            return {"SLEEPING", "STOPPED"}

        return {"RUNNING"}

    @staticmethod
    def _build_alert_message(
        worker_id: str,
        desired_control: str,
        desired_comment: str,
        runtime_status: str,
        runtime_comment: str,
        heartbeat_age_sec: float | None,
        issue_type: str,
    ) -> str:
        age_text = "unknown" if heartbeat_age_sec is None else f"{int(round(heartbeat_age_sec))}s"

        return (
            "⚠️ Scheduler / heartbeat mismatch\n"
            f"worker={worker_id}\n"
            f"issue={issue_type}\n"
            f"scheduler_status={desired_control}\n"
            f"scheduler_comment={desired_comment or '-'}\n"
            f"heartbeat_status={runtime_status}\n"
            f"heartbeat_comment={runtime_comment or '-'}\n"
            f"heartbeat_age={age_text}"
        )

    @staticmethod
    def _worker_has_daily_window(worker_rules: dict[str, str]) -> bool:
        return "daily_window_start" in worker_rules and "daily_window_end" in worker_rules

    @staticmethod
    def _rows_equal(current_row: dict[str, Any], desired_row: dict[str, Any]) -> bool:
        return (
            str(current_row.get("control_status", "")).upper() == str(desired_row["control_status"]).upper()
            and (current_row.get("comment") or "") == (desired_row["comment"] or "")
        )

    @staticmethod
    def _truncate_comment(comment: str | None, max_len: int = 64) -> str | None:
        if comment is None:
            return None
        return str(comment)[:max_len]

    @staticmethod
    def _load_rules_file(file_path: Path) -> dict[str, str]:
        if not file_path.exists():
            raise FileNotFoundError(f"Rules file not found: {file_path}")

        rules: dict[str, str] = {}

        for raw_line in file_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()

            if not line or line.startswith("#"):
                continue

            if "=" not in line:
                raise ValueError(f"Invalid rules line (missing '='): {line}")

            key, value = line.split("=", 1)
            rules[key.strip()] = value.strip()

        if not rules:
            raise ValueError(f"Rules file is empty: {file_path}")

        return rules

    @staticmethod
    def _build_worker_matrix(rules: dict[str, str]) -> dict[str, dict[str, str]]:
        matrix: dict[str, dict[str, str]] = {}

        for key, value in rules.items():
            if not key.startswith("worker."):
                continue

            parts = key.split(".")
            if len(parts) != 3:
                raise ValueError(f"Invalid worker rule key format: {key}")

            _, worker_id, field_name = parts
            matrix.setdefault(worker_id, {})
            matrix[worker_id][field_name] = value

        if not matrix:
            raise ValueError("No worker.* rules found in rules.txt")

        if "GLOBAL" not in matrix:
            raise ValueError("Missing required worker.GLOBAL.* rules")

        if "scheduler" not in matrix:
            raise ValueError("Missing required worker.scheduler.* rules")

        return matrix

    @staticmethod
    def _parse_bool(value: str) -> bool:
        normalized = str(value).strip().lower()
        if normalized in {"1", "true", "yes", "y"}:
            return True
        if normalized in {"0", "false", "no", "n"}:
            return False
        raise ValueError(f"Invalid boolean value: {value}")

    @staticmethod
    def _parse_positive_int(value: str, field_name: str) -> int:
        parsed = int(str(value).strip())
        if parsed <= 0:
            raise ValueError(f"{field_name} must be > 0, got {value}")
        return parsed

    @staticmethod
    def _validate_control_status(status: str, worker_id: str, field_name: str) -> None:
        if status not in VALID_CONTROL_STATUSES:
            raise ValueError(
                f"Invalid control status for worker_id={worker_id}, field={field_name}: {status}. "
                f"Allowed={sorted(VALID_CONTROL_STATUSES)}"
            )

    @staticmethod
    def _parse_weekend_window(start_value: str, end_value: str) -> WeekendWindow:
        start_weekday, start_hour, start_minute = SchedulerWorker._parse_day_time(start_value)
        end_weekday, end_hour, end_minute = SchedulerWorker._parse_day_time(end_value)

        return WeekendWindow(
            start_weekday=start_weekday,
            start_hour=start_hour,
            start_minute=start_minute,
            end_weekday=end_weekday,
            end_hour=end_hour,
            end_minute=end_minute,
        )

    @staticmethod
    def _parse_day_time(value: str) -> tuple[int, int, int]:
        day_map = {
            "MON": 0,
            "TUE": 1,
            "WED": 2,
            "THU": 3,
            "FRI": 4,
            "SAT": 5,
            "SUN": 6,
        }

        parts = value.strip().upper().split()
        if len(parts) != 2:
            raise ValueError(f"Invalid day-time format: {value}. Expected e.g. 'FRI 15:00'")

        day_part, time_part = parts
        if day_part not in day_map:
            raise ValueError(f"Invalid weekday in rules: {day_part}")

        hour_str, minute_str = time_part.split(":")
        hour = int(hour_str)
        minute = int(minute_str)

        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError(f"Invalid time in rules: {time_part}")

        return day_map[day_part], hour, minute

    @staticmethod
    def _parse_daily_window(start_value: str, end_value: str) -> DailyWindow:
        start_hour, start_minute = SchedulerWorker._parse_clock_time(start_value)
        end_hour, end_minute = SchedulerWorker._parse_clock_time(end_value)

        return DailyWindow(
            start_hour=start_hour,
            start_minute=start_minute,
            end_hour=end_hour,
            end_minute=end_minute,
        )

    @staticmethod
    def _parse_clock_time(value: str) -> tuple[int, int]:
        parts = value.strip().split(":")
        if len(parts) != 2:
            raise ValueError(f"Invalid clock time format: {value}. Expected HH:MM")

        hour = int(parts[0])
        minute = int(parts[1])

        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError(f"Invalid clock time: {value}")

        return hour, minute

    @staticmethod
    def _is_within_weekend_window(now_local: datetime, weekend_window: WeekendWindow) -> bool:
        current_minutes = now_local.weekday() * 1440 + now_local.hour * 60 + now_local.minute
        start_minutes = (
            weekend_window.start_weekday * 1440
            + weekend_window.start_hour * 60
            + weekend_window.start_minute
        )
        end_minutes = (
            weekend_window.end_weekday * 1440
            + weekend_window.end_hour * 60
            + weekend_window.end_minute
        )

        if start_minutes <= end_minutes:
            return start_minutes <= current_minutes < end_minutes

        return current_minutes >= start_minutes or current_minutes < end_minutes

    @staticmethod
    def _is_within_daily_window(now_local: datetime, daily_window: DailyWindow) -> bool:
        current_minutes = now_local.hour * 60 + now_local.minute
        start_minutes = daily_window.start_hour * 60 + daily_window.start_minute
        end_minutes = daily_window.end_hour * 60 + daily_window.end_minute

        if start_minutes <= end_minutes:
            return start_minutes <= current_minutes < end_minutes

        return current_minutes >= start_minutes or current_minutes < end_minutes


if __name__ == "__main__":
    worker = SchedulerWorker()
    worker.run_forever()