"""Kafka producer for FIFA World Cup 2026 completed-match data.

Reads completed matches from data/world_cup_matches.csv, validates each
record, writes rejected records to a local CSV file, and sends valid
matches to Kafka one message at a time.

Author: Sabri Hamdaoui
Date: 2026-06

Run from the project root:

    uv run python -m streaming.kafka_producer_sabri
"""

from collections.abc import Generator
from datetime import date
import os
from pathlib import Path
import time
from typing import Any, Final

from datafun_streaming.core.types import DataRecordDict
from datafun_streaming.io.errors import missing_csv_field_message
from datafun_streaming.io.io_utils import (
    append_csv_row,
    format_message_for_log,
    read_csv_rows,
)
from datafun_streaming.kafka.kafka_connection_utils import verify_kafka_connection
from datafun_streaming.kafka.kafka_producer_utils import (
    create_producer,
    prepare_producer_topic,
    produce_kafka_message,
)
from datafun_streaming.kafka.kafka_settings import KafkaSettings
from datafun_toolkit.logger import get_logger, log_header, log_path
from dotenv import load_dotenv

from streaming.core.utils import log_env_vars

# === CONFIGURE LOGGER ===

LOG = get_logger("P06-WORLD-CUP", level="DEBUG")

# === LOAD ENVIRONMENT VARIABLES ===

load_dotenv(override=True)
log_env_vars(LOG)

# === DECLARE GLOBAL CONSTANTS ===

msg_count = os.getenv("PRODUCER_MESSAGE_COUNT", "29")
msg_interval_seconds = os.getenv("PRODUCER_MESSAGE_INTERVAL_SECONDS", "2.0")

MESSAGE_COUNT: Final[int] = int(msg_count)
MESSAGE_INTERVAL_SECONDS: Final[float] = float(msg_interval_seconds)

# === DECLARE CONSTANT PATHS ===

ROOT_DIR: Final[Path] = Path.cwd()
DATA_DIR: Final[Path] = ROOT_DIR / "data"
OUTPUT_DIR: Final[Path] = DATA_DIR / "output"

WORLD_CUP_MATCHES_CSV: Final[Path] = DATA_DIR / "world_cup_matches.csv"
REJECTED_MATCHES_CSV: Final[Path] = (
    OUTPUT_DIR / "producer_rejected_world_cup_matches.csv"
)

REQUIRED_MATCH_FIELDS: Final[tuple[str, ...]] = (
    "match_id",
    "match_date",
    "stage",
    "matchday",
    "group",
    "home_team_code",
    "home_team",
    "away_team_code",
    "away_team",
    "home_goals",
    "away_goals",
    "winner",
    "result_type",
    "total_goals",
    "home_points",
    "away_points",
    "fifa_venue_name",
    "city",
    "host_country",
)

REJECTED_MATCH_FIELDNAMES: Final[list[str]] = [
    "match_id",
    "match_date",
    "stage",
    "matchday",
    "group",
    "home_team_code",
    "home_team",
    "away_team_code",
    "away_team",
    "home_goals",
    "away_goals",
    "winner",
    "result_type",
    "total_goals",
    "home_points",
    "away_points",
    "fifa_venue_name",
    "common_stadium_name",
    "city",
    "host_country",
    "source_url",
    "dataset_cutoff",
    "validation_errors",
]


# ==========================================================
# SECTION A. ACQUIRE RESOURCES AND GET READY
# ==========================================================


def log_paths() -> None:
    """Log run header and important project paths."""
    log_header(LOG, "P06 WORLD CUP")
    LOG.info("========================")
    LOG.info("START producer main()")
    LOG.info("========================")
    log_path(LOG, "ROOT_DIR", ROOT_DIR)
    log_path(LOG, "DATA_DIR", DATA_DIR)
    log_path(LOG, "WORLD_CUP_MATCHES_CSV", WORLD_CUP_MATCHES_CSV)
    log_path(LOG, "REJECTED_MATCHES_CSV", REJECTED_MATCHES_CSV)


def load_settings() -> KafkaSettings:
    """Load Kafka settings from the .env file."""
    LOG.info("Loading settings from .env...")
    settings = KafkaSettings.from_env()
    LOG.info(f"KAFKA_BOOTSTRAP_SERVERS           = {settings.bootstrap_servers}")
    LOG.info(f"KAFKA_TOPIC                       = {settings.topic}")
    LOG.info(f"PRODUCER_MESSAGE_COUNT            = {MESSAGE_COUNT}")
    LOG.info(f"PRODUCER_MESSAGE_INTERVAL_SECONDS = {MESSAGE_INTERVAL_SECONDS}")
    LOG.info(f"KAFKA_CLEAR_TOPIC_ON_START        = {settings.clear_topic_on_start}")
    return settings


def verify_connection(settings: KafkaSettings) -> None:
    """Verify that Kafka is reachable."""
    LOG.info("Verifying Kafka connection...")
    try:
        verify_kafka_connection(settings)
        LOG.info("Kafka port is reachable.")
    except ConnectionError as error:
        LOG.error(str(error))
        raise SystemExit(1) from error


def initialize_output() -> None:
    """Create the output directory and clear old rejected records."""
    LOG.info("Initializing output...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    if REJECTED_MATCHES_CSV.exists():
        REJECTED_MATCHES_CSV.unlink()
    LOG.info(f"Output directory ready: {OUTPUT_DIR.name}")


# ==========================================================
# SECTION V. VALIDATE WORLD CUP MATCH RECORDS
# ==========================================================


def validate_match_record(record: DataRecordDict) -> list[str]:
    """Return validation errors for one World Cup match record."""
    errors: list[str] = []

    for field in REQUIRED_MATCH_FIELDS:
        value = record.get(field)
        if value is None or str(value).strip() == "":
            errors.append(f"Missing required field: {field}")

    if errors:
        return errors

    try:
        date.fromisoformat(str(record["match_date"]))
    except ValueError:
        errors.append("match_date must use YYYY-MM-DD format")

    numeric_fields = (
        "matchday",
        "home_goals",
        "away_goals",
        "total_goals",
        "home_points",
        "away_points",
    )
    numeric_values: dict[str, int] = {}

    for field in numeric_fields:
        try:
            numeric_values[field] = int(str(record[field]))
        except ValueError:
            errors.append(f"{field} must be an integer")

    if len(numeric_values) != len(numeric_fields):
        return errors

    home_goals = numeric_values["home_goals"]
    away_goals = numeric_values["away_goals"]
    total_goals = numeric_values["total_goals"]
    home_points = numeric_values["home_points"]
    away_points = numeric_values["away_points"]

    if home_goals < 0 or away_goals < 0:
        errors.append("Goal values cannot be negative")

    if total_goals != home_goals + away_goals:
        errors.append("total_goals must equal home_goals + away_goals")

    if str(record["home_team_code"]) == str(record["away_team_code"]):
        errors.append("Home and away team codes must be different")

    result_type = str(record["result_type"])
    winner = str(record["winner"])
    home_team = str(record["home_team"])
    away_team = str(record["away_team"])

    if home_goals > away_goals:
        if result_type != "home_win":
            errors.append("result_type must be home_win")
        if winner != home_team:
            errors.append("winner must match the home team")
        if (home_points, away_points) != (3, 0):
            errors.append("Home win points must be 3 and 0")
    elif away_goals > home_goals:
        if result_type != "away_win":
            errors.append("result_type must be away_win")
        if winner != away_team:
            errors.append("winner must match the away team")
        if (home_points, away_points) != (0, 3):
            errors.append("Away win points must be 0 and 3")
    else:
        if result_type != "draw":
            errors.append("result_type must be draw")
        if winner != "Draw":
            errors.append("winner must be Draw")
        if (home_points, away_points) != (1, 1):
            errors.append("Draw points must be 1 and 1")

    return errors


# ==========================================================
# SECTION P. PRODUCE MESSAGES
# ==========================================================


def get_message_key(message: dict[str, Any]) -> str:
    """Use the World Cup group as the Kafka message key."""
    try:
        return str(message["group"])
    except KeyError as error:
        msg = missing_csv_field_message(
            field="group",
            available_fields=list(message.keys()),
        )
        raise KeyError(msg) from error


def generate_messages(count: int) -> Generator[dict[str, str]]:
    """Yield completed World Cup matches one record at a time."""
    match_rows = read_csv_rows(WORLD_CUP_MATCHES_CSV)
    yield from match_rows[:count]


def write_rejected_record(record: DataRecordDict, errors: list[str]) -> None:
    """Write one rejected match record to CSV."""
    rejected_record = dict(record)
    rejected_record["validation_errors"] = " | ".join(errors)

    append_csv_row(
        path=REJECTED_MATCHES_CSV,
        row=rejected_record,
        fieldnames=REJECTED_MATCH_FIELDNAMES,
    )


def send_messages(
    producer: Any,
    settings: KafkaSettings,
) -> tuple[int, int]:
    """Validate and send completed World Cup matches to Kafka."""
    LOG.info("Sending World Cup match messages...")
    LOG.info(f"Sending up to {MESSAGE_COUNT} message(s) to topic {settings.topic!r}.")
    LOG.info("Watch each completed match arrive. Press CTRL+C to stop early.\n")

    sent_count = 0
    rejected_count = 0
    seen_match_ids: set[str] = set()

    try:
        for message in generate_messages(MESSAGE_COUNT):
            LOG.info(format_message_for_log(message))

            match_id = str(message.get("match_id", "")).strip()
            errors = validate_match_record(message)

            if match_id in seen_match_ids:
                errors.append(f"Duplicate match_id: {match_id}")

            if errors:
                rejected_count += 1
                LOG.warning("MATCH MESSAGE REJECTED")
                LOG.warning(f"  errors={errors}")
                write_rejected_record(message, errors)
                continue

            seen_match_ids.add(match_id)

            key = get_message_key(message)
            LOG.info(f"  Sending match_id={match_id} with group key={key}")

            produce_kafka_message(
                producer=producer,
                topic=settings.topic,
                key=key,
                message=message,
            )

            sent_count += 1
            LOG.info(f"  MATCH MESSAGE SENT  sent={sent_count}")
            time.sleep(MESSAGE_INTERVAL_SECONDS)

    except (FileNotFoundError, KeyError, RuntimeError, ValueError) as error:
        LOG.error(str(error))
        LOG.error("Producer stopped before completing all messages.")
        raise SystemExit(1) from error

    return sent_count, rejected_count


def log_rejected(rejected_count: int) -> None:
    """Log the rejected-record file when needed."""
    LOG.info("Checking for rejected records...")
    if rejected_count > 0:
        log_path(LOG, "WROTE REJECTED_MATCHES_CSV", REJECTED_MATCHES_CSV)
    else:
        LOG.info("No records rejected.")


# ==========================================================
# SECTION E. EXIT AND CLEANUP
# ==========================================================


def log_summary(
    sent_count: int,
    rejected_count: int,
    settings: KafkaSettings,
) -> None:
    """Log the producer's final summary."""
    LOG.info("Summary:")
    LOG.info(f"Sent {sent_count} completed match message(s).")
    LOG.info(f"Rejected {rejected_count} match message(s).")
    LOG.info(f"Kafka topic: {settings.topic!r}")
    LOG.info("========================")
    LOG.info("World Cup producer executed successfully!")
    LOG.info("========================")


def main() -> None:
    """Run the World Cup Kafka producer."""
    log_paths()

    LOG.info("========================")
    LOG.info("SECTION A. Acquire")
    LOG.info("========================")

    settings = load_settings()
    verify_connection(settings)
    prepare_producer_topic(settings)
    producer = create_producer(settings)

    LOG.info("========================")
    LOG.info("SECTION P. Produce Messages")
    LOG.info("========================")

    initialize_output()
    sent_count, rejected_count = send_messages(producer, settings)
    log_rejected(rejected_count)

    LOG.info("========================")
    LOG.info("SECTION E. Exit")
    LOG.info("========================")

    producer.flush()
    log_summary(sent_count, rejected_count, settings)


if __name__ == "__main__":
    main()
