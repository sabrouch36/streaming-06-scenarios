"""Kafka consumer for FIFA World Cup 2026 completed-match analytics.

Consumes completed match messages from Kafka and runs a full pipeline:
- validates each match
- computes derived match fields
- calculates live group standings
- writes consumed matches and standings to CSV
- stores matches and standings in DuckDB
- updates and saves a standings chart

Author: Sabri Hamdaoui
Date: 2026-06

Run from the project root:

    uv run python -m streaming.kafka_consumer_sabri
"""

import csv
from datetime import date
import os
from pathlib import Path
from typing import Any, Final

from confluent_kafka.cimpl import OFFSET_BEGINNING, TopicPartition
from datafun_streaming.io.io_utils import append_csv_row
from datafun_streaming.kafka.kafka_admin_utils import (
    create_admin_client,
    get_topic_message_count,
    topic_exists,
)
from datafun_streaming.kafka.kafka_connection_utils import verify_kafka_connection
from datafun_streaming.kafka.kafka_consumer_utils import (
    consume_kafka_message,
    create_consumer,
)
from datafun_streaming.kafka.kafka_settings import KafkaSettings
from datafun_toolkit.logger import get_logger, log_header, log_path
from dotenv import load_dotenv
import duckdb
from matplotlib import pyplot as plt

from streaming.core.utils import log_env_vars

# === CONFIGURE LOGGER ===

LOG = get_logger("C06-WORLD-CUP", level="DEBUG")

# === LOAD ENVIRONMENT VARIABLES ===

load_dotenv(override=True)
log_env_vars(LOG)

# === DECLARE GLOBAL CONSTANTS ===

TIMEOUT_SECONDS: Final[float] = float(os.getenv("CONSUMER_TIMEOUT_SECONDS", "10.0"))
MAX_MESSAGES: Final[int] = int(os.getenv("CONSUMER_MAX_MESSAGES", "1000"))

# === DECLARE CONSTANT PATHS ===

ROOT_DIR: Final[Path] = Path.cwd()
DATA_DIR: Final[Path] = ROOT_DIR / "data"
OUTPUT_DIR: Final[Path] = DATA_DIR / "output"

OUTPUT_MATCHES_CSV: Final[Path] = OUTPUT_DIR / "consumed_world_cup_matches.csv"
OUTPUT_STANDINGS_CSV: Final[Path] = OUTPUT_DIR / "world_cup_team_standings.csv"
OUTPUT_DB: Final[Path] = OUTPUT_DIR / "world_cup.duckdb"
OUTPUT_CHART: Final[Path] = OUTPUT_DIR / "world_cup_team_standings.png"

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

CONSUMED_MATCH_FIELDNAMES: Final[list[str]] = [
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
    "scoreline",
    "winning_margin",
    "is_draw",
]

STANDINGS_FIELDNAMES: Final[list[str]] = [
    "group",
    "position",
    "team_code",
    "team",
    "played",
    "wins",
    "draws",
    "losses",
    "goals_for",
    "goals_against",
    "goal_difference",
    "points",
    "win_percentage",
]


# ==========================================================
# SECTION A. ACQUIRE RESOURCES AND GET READY
# ==========================================================


def log_paths() -> None:
    """Log run header and output paths."""
    log_header(LOG, "C06 WORLD CUP")
    LOG.info("========================")
    LOG.info("START consumer main()")
    LOG.info("========================")
    log_path(LOG, "ROOT_DIR", ROOT_DIR)
    log_path(LOG, "DATA_DIR", DATA_DIR)
    log_path(LOG, "OUTPUT_MATCHES_CSV", OUTPUT_MATCHES_CSV)
    log_path(LOG, "OUTPUT_STANDINGS_CSV", OUTPUT_STANDINGS_CSV)
    log_path(LOG, "OUTPUT_DB", OUTPUT_DB)
    log_path(LOG, "OUTPUT_CHART", OUTPUT_CHART)


def load_settings() -> KafkaSettings:
    """Load Kafka settings from .env."""
    LOG.info("Loading settings from .env...")
    settings = KafkaSettings.from_env()
    LOG.info(f"KAFKA_BOOTSTRAP_SERVERS  = {settings.bootstrap_servers}")
    LOG.info(f"KAFKA_TOPIC              = {settings.topic}")
    LOG.info(f"KAFKA_GROUP_ID           = {settings.group_id}")
    LOG.info(f"CONSUMER_TIMEOUT_SECONDS = {TIMEOUT_SECONDS}")
    LOG.info(f"CONSUMER_MAX_MESSAGES    = {MAX_MESSAGES}")
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


def verify_topic(settings: KafkaSettings) -> None:
    """Verify that the Kafka topic exists and contains messages."""
    LOG.info("Verifying Kafka topic...")
    admin = create_admin_client(settings)

    if not topic_exists(admin, settings.topic):
        LOG.error(f"Topic {settings.topic!r} does not exist.")
        LOG.error("Run the World Cup producer first.")
        raise SystemExit(1)

    message_count = get_topic_message_count(admin, settings.topic, settings)
    LOG.info(f"Topic {settings.topic!r} exists.")
    LOG.info(f"Found {message_count} message(s) available.")

    if message_count == 0:
        LOG.error("Topic is empty. Run the World Cup producer first.")
        raise SystemExit(1)


def get_kafka_consumer(settings: KafkaSettings) -> Any:
    """Create a consumer that reads the topic from the beginning."""
    LOG.info("Creating Kafka consumer...")
    consumer = create_consumer(settings)
    consumer.subscribe(
        [settings.topic],
        on_assign=lambda current_consumer, partitions: current_consumer.assign(
            [
                TopicPartition(
                    partition.topic,
                    partition.partition,
                    OFFSET_BEGINNING,
                )
                for partition in partitions
            ]
        ),
    )
    LOG.info(f"Subscribed to topic: {settings.topic!r} (reading from beginning)")
    return consumer


# ==========================================================
# SECTION S. STORAGE AND VISUALIZATION SETUP
# ==========================================================


def initialize_database() -> duckdb.DuckDBPyConnection:
    """Create a fresh DuckDB database and its tables."""
    if OUTPUT_DB.exists():
        OUTPUT_DB.unlink()

    connection = duckdb.connect(str(OUTPUT_DB))

    connection.execute(
        """
        CREATE TABLE matches (
            match_id VARCHAR PRIMARY KEY,
            match_date DATE,
            stage VARCHAR,
            matchday INTEGER,
            group_name VARCHAR,
            home_team_code VARCHAR,
            home_team VARCHAR,
            away_team_code VARCHAR,
            away_team VARCHAR,
            home_goals INTEGER,
            away_goals INTEGER,
            winner VARCHAR,
            result_type VARCHAR,
            total_goals INTEGER,
            home_points INTEGER,
            away_points INTEGER,
            fifa_venue_name VARCHAR,
            common_stadium_name VARCHAR,
            city VARCHAR,
            host_country VARCHAR,
            source_url VARCHAR,
            dataset_cutoff VARCHAR,
            scoreline VARCHAR,
            winning_margin INTEGER,
            is_draw BOOLEAN
        )
        """
    )

    connection.execute(
        """
        CREATE TABLE team_standings (
            group_name VARCHAR,
            position INTEGER,
            team_code VARCHAR,
            team VARCHAR,
            played INTEGER,
            wins INTEGER,
            draws INTEGER,
            losses INTEGER,
            goals_for INTEGER,
            goals_against INTEGER,
            goal_difference INTEGER,
            points INTEGER,
            win_percentage DOUBLE
        )
        """
    )

    return connection


def initialize_chart() -> tuple[Any, Any]:
    """Create the live standings chart."""
    plt.ion()
    figure, axis = plt.subplots(figsize=(11, 7))
    figure.tight_layout()
    return figure, axis


def initialize_output() -> tuple[duckdb.DuckDBPyConnection, Any, Any]:
    """Clear old output files and initialize storage resources."""
    LOG.info("Initializing output...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for path in (
        OUTPUT_MATCHES_CSV,
        OUTPUT_STANDINGS_CSV,
        OUTPUT_CHART,
    ):
        if path.exists():
            path.unlink()

    connection = initialize_database()
    figure, axis = initialize_chart()

    LOG.info("Output CSV files cleared.")
    LOG.info(f"Database initialized: {OUTPUT_DB.name}")
    LOG.info("Standings chart initialized.")
    return connection, figure, axis


# ==========================================================
# SECTION V. VALIDATE AND ENRICH MATCHES
# ==========================================================


def validate_match_record(row: dict[str, Any]) -> list[str]:
    """Return validation errors for one consumed match."""
    errors: list[str] = []

    for field in REQUIRED_MATCH_FIELDS:
        value = row.get(field)
        if value is None or str(value).strip() == "":
            errors.append(f"Missing required field: {field}")

    if errors:
        return errors

    try:
        date.fromisoformat(str(row["match_date"]))
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
            numeric_values[field] = int(str(row[field]))
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
        errors.append("total_goals does not match the score")

    if str(row["home_team_code"]) == str(row["away_team_code"]):
        errors.append("Home and away teams must be different")

    result_type = str(row["result_type"])
    winner = str(row["winner"])
    home_team = str(row["home_team"])
    away_team = str(row["away_team"])

    if home_goals > away_goals:
        if result_type != "home_win":
            errors.append("Expected result_type=home_win")
        if winner != home_team:
            errors.append("Winner does not match the home team")
        if (home_points, away_points) != (3, 0):
            errors.append("Home-win points must be 3 and 0")
    elif away_goals > home_goals:
        if result_type != "away_win":
            errors.append("Expected result_type=away_win")
        if winner != away_team:
            errors.append("Winner does not match the away team")
        if (home_points, away_points) != (0, 3):
            errors.append("Away-win points must be 0 and 3")
    else:
        if result_type != "draw":
            errors.append("Expected result_type=draw")
        if winner != "Draw":
            errors.append("Drawn matches must use winner=Draw")
        if (home_points, away_points) != (1, 1):
            errors.append("Draw points must be 1 and 1")

    return errors


def enrich_match(row: dict[str, Any]) -> dict[str, Any]:
    """Convert numeric values and add derived match fields."""
    enriched = dict(row)

    for field in (
        "matchday",
        "home_goals",
        "away_goals",
        "total_goals",
        "home_points",
        "away_points",
    ):
        enriched[field] = int(str(row[field]))

    enriched["scoreline"] = (
        f"{enriched['home_team']} {enriched['home_goals']}-"
        f"{enriched['away_goals']} {enriched['away_team']}"
    )
    enriched["winning_margin"] = abs(enriched["home_goals"] - enriched["away_goals"])
    enriched["is_draw"] = enriched["home_goals"] == enriched["away_goals"]

    return enriched


# ==========================================================
# SECTION T. TEAM STANDINGS ANALYTICS
# ==========================================================


def create_team_record(
    *,
    group: str,
    team_code: str,
    team: str,
) -> dict[str, Any]:
    """Create an empty team-statistics record."""
    return {
        "group": group,
        "team_code": team_code,
        "team": team,
        "played": 0,
        "wins": 0,
        "draws": 0,
        "losses": 0,
        "goals_for": 0,
        "goals_against": 0,
        "goal_difference": 0,
        "points": 0,
    }


def update_team_standings(
    standings: dict[str, dict[str, Any]],
    match: dict[str, Any],
) -> None:
    """Update both teams' cumulative statistics from one match."""
    group = str(match["group"])
    home_code = str(match["home_team_code"])
    away_code = str(match["away_team_code"])

    if home_code not in standings:
        standings[home_code] = create_team_record(
            group=group,
            team_code=home_code,
            team=str(match["home_team"]),
        )

    if away_code not in standings:
        standings[away_code] = create_team_record(
            group=group,
            team_code=away_code,
            team=str(match["away_team"]),
        )

    home = standings[home_code]
    away = standings[away_code]
    home_goals = int(match["home_goals"])
    away_goals = int(match["away_goals"])

    home["played"] += 1
    away["played"] += 1

    home["goals_for"] += home_goals
    home["goals_against"] += away_goals
    away["goals_for"] += away_goals
    away["goals_against"] += home_goals

    home["points"] += int(match["home_points"])
    away["points"] += int(match["away_points"])

    if home_goals > away_goals:
        home["wins"] += 1
        away["losses"] += 1
    elif away_goals > home_goals:
        away["wins"] += 1
        home["losses"] += 1
    else:
        home["draws"] += 1
        away["draws"] += 1

    home["goal_difference"] = home["goals_for"] - home["goals_against"]
    away["goal_difference"] = away["goals_for"] - away["goals_against"]


def get_ranked_standings(
    standings: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Return teams ranked within each World Cup group."""
    ranked_rows: list[dict[str, Any]] = []

    groups = sorted({str(record["group"]) for record in standings.values()})

    for group in groups:
        group_teams = [
            record.copy() for record in standings.values() if record["group"] == group
        ]
        group_teams.sort(
            key=lambda record: (
                -int(record["points"]),
                -int(record["goal_difference"]),
                -int(record["goals_for"]),
                str(record["team"]),
            )
        )

        for position, record in enumerate(group_teams, start=1):
            played = int(record["played"])
            wins = int(record["wins"])
            record["position"] = position
            record["win_percentage"] = (
                round((wins / played) * 100, 2) if played else 0.0
            )
            ranked_rows.append(record)

    return ranked_rows


def write_standings_csv(rows: list[dict[str, Any]]) -> None:
    """Replace the standings CSV with the latest rankings."""
    with OUTPUT_STANDINGS_CSV.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=STANDINGS_FIELDNAMES,
        )
        writer.writeheader()
        writer.writerows(
            [
                {field: row.get(field, "") for field in STANDINGS_FIELDNAMES}
                for row in rows
            ]
        )


def replace_standings_table(
    connection: duckdb.DuckDBPyConnection,
    rows: list[dict[str, Any]],
) -> None:
    """Replace DuckDB standings with the latest rankings."""
    connection.execute("DELETE FROM team_standings")

    values = [
        (
            row["group"],
            row["position"],
            row["team_code"],
            row["team"],
            row["played"],
            row["wins"],
            row["draws"],
            row["losses"],
            row["goals_for"],
            row["goals_against"],
            row["goal_difference"],
            row["points"],
            row["win_percentage"],
        )
        for row in rows
    ]

    if values:
        connection.executemany(
            """
            INSERT INTO team_standings VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            values,
        )


def update_standings_chart(
    figure: Any,
    axis: Any,
    rows: list[dict[str, Any]],
) -> None:
    """Update a chart showing the current top 12 teams."""
    overall = sorted(
        rows,
        key=lambda row: (
            int(row["points"]),
            int(row["goal_difference"]),
            int(row["goals_for"]),
        ),
        reverse=True,
    )[:12]

    overall.reverse()

    labels = [f"{row['team']} ({row['group']})" for row in overall]
    points = [int(row["points"]) for row in overall]

    axis.clear()
    axis.barh(labels, points)
    axis.set_title("FIFA World Cup 2026 — Current Top Teams")
    axis.set_xlabel("Points")
    axis.set_ylabel("Team (Group)")
    axis.grid(axis="x", alpha=0.3)

    for index, value in enumerate(points):
        axis.text(value + 0.05, index, str(value), va="center")

    figure.tight_layout()
    figure.canvas.draw()
    figure.canvas.flush_events()
    plt.pause(0.01)


# ==========================================================
# SECTION D. DATABASE MATCH STORAGE
# ==========================================================


def write_match_to_database(
    connection: duckdb.DuckDBPyConnection,
    match: dict[str, Any],
) -> None:
    """Insert one processed match into DuckDB."""
    connection.execute(
        """
        INSERT INTO matches VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?
        )
        """,
        [
            match["match_id"],
            match["match_date"],
            match["stage"],
            match["matchday"],
            match["group"],
            match["home_team_code"],
            match["home_team"],
            match["away_team_code"],
            match["away_team"],
            match["home_goals"],
            match["away_goals"],
            match["winner"],
            match["result_type"],
            match["total_goals"],
            match["home_points"],
            match["away_points"],
            match["fifa_venue_name"],
            match.get("common_stadium_name", ""),
            match["city"],
            match["host_country"],
            match.get("source_url", ""),
            match.get("dataset_cutoff", ""),
            match["scoreline"],
            match["winning_margin"],
            match["is_draw"],
        ],
    )


# ==========================================================
# SECTION C. CONSUME AND PROCESS MESSAGES
# ==========================================================


def consume_messages(
    consumer: Any,
    *,
    connection: duckdb.DuckDBPyConnection,
    figure: Any,
    axis: Any,
) -> tuple[int, int, dict[str, int], list[dict[str, Any]]]:
    """Consume, validate, analyze, visualize, and store matches."""
    LOG.info("Consuming World Cup match messages...")
    LOG.info(f"Waiting for up to {MAX_MESSAGES} message(s).")
    LOG.info("Press CTRL+C to stop early.\n")

    consumed_count = 0
    skipped_count = 0
    seen_match_ids: set[str] = set()
    standings: dict[str, dict[str, Any]] = {}

    summary = {
        "total_goals": 0,
        "draws": 0,
        "home_wins": 0,
        "away_wins": 0,
    }

    latest_rankings: list[dict[str, Any]] = []

    while consumed_count + skipped_count < MAX_MESSAGES:
        row = consume_kafka_message(
            consumer=consumer,
            timeout_seconds=TIMEOUT_SECONDS,
        )

        if row is None:
            LOG.info(f"No message received within {TIMEOUT_SECONDS}s timeout.")
            LOG.info("Producer finished or paused. Stopping consumer.")
            break

        match_id = str(row.get("match_id", "")).strip()
        errors = validate_match_record(row)

        if match_id in seen_match_ids:
            errors.append(f"Duplicate match_id: {match_id}")

        if errors:
            skipped_count += 1
            LOG.warning("MATCH MESSAGE REJECTED")
            LOG.warning(f"match_id={match_id or '?'}")
            LOG.warning(f"errors={errors}")
            LOG.warning(f"skipped={skipped_count}")
            continue

        match = enrich_match(row)
        seen_match_ids.add(match_id)

        append_csv_row(
            path=OUTPUT_MATCHES_CSV,
            row={field: match.get(field, "") for field in CONSUMED_MATCH_FIELDNAMES},
            fieldnames=CONSUMED_MATCH_FIELDNAMES,
        )

        write_match_to_database(connection, match)
        update_team_standings(standings, match)

        latest_rankings = get_ranked_standings(standings)
        write_standings_csv(latest_rankings)
        replace_standings_table(connection, latest_rankings)
        update_standings_chart(figure, axis, latest_rankings)

        summary["total_goals"] += int(match["total_goals"])
        if match["result_type"] == "draw":
            summary["draws"] += 1
        elif match["result_type"] == "home_win":
            summary["home_wins"] += 1
        else:
            summary["away_wins"] += 1

        consumed_count += 1

        LOG.info("MATCH MESSAGE ACCEPTED")
        LOG.info(f"match={match['scoreline']}")
        LOG.info(f"group={match['group']}")
        LOG.info(f"winner={match['winner']}")
        LOG.info(f"total_goals={match['total_goals']}")
        LOG.info(f"consumed={consumed_count}")

    return consumed_count, skipped_count, summary, latest_rankings


def save_artifacts(figure: Any) -> None:
    """Save the final standings chart and log all output paths."""
    LOG.info("Saving output artifacts...")
    figure.savefig(OUTPUT_CHART, dpi=150, bbox_inches="tight")

    log_path(LOG, "WROTE OUTPUT_MATCHES_CSV", OUTPUT_MATCHES_CSV)
    log_path(LOG, "WROTE OUTPUT_STANDINGS_CSV", OUTPUT_STANDINGS_CSV)
    log_path(LOG, "WROTE OUTPUT_DB", OUTPUT_DB)
    log_path(LOG, "WROTE OUTPUT_CHART", OUTPUT_CHART)


# ==========================================================
# SECTION E. EXIT AND SUMMARY
# ==========================================================


def log_group_leaders(rows: list[dict[str, Any]]) -> None:
    """Log the current first-place team in each group."""
    leaders = [row for row in rows if int(row["position"]) == 1]

    if not leaders:
        return

    LOG.info("CURRENT GROUP LEADERS")
    for leader in leaders:
        LOG.info(
            f"Group {leader['group']}: {leader['team']} — "
            f"{leader['points']} point(s), "
            f"GD {leader['goal_difference']:+d}"
        )


def log_summary(
    consumed_count: int,
    skipped_count: int,
    summary: dict[str, int],
    rankings: list[dict[str, Any]],
    settings: KafkaSettings,
) -> None:
    """Log final World Cup streaming statistics."""
    LOG.info("Summary:")
    LOG.info(f"Consumed {consumed_count} match message(s).")
    LOG.info(f"Skipped  {skipped_count} match message(s).")
    LOG.info(f"Topic: {settings.topic!r}")

    if consumed_count > 0:
        average_goals = summary["total_goals"] / consumed_count
        LOG.info(f"Total goals:   {summary['total_goals']}")
        LOG.info(f"Average goals: {average_goals:.2f} per match")
        LOG.info(f"Home wins:     {summary['home_wins']}")
        LOG.info(f"Away wins:     {summary['away_wins']}")
        LOG.info(f"Draws:         {summary['draws']}")
        log_group_leaders(rankings)

    LOG.info("========================")
    LOG.info("World Cup consumer executed successfully!")
    LOG.info("========================")


def main() -> None:
    """Run the World Cup Kafka consumer."""
    log_paths()

    LOG.info("========================")
    LOG.info("SECTION A. Acquire")
    LOG.info("========================")

    settings = load_settings()
    verify_connection(settings)
    verify_topic(settings)
    consumer = get_kafka_consumer(settings)

    LOG.info("========================")
    LOG.info("SECTION C. Consume and Process Messages")
    LOG.info("========================")

    connection, figure, axis = initialize_output()

    consumed_count = 0
    skipped_count = 0
    summary = {
        "total_goals": 0,
        "draws": 0,
        "home_wins": 0,
        "away_wins": 0,
    }
    rankings: list[dict[str, Any]] = []

    try:
        try:
            (
                consumed_count,
                skipped_count,
                summary,
                rankings,
            ) = consume_messages(
                consumer,
                connection=connection,
                figure=figure,
                axis=axis,
            )
        finally:
            consumer.close()
            LOG.info("Kafka consumer closed.")

        save_artifacts(figure)

    finally:
        connection.close()
        plt.close(figure)
        plt.ioff()
        LOG.info("Database and chart resources closed.")

    LOG.info("========================")
    LOG.info("SECTION E. Exit")
    LOG.info("========================")

    log_summary(
        consumed_count,
        skipped_count,
        summary,
        rankings,
        settings,
    )


if __name__ == "__main__":
    main()
