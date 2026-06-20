# FIFA World Cup 2026 Streaming Analytics

[![Documentation](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://sabrouch36.github.io/streaming-06-scenarios/)
[![API Reference](https://img.shields.io/badge/API--Utils-datafun--streaming-purple)](https://denisecase.github.io/datafun-streaming/api/)
[![Workflow Guide](https://img.shields.io/badge/Pro--Guide-pro--analytics--02-green)](https://denisecase.github.io/pro-analytics-02/workflow-b-apply-example-project/)
[![Python 3.14](https://img.shields.io/badge/python-3.14%2B-blue?logo=python)](./pyproject.toml)
[![MIT License](https://img.shields.io/badge/license-MIT-yellow.svg)](./LICENSE)

> A complete Kafka streaming analytics pipeline for completed FIFA World Cup
> 2026 matches.

## Project Overview

This project applies the full streaming analytics workflow to international
football match data.

The original example processed sales transactions. This custom version replaces
the sales dataset with completed FIFA World Cup 2026 match results.

The Kafka producer reads one completed match at a time from a CSV file,
validates it, and sends it to a Kafka topic. The consumer validates each
message, calculates match and team statistics, updates group standings,
generates a chart, writes CSV output, and stores the results in DuckDB.

```text
World Cup CSV
      ↓
Kafka Producer
      ↓
Kafka Topic
      ↓
Kafka Consumer
      ↓
Validation and Analytics
      ↓
CSV + DuckDB + Visualization
```

## Features

The project includes:

* Kafka message production and consumption
* match-record validation
* duplicate match detection
* derived match fields
* wins, draws, and losses
* goals scored and conceded
* goal difference
* points and win percentage
* live group standings
* CSV output
* DuckDB storage
* standings visualization

## Dataset

The producer reads:

```text
data/world_cup_matches.csv
```

The dataset contains 29 completed FIFA World Cup 2026 matches available at the
dataset cutoff time.

Each row represents one completed match and includes:

* match ID and date
* tournament stage and matchday
* group
* home and away teams
* team codes
* goals
* winner and result type
* points awarded
* stadium
* city and host country
* source URL
* dataset cutoff information

The dataset is a fixed snapshot so the project can be reproduced consistently.

## Custom Kafka Producer

The custom producer is:

```text
src/streaming/kafka_producer_sabri.py
```

The producer:

1. reads completed matches from the CSV file
2. validates required fields
3. validates dates, goals, results, winners, and points
4. checks for duplicate match IDs
5. sends valid matches to Kafka
6. writes invalid matches to a rejected-records CSV

The Kafka topic is:

```text
streaming-06-scenarios-case
```

The Kafka message key is the World Cup group letter.

Examples:

```text
A
B
J
```

Using the group as the key logically associates matches from the same group.

Run the producer with:

```shell
uv run python -m streaming.kafka_producer_sabri
```

Successful producer result:

```text
Sent 29 completed match message(s).
Rejected 0 match message(s).
World Cup producer executed successfully!
```

## Custom Kafka Consumer

The custom consumer is:

```text
src/streaming/kafka_consumer_sabri.py
```

For every valid match message, the consumer:

1. validates the match
2. converts numeric fields
3. creates a readable scoreline
4. calculates the winning margin
5. identifies draws
6. updates both teams' statistics
7. recalculates group standings
8. updates the visualization
9. writes the match to CSV
10. writes matches and standings to DuckDB

The consumer calculates:

* matches played
* wins
* draws
* losses
* goals for
* goals against
* goal difference
* points
* win percentage
* group position

Run the consumer with:

```shell
uv run python -m streaming.kafka_consumer_sabri
```

## Results

The completed pipeline processed:

| Metric              | Result |
| ------------------- | -----: |
| Matches produced    |     29 |
| Producer rejections |      0 |
| Matches consumed    |     29 |
| Consumer skips      |      0 |
| National teams      |     48 |
| Groups              |     12 |

Selected findings:

| Finding                      | Result |
| ---------------------------- | -----: |
| Mexico points                |      6 |
| United States points         |      6 |
| Canada goals scored          |      7 |
| Germany goals scored         |      7 |
| Canada goal difference       |     +6 |
| Germany goal difference      |     +6 |
| Mexico win percentage        |   100% |
| United States win percentage |   100% |

Mexico led Group A after winning its first two matches. The United States led
Group D with six points. Canada and Switzerland both earned four points in
Group B, with Canada ranked first because of its stronger goal difference.

## Visualization

The consumer generates a chart showing the current leading teams.

![FIFA World Cup 2026 team standings](docs/images/world_cup_team_standings.png)

## Output Files

The project generates:

```text
data/output/consumed_world_cup_matches.csv
data/output/world_cup_team_standings.csv
data/output/world_cup.duckdb
data/output/world_cup_team_standings.png
```

The DuckDB database contains:

```text
matches
team_standings
```

## Project Structure

```text
streaming-06-scenarios/
├── data/
│   ├── world_cup_matches.csv
│   └── output/
│       ├── consumed_world_cup_matches.csv
│       ├── world_cup_team_standings.csv
│       ├── world_cup_team_standings.png
│       └── world_cup.duckdb
├── docs/
│   ├── images/
│   │   └── world_cup_team_standings.png
│   └── index.md
├── src/
│   └── streaming/
│       ├── kafka_producer_sabri.py
│       └── kafka_consumer_sabri.py
├── tests/
├── pyproject.toml
├── zensical.toml
└── README.md
```

## Setup

Clone the repository:

```shell
git clone https://github.com/sabrouch36/streaming-06-scenarios.git
cd streaming-06-scenarios
code .
```

Install the project dependencies:

```shell
uv self update
uv python pin 3.14
uv sync --extra dev --extra docs --upgrade
```

Copy the environment example if needed:

```shell
Copy-Item .env.example .env
```

Important `.env` values include:

```text
KAFKA_TOPIC=streaming-06-scenarios-case
PRODUCER_MESSAGE_COUNT=29
PRODUCER_MESSAGE_INTERVAL_SECONDS=2
KAFKA_AUTO_OFFSET_RESET=earliest
```

## Start Kafka

Use a WSL terminal.

Verify Java:

```bash
echo "$JAVA_HOME"
"$JAVA_HOME/bin/java" --version
```

Rebuild the Kafka cluster metadata when starting fresh:

```bash
cd ~/kafka
rm -rf /tmp/kraft-combined-logs
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format --standalone \
  -t "$KAFKA_CLUSTER_ID" \
  -c config/server.properties
```

Start Kafka and leave the terminal running:

```bash
cd ~/kafka
bin/kafka-server-start.sh config/server.properties
```

## Create the Kafka Topic

Open another WSL terminal:

```bash
cd ~/kafka

bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1 \
  --topic streaming-06-scenarios-case
```

Verify the topic:

```bash
bin/kafka-topics.sh --list \
  --bootstrap-server localhost:9092
```

## Run the Pipeline

Use a PowerShell terminal from the project root.

Run the producer:

```shell
uv run python -m streaming.kafka_producer_sabri
```

Run the consumer:

```shell
uv run python -m streaming.kafka_consumer_sabri
```

## Quality Checks

Run formatting and validation:

```shell
uv run ruff format .
uv run ruff check . --fix
uv run python -m pyright
uv run python -m pytest
uv run python -m zensical build
```

Run pre-commit checks:

```shell
git add -A
uvx pre-commit run --all-files
```

Repeat the pre-commit command if it modifies files.

## Build the Documentation

Build the Zensical documentation site:

```shell
uv run python -m zensical build
```

The generated site is written to:

```text
site/
```

Hosted documentation:

[Project Documentation](https://sabrouch36.github.io/streaming-06-scenarios/)

## Save the Work

```shell
git add -A
git commit -m "Complete World Cup streaming analytics project"
git push -u origin main
```

## Custom Modification

The original sales example calculated transaction totals and detected
high-value orders.

My custom modification replaces that logic with football-match validation and
live tournament analytics.

The producer validates the relationship between:

* goals
* winner
* result type
* total goals
* points

The consumer updates the standings after every match and ranks teams using:

1. points
2. goal difference
3. goals scored
4. team name as a deterministic final tie breaker

This modification demonstrates how Kafka can transform individual sporting
events into continuously updated tournament intelligence.

## Documentation

Additional project details, experiments, results, and interpretation are
available in the hosted documentation:

[View the complete project documentation](https://sabrouch36.github.io/streaming-06-scenarios/)
