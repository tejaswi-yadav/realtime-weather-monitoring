# Real-Time Weather Monitoring

Small Go pipeline: pull OpenWeather forecasts, fan out readings on Kafka, scrape metrics with Prometheus, fire rules through Alertmanager, and chart things in Grafana. Meant to run on one machine with Docker Compose.

Three binaries in one image (`weather-app`, `weather-consumer`, `weather-evaluator`). Kafka uses seven topics (`weather-forecasts`, `temperature-data`, `humidity-data`, `wind-data`, `pressure-data`, `weather-conditions`, `alert-config`). Compose brings up nine containers including Zookeeper, Kafka UI, and Grafana.

## What you need

- Docker with Compose
- OpenWeather API key (free tier is fine for trying it out)

## Get it running

From the repo root (same folder as `docker-compose.yml`):

```bash
cp env.example .env
```

Put your key in `.env`:

```
OPENWEATHER_API_KEY=...
```

Compose expects a local image tag. Build it once:

```bash
docker build -t weather-monitoring:minimal .
docker compose up -d
```

Give Kafka a minute to settle, then drive the producer:

```bash
docker exec -it weather-app ./weather-app
```

It asks for US ZIP codes (5 digits, comma-separated), how many forecast days you want, and temperature/humidity/wind/pressure thresholds for alerts.

**Windows:** use the same commands in PowerShell; skip `start-stack.sh` unless you are on WSL/Git Bash. That script also assumes you already built the image.

## URLs worth bookmarking

- Grafana: http://localhost:3000 (`admin` / `admin` out of the box)
- Prometheus: http://localhost:9090
- Alertmanager: http://localhost:9093
- Kafka UI: http://localhost:8081 (not 8080 — consumer uses 8080)
- Metrics: http://localhost:8080/metrics
- Health: http://localhost:8080/health

Email alerts only matter if you fill in `GMAIL_ADDRESS`, `GMAIL_APP_PASSWORD`, and `ALERT_EMAIL_TO` in `.env` and wire receivers in `alertmanager.yml`.

## Without Docker

You need Go 1.23+, Kafka running somewhere, and the same env vars as `env.example`.

```bash
go mod download
go run main.go
go run consumer.go
go run evaluator.go
```

I mostly develop against Compose; standing up Kafka by hand is more work than it is worth for a quick demo.

## When something breaks

If `docker compose up` complains about `weather-monitoring:minimal`, run the `docker build` line above.

Empty Grafana usually means no data hit Kafka yet — run `docker exec -it weather-app ./weather-app` and check Prometheus targets (`weather-consumer` should be green).

If ZIP entry fails, the app only accepts valid 5-digit US codes.

## Files

`main.go` talks to OpenWeather and publishes. `consumer.go` reads topics and serves Prometheus metrics. `evaluator.go` listens on `alert-config`. Rule and scrape configs are `alert_rules.yml` and `prometheus.yml`; dashboards live under `grafana/`.