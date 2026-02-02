# Postgres Explorer

UI tool for browsing Postgres clusters. It mirrors the Elastic Explorer UX and
focuses on a fast operator workflow: connections, dashboards, tables, schemas,
indices, and a dev console.

## Features

- Connection manager (stored locally in SQLite)
- Encrypted passwords (key stored under macOS Application Support via `dirs`)
- Dashboard with top tables and key stats
- Schemas and tables browser with filtering, sorting, pagination
- Table detail: columns, indexes, partitions, triggers, relationships, data
- Indices browser with quick info and reindex action
- Dev console for running SQL (read/write)
- Caching for list pages to reduce DB load

## Requirements

- Rust (stable)
- Postgres (local or remote)

## Run

```bash
cargo run
```

Default URL: `http://127.0.0.1:8080`

### CLI options

```bash
postgres-explorer --host 127.0.0.1 --port 8080 --base-path / --no-open
```

Options:
- `--host` Host for HTTP server (default: `127.0.0.1`)
- `--port` Port for HTTP server (default: `8080`)
- `--base-path` Base path when behind reverse proxy (default: `/`)
- `--no-open` Do not open browser on startup

## Stateless mode (no local storage)

Use `--stateless` to run without SQLite and provide a single connection via CLI
or `.env`:

```bash
postgres-explorer \
  --stateless \
  --conf-db-url "postgres://127.0.0.1:5432/explorer" \
  --conf-db-username postgres \
  --conf-db-password postgres
```

Supported `--conf-*` parameters (also via `.env`):
- `--conf-name` / `CONF_NAME` (UI label)
- `--conf-db-url` / `CONF_DB_URL`
- `--conf-db-username` / `CONF_DB_USERNAME`
- `--conf-db-password` / `CONF_DB_PASSWORD`
- `--conf-db-ssl-mode` / `CONF_DB_SSL_MODE`
- `--conf-db-insecure` / `CONF_DB_INSECURE`
- `--conf-db-search-path` / `CONF_DB_SEARCH_PATH`

## Docker (dev)

There is a local `docker-compose.yml` for running a dev Postgres instance with
seed data. Start it with:

```bash
docker compose up -d
```

Use `.env.example` for local stateless config against the dev container.

## Notes

- List pages (`/schemas`, `/tables`, `/tables/indices`) are cached per
  connection for ~15 minutes to avoid hammering the DB.
- Filters use substring match with OR via comma:
  - `related` matches any name containing `related`
  - `related,act,bill` matches any name containing any of those terms
- Build Linux binary on macOS:
  - `brew install zigbuild`
  - `cargo zigbuild --release --target x86_64-unknown-linux-musl`

## License

AGPLv3. See `LICENSE`.

## Author

Martin Mareš

