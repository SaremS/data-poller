# data-poller-rs

Rust workspace for polling remote datasets, transforming them, and storing the results on a schedule.

## Workspace layout

- `core/` — ingestion, storage, transformation, and shared pipeline traits/types
- `orchestration/` — orchestrator and YAML config model
- `cli/` — command-line entrypoint for running an orchestrator from a YAML file
- `examples/` — example configs, including `examples/pmxt.yaml`

## Running the CLI

From the repository root:

```bash
cargo run -p data-poller-cli -- /home/sarem/projects/trading/data-poller-rs/examples/pmxt.yaml
```

If you are already in the repo root, the relative path also works:

```bash
cargo run -p data-poller-cli -- examples/pmxt.yaml
```

The CLI expects exactly one argument: the path to a YAML config file.

## Example config

`examples/pmxt.yaml` is a ready-to-run example that:

- fetches the PMXT archive index at `https://archive.pmxt.dev/Polymarket/v2`
- selects parquet listing rows with `pre > span`
- extracts each parquet URL from the nested `a` tag
- runs `SELECT * FROM remote_parquet LIMIT 5`
- writes output to the configured local directory

Example:

```bash
cargo run -p data-poller-cli -- examples/pmxt.yaml
```

## Testing

Run the workspace test suite from the repo root:

```bash
cargo test --workspace
```
