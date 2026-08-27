# Human modifications

My own backlog for Contrail, written after the build was code-complete. These are the things I
deliberately left out of the main build — either because they'd have muddied a benchmark, or because
they're the next honest step rather than part of the original three claims.

Rough priority order.

## Correctness

- [ ] **Idle-partition timeout for the watermark.** Right now the global watermark is the minimum
      across per-partition marks, so one silent partition freezes window finalization for everyone.
      Needs a per-source idleness deadline that drops a quiet partition out of the minimum. The
      catch is that it needs a wall clock, and the replay determinism proof depends on that
      processor being clock-free — so it has to be opt-in, off by default, and the determinism tests
      need to keep running with it off.
- [ ] **Decide what to do about window retractions.** Late events currently go to a side output and
      the finalized aggregate stands. The alternative is reopening the window and emitting a
      correction, which only makes sense if something downstream understands retractions. Nothing
      does yet. Revisit if the WebSocket consumers ever need corrected history rather than a live
      feed.

## Measurement gaps I know about

- [ ] **Run the control benchmark with `--max-workers 2` and record the shed numbers.** The current
      claim-2 run never triggers shedding because four workers absorb the burst outright. The shed
      path is only evidenced by the 1.4 integration run. One 7-minute run closes this.
- [ ] **Re-run the API load test on something that isn't a two-core laptop.** The 100-user column is
      measuring my machine, not the API. A box where the stack and the load generator aren't fighting
      over the same two cores would give a number worth quoting.

## Operational

- [ ] **OpenSky OAuth2.** Anonymous access is credit-limited per day and will eventually 429 on a
      long run. Registering gets a much higher allowance and lets the poll interval drop.
- [ ] **Rate limiting across API instances.** The token bucket is per-process and in-memory, so two
      API replicas mean double the intended rate. Move the buckets into Redis if this is ever more
      than one process.
- [ ] **Bound the dedup set in the windowing engine.** It grows one entry per unique event and is
      only fine because every run is bounded. Evict below the watermark before running it as a
      long-lived service.
- [ ] **Hypertable retention and compression.** No policy set, so `flight_events` grows forever.
      TimescaleDB gives both as one-liners; pick a window that matches whatever the dashboard
      actually queries.
- [ ] **Grafana alert rules.** The dashboard shows lag, watermark skew and shed rate but nothing
      fires on them. Sensible starting points: shedding engaged for more than a minute, watermark
      skew vs wall clock above a few minutes, consumer lag trending up for five consecutive samples.

## If it goes regional

- [ ] **Revisit the geographic partition key.** The 5-degree grid is tuned for worldwide synthetic
      traffic. On the live central-Europe bounding box it collapses to four cells with most traffic
      in one, so Kafka partitions go badly uneven. Either shrink `GRID_DEG` or swap `grid_cell()`
      for H3 — it's one function, which is why it was written that way.

## Nice to have

- [ ] **Alerts endpoint.** Was in the original scope as "if time permits" and it didn't. Something
      like altitude/velocity outliers per cell, served off the same Redis state the windows use.
- [ ] **A small frontend for the WebSocket feed.** A map with live aircraft would demo far better
      than curl, and the fan-out already works.
