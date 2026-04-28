# SUMMARY.md update — Data Pipelines Track entry

This file shows the lines to add to your existing `src/SUMMARY.md`, immediately
after the Database Track block. The pattern matches the deployed structure
(separator headers + nested module/lesson links); adjust the prefix `# Data
Pipelines Track` if your existing separators use a different style.

## What to add

Append this block after the last line of the Database Track section
(currently the project link for Module 6: Catalog Merge):

```markdown
# Data Pipelines Track

- [Module 1: Stream Processing Foundations](./data_pipelines_track/module-01-stream-processing-foundations/README.md)
    - [Streams, Sources, and Sinks](./data_pipelines_track/module-01-stream-processing-foundations/lesson-01-streams-sources-sinks.md)
    - [The Dataflow Model](./data_pipelines_track/module-01-stream-processing-foundations/lesson-02-dataflow-model.md)
    - [Push, Pull, and Poll Semantics](./data_pipelines_track/module-01-stream-processing-foundations/lesson-03-push-pull-poll.md)
    - [Project: Sensor Ingestion Service](./data_pipelines_track/module-01-stream-processing-foundations/project-sensor-ingestion.md)
```

## Reserved entries for upcoming modules

For convenience, the full track-level outline (modules 2–6) is below. These
entries should be added as each module ships; do not enable links until the
referenced files exist or `mdbook build` will fail.

```markdown
- [Module 2: Pipeline Orchestration Internals]()  # 4 lessons + project — coming
- [Module 3: Event Time and Watermarks]()          # 4 lessons + project — coming
- [Module 4: Backpressure and Flow Control]()      # 3 lessons + project — coming
- [Module 5: Delivery Guarantees and Fault Tolerance]()  # 4 lessons + project — coming
- [Module 6: Observability and Lineage]()          # 3 lessons + project — coming
```

mdBook treats `[Title]()` (empty link) as a non-clickable placeholder in the
sidebar, which is the right behavior while a module is in flight.
