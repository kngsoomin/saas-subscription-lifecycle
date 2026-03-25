In Phase1 and Phase2, I designed the data pipeline around an **append-only Bronze layer and deterministic recomputation downstream**.

At the Bronze layer, all incoming events are stored as immutable logs, partitioned by `ingested_at`. This ensures that the raw data remains untouched and serves as a reliable source of truth. Instead of attempting to update or mutate data in place, I intentionally structured the system so that all downstream datasets can be rebuilt from this immutable layer.

In the Silver layer, I reconstruct both the full event history and the current state snapshot by aggregating and processing Bronze data. The Gold layer further derives business-level metrics from these results. Importantly, both Silver and Gold are produced through **full recomputation**, rather than incremental updates or partial overwrites.

This design choice was heavily influenced by the nature of S3.

S3 is not a transactional system — it does not provide ACID guarantees such as atomicity or isolation. While it guarantees durability, it does not support atomic updates at the dataset level. In practice, this means that if we attempt to partially overwrite data (for example, updating only a subset of a partition), the system can be left in an inconsistent state if the job fails midway. Since S3 operations like overwrite are effectively implemented as a sequence of LIST, DELETE, and WRITE operations, they are not atomic.

Because of this, I deliberately avoided any design that depends on partial updates. Instead, I leaned into a recompute-based approach:

- Bronze is immutable and append-only  
- Transformations are deterministic  
- Outputs can always be rebuilt  

This eliminates the need to rely on atomic writes. Even if a job fails during the write phase, the system can simply be rerun, since the source of truth remains intact. In this sense, the system trades transactional guarantees for recomputability and simplicity.

This design also naturally handles late-arriving data. By partitioning Bronze data by `ingested_at` and recomputing downstream tables, late events are automatically incorporated without requiring complex backfills or partition rewrites.

However, while this approach is robust and simple, it comes with clear limitations.

First, **full recomputation is inherently costly**. As data volume grows, rebuilding entire datasets (especially in Silver and Gold) becomes increasingly inefficient in terms of both compute and I/O.

Second, although I avoid partial updates logically, the system still performs **physical overwrite operations at the file level** when writing Parquet outputs. These overwrites are not atomic in S3, which means there is still a window where the dataset may be temporarily inconsistent during execution.

Third, repeated recomputation leads to issues such as **small file proliferation**, which negatively impacts query performance and downstream processing.

These limitations led me to consider modern table formats like Apache Iceberg or Delta Lake.

Unlike a plain S3 + Parquet approach, these systems separate **data files from table state**. Data is still written in an append-only manner, but the “current state” of the table is managed through metadata snapshots. Updates are performed by creating new metadata and atomically switching a pointer to it, rather than modifying existing files.

This fundamentally changes the system behavior:

- No need for full dataset recomputation  
- Atomic table-level updates  
- Safe concurrent reads and writes  
- Support for incremental processing and merges  

In other words, Iceberg shifts the system from a **recompute-based model** to a **state-managed model**, while still preserving the benefits of immutable data.

Looking back, the current architecture represents a deliberate tradeoff:

> Instead of relying on storage-level guarantees (like atomicity), I designed the system to be correct by construction through immutability and recomputation.

This was a reasonable choice for an initial batch-based data lake, especially given S3’s characteristics. However, as the system scales, the limitations of this approach become more apparent, making a strong case for evolving toward a table format that provides metadata-level atomicity and efficient incremental processing.

This evolution—from append-only recomputation to metadata-driven table management—is the natural next step in building a production-grade data platform.