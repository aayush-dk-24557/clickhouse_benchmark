package com.benchmark.config;

public class BenchmarkConfig {
    public static final String CLICKHOUSE_URL = "http://localhost:8123";
    public static final String CLICKHOUSE_DATABASE = "AADDBB";
    public static final String CLICKHOUSE_USER = "default";
    public static final String CLICKHOUSE_PASSWORD = "";

    public static final String PROMETHEUS_URL = "http://localhost:9090";

    public static final int TOTAL_ROWS = 100_000_000;
    public static final int BATCH_SIZE = 10_000_000;
    public static final int TOTAL_BATCHES = (int) Math.ceil((double) TOTAL_ROWS / BATCH_SIZE); // 10

    public static final String OUTPUT_FILE = "benchmark_results.csv";
    public static final int LOG_INTERVAL = 1; // log every batch
    public static final int MERGE_SETTLE_DELAY_MS = 3000;
    // After DROP DATABASE (which removes all ~360 tables), ClickHouse deletes files
    // asynchronously. Wait long enough for the OS to fully reclaim disk space so
    // the first disk_before_mb reading is accurate.
    public static final int DB_DROP_SETTLE_DELAY_MS = 30_000;
}
