package com.benchmark.clickhouse;

import com.benchmark.config.BenchmarkConfig;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ClickHouseManager {

    private static final MediaType TEXT_PLAIN =
            MediaType.parse("text/plain; charset=utf-8");

    private final OkHttpClient client;
    private final String baseUrl;

    public ClickHouseManager() {
        this.client = new OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS)
                .readTimeout(600, TimeUnit.SECONDS)
                .writeTimeout(600, TimeUnit.SECONDS)
                .build();
        this.baseUrl = BenchmarkConfig.CLICKHOUSE_URL + "/?database="
                + BenchmarkConfig.CLICKHOUSE_DATABASE
                + "&user=" + BenchmarkConfig.CLICKHOUSE_USER
                + "&password=" + BenchmarkConfig.CLICKHOUSE_PASSWORD;
    }

    /**
     * Drop the configured database if it exists, then create it fresh.
     * Uses a connection URL without the database parameter since we're
     * operating on the database itself.
     */
    public void recreateDatabase() {
        String url = BenchmarkConfig.CLICKHOUSE_URL + "/?user=" + BenchmarkConfig.CLICKHOUSE_USER
                + "&password=" + BenchmarkConfig.CLICKHOUSE_PASSWORD;
        executeOnUrl("DROP DATABASE IF EXISTS " + BenchmarkConfig.CLICKHOUSE_DATABASE, url);
        executeOnUrl("CREATE DATABASE " + BenchmarkConfig.CLICKHOUSE_DATABASE, url);
    }

    private void executeOnUrl(String sql, String url) {
        RequestBody body = RequestBody.create(sql, TEXT_PLAIN);
        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .build();
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                String errorBody = response.body() != null ? response.body().string() : "(no body)";
                throw new RuntimeException("ClickHouse error [" + response.code() + "]: " + errorBody);
            }
        } catch (IOException e) {
            throw new RuntimeException("ClickHouse HTTP error: " + e.getMessage(), e);
        }
    }

    /**
     * Ensure the configured database exists, creating it if necessary.
     * Uses a connection URL without the database parameter so the command
     * succeeds even when the database does not yet exist.
     */
    public void ensureDatabaseExists() {
        String url = BenchmarkConfig.CLICKHOUSE_URL + "/?user=" + BenchmarkConfig.CLICKHOUSE_USER
                + "&password=" + BenchmarkConfig.CLICKHOUSE_PASSWORD;
        String sql = "CREATE DATABASE IF NOT EXISTS " + BenchmarkConfig.CLICKHOUSE_DATABASE;
        RequestBody body = RequestBody.create(sql, TEXT_PLAIN);
        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .build();
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                String errorBody = response.body() != null ? response.body().string() : "(no body)";
                throw new RuntimeException("ClickHouse error [" + response.code() + "]: " + errorBody);
            }
        } catch (IOException e) {
            throw new RuntimeException("ClickHouse HTTP error: " + e.getMessage(), e);
        }
    }

    /**
     * Drop a table if it exists.
     */
    public void dropTableIfExists(String tableName) {
        execute("DROP TABLE IF EXISTS " + tableName);
    }

    /**
     * Execute any SQL statement. Throws RuntimeException on failure.
     */
    public void execute(String sql) {
        RequestBody body = RequestBody.create(sql, TEXT_PLAIN);
        Request request = new Request.Builder()
                .url(baseUrl)
                .post(body)
                .build();
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                String errorBody = response.body() != null ? response.body().string() : "(no body)";
                throw new RuntimeException("ClickHouse error [" + response.code() + "]: " + errorBody);
            }
        } catch (IOException e) {
            throw new RuntimeException("ClickHouse HTTP error: " + e.getMessage(), e);
        }
    }

    /**
     * Create a table using the provided DDL.
     */
    public void createTable(String ddl) {
        execute(ddl);
    }

    /**
     * Insert a batch of values into the given table.
     * id starts from startId for this batch.
     * values is a list of SQL literals (already quoted if needed).
     */
    public void insertBatch(String tableName, List<String> values, long startId) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(tableName).append(" (id, value) VALUES ");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) sb.append(',');
            sb.append('(').append(startId + i).append(',').append(values.get(i)).append(')');
        }
        execute(sb.toString());
    }

    /**
     * Run OPTIMIZE TABLE FINAL to force merges.
     */
    public void optimizeTable(String tableName) {
        execute("OPTIMIZE TABLE " + tableName + " FINAL");
    }

    /**
     * Query system.columns for the value column's compressed and uncompressed bytes.
     *
     * @return [compressedBytes, uncompressedBytes]
     */
    public long[] getColumnSizes(String tableName) {
        String sql = "SELECT sum(data_compressed_bytes) AS compressed, " +
                "sum(data_uncompressed_bytes) AS uncompressed " +
                "FROM system.columns " +
                "WHERE database='" + BenchmarkConfig.CLICKHOUSE_DATABASE +
                "' AND table='" + tableName + "' AND name='value'";

        RequestBody body = RequestBody.create(sql, TEXT_PLAIN);
        Request request = new Request.Builder()
                .url(baseUrl)
                .post(body)
                .build();
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                String errorBody = response.body() != null ? response.body().string() : "(no body)";
                throw new RuntimeException("ClickHouse error [" + response.code() + "]: " + errorBody);
            }
            String responseBody = response.body() != null ? response.body().string().trim() : "";
            if (responseBody.isEmpty()) {
                return new long[]{0L, 0L};
            }
            // Response is TSV: compressed\tuncompressed
            try {
                String[] parts = responseBody.split("\t");
                long compressed = Long.parseLong(parts[0].trim());
                long uncompressed = parts.length > 1 ? Long.parseLong(parts[1].trim()) : 0L;
                return new long[]{compressed, uncompressed};
            } catch (NumberFormatException e) {
                throw new RuntimeException(
                        "Failed to parse column sizes from ClickHouse response: '" + responseBody + "'", e);
            }
        } catch (IOException e) {
            throw new RuntimeException("ClickHouse HTTP error: " + e.getMessage(), e);
        }
    }
}
