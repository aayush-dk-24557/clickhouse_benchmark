package com.benchmark;

import com.benchmark.clickhouse.ClickHouseManager;
import com.benchmark.clickhouse.TableSchemaBuilder;
import com.benchmark.config.BenchmarkConfig;
import com.benchmark.generator.DataGenerator;
import com.benchmark.model.BenchmarkResult;
import com.benchmark.model.Combination;
import com.benchmark.model.DataTypeInfo;
import com.benchmark.prometheus.PrometheusMetrics;
import com.benchmark.report.CsvReportWriter;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        // 1. Initialize all DataTypeInfo objects (20 types)
        List<DataTypeInfo> dataTypes = initDataTypes();

        // 2. Build all valid Combination objects
        List<Combination> allCombinations = buildCombinations(dataTypes);
        System.out.println("Total combinations: " + allCombinations.size());

        // 3. Initialize CSV writer, check for already completed (resumability)
        CsvReportWriter csvWriter = new CsvReportWriter(BenchmarkConfig.OUTPUT_FILE);
        csvWriter.initialize();
        Set<String> completed = csvWriter.getCompletedTableNames();

        // 4. Initialize managers
        ClickHouseManager chManager = new ClickHouseManager();
        PrometheusMetrics prometheus = new PrometheusMetrics();
        DataGenerator generator = new DataGenerator();

        // 5. Run each combination
        int total = allCombinations.size();
        int current = 0;
        long overallStart = System.currentTimeMillis();

        for (Combination combo : allCombinations) {
            current++;
            if (completed.contains(combo.tableName)) {
                System.out.println("[" + current + "/" + total + "] SKIPPING (already done): " + combo.tableName);
                continue;
            }

            System.out.println("[" + current + "/" + total + "] Starting: " + combo.tableName);

            try {
                // a. Create table
                String ddl = TableSchemaBuilder.buildCreateTable(
                        combo.tableName, combo.fullTypeDef, combo.preprocessor, combo.codec);
                chManager.createTable(ddl);

                // b. Record start time
                long startEpoch = System.currentTimeMillis() / 1000;
                long insertStart = System.nanoTime();

                // c. Reset seed for this data type
                generator.resetSeed(combo.dataType);

                // d. Insert batches
                for (int batch = 0; batch < BenchmarkConfig.TOTAL_BATCHES; batch++) {
                    long startId = (long) batch * BenchmarkConfig.BATCH_SIZE + 1;
                    List<String> values = generator.generateBatch(combo.dataType, BenchmarkConfig.BATCH_SIZE);
                    chManager.insertBatch(combo.tableName, values, startId);

                    if ((batch + 1) % BenchmarkConfig.LOG_INTERVAL == 0) {
                        System.out.println("  Batch " + (batch + 1) + "/" + BenchmarkConfig.TOTAL_BATCHES);
                    }
                }

                // e. Record end time
                long insertEnd = System.nanoTime();
                long endEpoch = System.currentTimeMillis() / 1000;
                double totalInsertSec = (insertEnd - insertStart) / 1_000_000_000.0;
                double avgBatchMs = (totalInsertSec * 1000) / BenchmarkConfig.TOTAL_BATCHES;

                // f. Optimize table
                chManager.optimizeTable(combo.tableName);
                Thread.sleep(BenchmarkConfig.MERGE_SETTLE_DELAY_MS);

                // g. Get column sizes
                long[] sizes = chManager.getColumnSizes(combo.tableName);
                long compressed = sizes[0];
                long uncompressed = sizes[1];
                double ratio = uncompressed > 0 && compressed > 0
                        ? (double) uncompressed / compressed : 0;

                // h. Get prometheus metrics
                double[] cpu = prometheus.getCpuUsage(startEpoch, endEpoch);
                double[] mem = prometheus.getMemoryUsage(startEpoch, endEpoch);
                double[] disk = prometheus.getDiskUsage(startEpoch, endEpoch);

                // i. Build result
                BenchmarkResult result = new BenchmarkResult();
                result.dataType = combo.dataType;
                result.preprocessor = combo.preprocessor;
                result.codec = combo.codec;
                result.tableName = combo.tableName;
                result.totalRows = BenchmarkConfig.TOTAL_ROWS;
                result.batchSize = BenchmarkConfig.BATCH_SIZE;
                result.totalBatches = BenchmarkConfig.TOTAL_BATCHES;
                result.totalInsertTimeSec = totalInsertSec;
                result.avgBatchTimeMs = avgBatchMs;
                result.compressedBytes = compressed;
                result.uncompressedBytes = uncompressed;
                result.compressionRatio = ratio;
                result.cpuMinPct = cpu[0];
                result.cpuMaxPct = cpu[1];
                result.cpuAvgPct = cpu[2];
                result.memMinBytes = mem[0];
                result.memMaxBytes = mem[1];
                result.memAvgBytes = mem[2];
                result.memMinPct = mem[3];
                result.memMaxPct = mem[4];
                result.memAvgPct = mem[5];
                result.diskUsedBeforeBytes = disk[0];
                result.diskUsedAfterBytes = disk[1];
                result.diskDeltaBytes = disk[2];
                result.diskUsedPct = disk[3];

                // j. Write to CSV
                csvWriter.appendResult(result);

                // k. Log completion
                long elapsed = System.currentTimeMillis() - overallStart;
                int remaining = total - current;
                long eta = remaining > 0 ? (elapsed / current) * remaining : 0;
                System.out.printf("[%d/%d] Completed: %s in %.1fs | Compressed: %d bytes | Ratio: %.2fx | ETA: %s%n",
                        current, total, combo.tableName, totalInsertSec, compressed, ratio, formatDuration(eta));

            } catch (Exception e) {
                System.err.println("[" + current + "/" + total + "] FAILED: " + combo.tableName + " - " + e.getMessage());
                e.printStackTrace();
                // Write error row to CSV with -1 values
                BenchmarkResult errorResult = createErrorResult(combo);
                csvWriter.appendResult(errorResult);
            }
        }

        System.out.println("Benchmark complete! Results written to " + BenchmarkConfig.OUTPUT_FILE);
    }

    // ---- Data type initialization ----

    private static List<DataTypeInfo> initDataTypes() {
        List<DataTypeInfo> types = new ArrayList<>();

        // All integer/numeric types: support Delta, DoubleDelta, T64
        types.add(new DataTypeInfo("UInt8", "UInt8", true, true, true, seed("UInt8")));
        types.add(new DataTypeInfo("UInt16", "UInt16", true, true, true, seed("UInt16")));
        types.add(new DataTypeInfo("UInt32", "UInt32", true, true, true, seed("UInt32")));
        types.add(new DataTypeInfo("UInt64", "UInt64", true, true, true, seed("UInt64")));
        types.add(new DataTypeInfo("Int8", "Int8", true, true, true, seed("Int8")));
        types.add(new DataTypeInfo("Int16", "Int16", true, true, true, seed("Int16")));
        types.add(new DataTypeInfo("Int32", "Int32", true, true, true, seed("Int32")));
        types.add(new DataTypeInfo("Int64", "Int64", true, true, true, seed("Int64")));

        // Float types: support Delta, DoubleDelta but NOT T64
        types.add(new DataTypeInfo("Float32", "Float32", true, true, false, seed("Float32")));
        types.add(new DataTypeInfo("Float64", "Float64", true, true, false, seed("Float64")));

        // Date/DateTime: support Delta, DoubleDelta, T64
        types.add(new DataTypeInfo("Date", "Date", true, true, true, seed("Date")));
        types.add(new DataTypeInfo("Date32", "Date32", true, true, true, seed("Date32")));
        types.add(new DataTypeInfo("DateTime", "DateTime", true, true, true, seed("DateTime")));
        types.add(new DataTypeInfo("DateTime64(3)", "DateTime64(3)", true, true, true, seed("DateTime64(3)")));

        // Decimal: support Delta, DoubleDelta, T64
        types.add(new DataTypeInfo("Decimal32(2)", "Decimal32(2)", true, true, true, seed("Decimal32(2)")));
        types.add(new DataTypeInfo("Decimal64(2)", "Decimal64(2)", true, true, true, seed("Decimal64(2)")));

        // Enum8: support Delta, DoubleDelta, T64
        String enum8Def = "Enum8('v1'=1,'v2'=2,'v3'=3,'v4'=4,'v5'=5,'v6'=6,'v7'=7,'v8'=8,'v9'=9,'v10'=10)";
        types.add(new DataTypeInfo("Enum8", enum8Def, true, true, true, seed("Enum8")));

        // IPv4: support Delta, DoubleDelta, T64
        types.add(new DataTypeInfo("IPv4", "IPv4", true, true, true, seed("IPv4")));

        // String: no preprocessors
        types.add(new DataTypeInfo("String", "String", false, false, false, seed("String")));

        // LowCardinality(String): no preprocessors
        types.add(new DataTypeInfo("LowCardinality(String)", "LowCardinality(String)",
                false, false, false, seed("LowCardinality(String)")));

        return types;
    }

    private static long seed(String typeName) {
        return (long) typeName.hashCode();
    }

    // ---- Combination building ----

    private static List<Combination> buildCombinations(List<DataTypeInfo> dataTypes) {
        String[] preprocessors = {"None", "Delta", "DoubleDelta", "T64"};
        String[] codecs = {"LZ4", "ZSTD(1)"};

        List<Combination> combinations = new ArrayList<>();
        for (DataTypeInfo dt : dataTypes) {
            for (String preprocessor : preprocessors) {
                // Check compatibility
                if ("Delta".equals(preprocessor) && !dt.supportsDelta) continue;
                if ("DoubleDelta".equals(preprocessor) && !dt.supportsDoubleDelta) continue;
                if ("T64".equals(preprocessor) && !dt.supportsT64) continue;

                for (String codec : codecs) {
                    String tableName = buildTableName(dt.typeName, preprocessor, codec);
                    combinations.add(new Combination(dt.typeName, dt.fullTypeDef,
                            preprocessor, codec, tableName));
                }
            }
        }
        return combinations;
    }

    /**
     * Build table name from type name, preprocessor, and codec.
     * Format: test_{typename}_{preprocessor}_{codec}
     * Sanitize: lowercase, remove/replace special chars.
     */
    private static String buildTableName(String typeName, String preprocessor, String codec) {
        String sanitizedType = sanitizeTypeName(typeName);
        String sanitizedPreprocessor = preprocessor.toLowerCase();
        String sanitizedCodec = codec.toLowerCase()
                .replace("(", "")
                .replace(")", "")
                .replace(" ", "");
        return "test_" + sanitizedType + "_" + sanitizedPreprocessor + "_" + sanitizedCodec;
    }

    /**
     * Sanitize type name: lowercase, replace special chars with underscores,
     * strip outer parentheses content for Enum8, etc.
     */
    private static String sanitizeTypeName(String typeName) {
        // Remove the enum values from Enum8(...) -> "enum8"
        if (typeName.startsWith("Enum8(") || typeName.startsWith("Enum16(")) {
            return typeName.replaceAll("\\(.*\\)", "").toLowerCase();
        }
        // LowCardinality(String) -> lowcardinality_string
        String result = typeName.toLowerCase();
        // Replace '(' and ')' with _ or remove
        result = result.replace("(", "_").replace(")", "");
        // Remove trailing underscore
        result = result.replaceAll("_+$", "");
        // Replace spaces with _
        result = result.replace(" ", "_");
        return result;
    }

    // ---- Error result helper ----

    private static BenchmarkResult createErrorResult(Combination combo) {
        BenchmarkResult r = new BenchmarkResult();
        r.dataType = combo.dataType;
        r.preprocessor = combo.preprocessor;
        r.codec = combo.codec;
        r.tableName = combo.tableName;
        r.totalRows = BenchmarkConfig.TOTAL_ROWS;
        r.batchSize = BenchmarkConfig.BATCH_SIZE;
        r.totalBatches = BenchmarkConfig.TOTAL_BATCHES;
        r.totalInsertTimeSec = -1;
        r.avgBatchTimeMs = -1;
        r.compressedBytes = -1;
        r.uncompressedBytes = -1;
        r.compressionRatio = -1;
        r.cpuMinPct = -1;
        r.cpuMaxPct = -1;
        r.cpuAvgPct = -1;
        r.memMinBytes = -1;
        r.memMaxBytes = -1;
        r.memAvgBytes = -1;
        r.memMinPct = -1;
        r.memMaxPct = -1;
        r.memAvgPct = -1;
        r.diskUsedBeforeBytes = -1;
        r.diskUsedAfterBytes = -1;
        r.diskDeltaBytes = -1;
        r.diskUsedPct = -1;
        return r;
    }

    // ---- Utility ----

    private static String formatDuration(long ms) {
        long secs = ms / 1000;
        long hours = secs / 3600;
        long minutes = (secs % 3600) / 60;
        long seconds = secs % 60;
        if (hours > 0) {
            return String.format("%dh %02dm %02ds", hours, minutes, seconds);
        } else if (minutes > 0) {
            return String.format("%dm %02ds", minutes, seconds);
        } else {
            return String.format("%ds", seconds);
        }
    }
}
