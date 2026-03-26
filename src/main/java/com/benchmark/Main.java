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
        // 1. Initialize all DataTypeInfo objects
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
        chManager.recreateDatabase();
        System.out.println("Waiting " + BenchmarkConfig.DB_DROP_SETTLE_DELAY_MS / 1000
                + "s for ClickHouse to finish deleting all tables from disk...");
        Thread.sleep(BenchmarkConfig.DB_DROP_SETTLE_DELAY_MS);
        System.out.println("Disk settle wait complete. Starting benchmark.");
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
                // a. Drop existing table and create a new one
                chManager.dropTableIfExists(combo.tableName);
                Thread.sleep(BenchmarkConfig.MERGE_SETTLE_DELAY_MS); // wait for ClickHouse to free disk
                String ddl = TableSchemaBuilder.buildCreateTable(
                        combo.tableName, combo.fullTypeDef, combo.preprocessor, combo.codec,
                        combo.orderBy);
                chManager.createTable(ddl);

                // b. Record start time
                long startEpoch = System.currentTimeMillis() / 1000;
                long insertStart = System.nanoTime();

                // c. Reset seed for this data type + property
                generator.resetSeed(combo.dataType, combo.dataProperty);

                // d. Insert batches
                for (int batch = 0; batch < BenchmarkConfig.TOTAL_BATCHES; batch++) {
                    long startId = (long) batch * BenchmarkConfig.BATCH_SIZE + 1;
                    List<String> values = generator.generateBatch(
                            combo.dataType, combo.dataProperty, BenchmarkConfig.BATCH_SIZE);
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

                // g. Get column sizes (bytes from ClickHouse)
                long[] sizes = chManager.getColumnSizes(combo.tableName);
                long compressed = sizes[0];
                long uncompressed = sizes[1];
                double ratio = uncompressed > 0 && compressed > 0
                        ? (double) uncompressed / compressed : 0;

                // h. Get prometheus metrics
                double[] cpu = prometheus.getCpuUsage(startEpoch, endEpoch);
                double[] mem = prometheus.getMemoryUsage(startEpoch, endEpoch);
                double[] disk = prometheus.getDiskUsage(startEpoch, endEpoch);

                // i. Build result — convert bytes to MB
                BenchmarkResult result = new BenchmarkResult();
                result.dataType = combo.dataType;
                result.dataProperty = combo.dataProperty + "_orderby_" + combo.orderBy;
                result.preprocessor = combo.preprocessor;
                result.codec = combo.codec;
                result.tableName = combo.tableName;
                result.totalRows = BenchmarkConfig.TOTAL_ROWS;
                result.batchSize = BenchmarkConfig.BATCH_SIZE;
                result.totalBatches = BenchmarkConfig.TOTAL_BATCHES;
                result.totalInsertTimeSec = totalInsertSec;
                result.avgBatchTimeMs = avgBatchMs;
                result.compressedMb = compressed / 1048576.0;
                result.uncompressedMb = uncompressed / 1048576.0;
                result.compressionRatio = ratio;
                result.cpuMinPct = cpu[0];
                result.cpuMaxPct = cpu[1];
                result.cpuAvgPct = cpu[2];
                result.memMinMb = Double.isNaN(mem[0]) ? Double.NaN : mem[0] / 1048576.0;
                result.memMaxMb = Double.isNaN(mem[1]) ? Double.NaN : mem[1] / 1048576.0;
                result.memAvgMb = Double.isNaN(mem[2]) ? Double.NaN : mem[2] / 1048576.0;
                result.memMinPct = mem[3];
                result.memMaxPct = mem[4];
                result.memAvgPct = mem[5];
                result.diskBeforeMb = Double.isNaN(disk[0]) ? Double.NaN : disk[0] / 1048576.0;
                result.diskAfterMb = Double.isNaN(disk[1]) ? Double.NaN : disk[1] / 1048576.0;
                result.diskDeltaMb = Double.isNaN(disk[2]) ? Double.NaN : disk[2] / 1048576.0;
                result.diskUsedPct = disk[3];

                // j. Write to CSV
                csvWriter.appendResult(result);

                // k. Log completion
                long elapsed = System.currentTimeMillis() - overallStart;
                int remaining = total - current;
                long eta = remaining > 0 ? (elapsed / current) * remaining : 0;
                System.out.printf("[%d/%d] Completed: %s in %.1fs | Compressed: %.2f MB | Ratio: %.2fx | ETA: %s%n",
                        current, total, combo.tableName, totalInsertSec,
                        result.compressedMb, ratio, formatDuration(eta));

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

        types.add(new DataTypeInfo("UInt8", "UInt8", true, true, true, seed("UInt8"),
                List.of("0-20", "0-100", "0-255")));
        types.add(new DataTypeInfo("UInt16", "UInt16", true, true, true, seed("UInt16"),
                List.of("0-100", "0-500", "0-5000", "0-65535")));
        types.add(new DataTypeInfo("UInt32", "UInt32", true, true, true, seed("UInt32"),
                List.of("0-5000", "0-50000", "0-4294967295")));
        types.add(new DataTypeInfo("UInt64", "UInt64", true, true, true, seed("UInt64"),
                List.of("0-1000000", "0-50000000", "random", "ordered_epoch_ms")));
        types.add(new DataTypeInfo("Int8", "Int8", true, true, true, seed("Int8"),
                List.of("-10_to_10", "-50_to_50", "-128_to_127")));
        types.add(new DataTypeInfo("Int16", "Int16", true, true, true, seed("Int16"),
                List.of("-100_to_100", "-1000_to_1000", "-32768_to_32767")));
        types.add(new DataTypeInfo("Int32", "Int32", true, true, true, seed("Int32"),
                List.of("-50000_to_50000", "full_range")));
        types.add(new DataTypeInfo("Int64", "Int64", true, true, true, seed("Int64"),
                List.of("-1000000_to_1000000", "-50000000_to_50000000", "full_range", "uid_60xxxxxxxxx")));
        types.add(new DataTypeInfo("Float32", "Float32", true, true, false, seed("Float32"),
                List.of("0.0_to_1.0", "-1000_to_1000", "full_range")));
        types.add(new DataTypeInfo("Float64", "Float64", true, true, false, seed("Float64"),
                List.of("0.0_to_1.0", "-100000_to_100000", "full_range")));
        types.add(new DataTypeInfo("Date", "Date", true, true, true, seed("Date"),
                List.of("ordered_10yr")));
        types.add(new DataTypeInfo("Date32", "Date32", true, true, true, seed("Date32"),
                List.of("ordered_10yr")));
        types.add(new DataTypeInfo("DateTime", "DateTime", true, true, true, seed("DateTime"),
                List.of("ordered_10yr")));
        types.add(new DataTypeInfo("DateTime64(3)", "DateTime64(3)", true, true, true, seed("DateTime64(3)"),
                List.of("ordered_10yr")));
        types.add(new DataTypeInfo("Decimal32(2)", "Decimal32(2)", true, true, true, seed("Decimal32(2)"),
                List.of("0_to_100", "-10000_to_10000", "full_range")));
        types.add(new DataTypeInfo("Decimal64(2)", "Decimal64(2)", true, true, true, seed("Decimal64(2)"),
                List.of("0_to_10000", "-1000000_to_1000000", "full_range")));
        // Enum8: base fullTypeDef is null; resolved per property in resolveFullTypeDef()
        types.add(new DataTypeInfo("Enum8", null, true, true, true, seed("Enum8"),
                List.of("10_constants", "20_constants", "30_constants", "40_constants", "50_constants")));
        types.add(new DataTypeInfo("IPv4", "IPv4", true, true, true, seed("IPv4"),
                List.of("random")));
        types.add(new DataTypeInfo("String", "String", false, false, false, seed("String"),
                List.of("len_20_50", "len_50_100", "len_100_150", "len_150_250")));
        types.add(new DataTypeInfo("LowCardinality(String)", "LowCardinality(String)",
                false, false, false, seed("LowCardinality(String)"),
                List.of("50_distinct", "100_distinct", "500_distinct",
                        "1000_distinct", "5000_distinct", "10000_distinct")));
        types.add(new DataTypeInfo("FixedString", null, false, false, false, seed("FixedString"),
                List.of("len_20", "len_50", "len_100")));

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
            for (String property : dt.dataProperties) {
                String fullTypeDef = resolveFullTypeDef(dt, property);

                // Determine which ORDER BY variants to use for this property
                String[] orderBys;
                if ("uid_60xxxxxxxxx".equals(property)) {
                    orderBys = new String[]{"id", "value"};
                } else {
                    orderBys = new String[]{"id"};
                }

                for (String preprocessor : preprocessors) {
                    if ("Delta".equals(preprocessor) && !dt.supportsDelta) continue;
                    if ("DoubleDelta".equals(preprocessor) && !dt.supportsDoubleDelta) continue;
                    if ("T64".equals(preprocessor) && !dt.supportsT64) continue;

                    for (String codec : codecs) {
                        for (String orderBy : orderBys) {
                            String tableName = buildTableName(
                                    dt.typeName, property, preprocessor, codec, orderBy);
                            combinations.add(new Combination(dt.typeName, property, fullTypeDef,
                                    preprocessor, codec, orderBy, tableName));
                        }
                    }
                }
            }
        }
        return combinations;
    }

    /**
     * Resolve the full ClickHouse type definition for a given DataTypeInfo + dataProperty.
     * For Enum8, builds the full enum definition based on constant count.
     * For all other types, returns dt.fullTypeDef.
     */
    private static String resolveFullTypeDef(DataTypeInfo dt, String property) {
        if ("Enum8".equals(dt.typeName)) {
            try {
                int count = Integer.parseInt(property.replace("_constants", ""));
                StringBuilder sb = new StringBuilder("Enum8(");
                for (int i = 1; i <= count; i++) {
                    if (i > 1) sb.append(",");
                    sb.append("'v").append(i).append("'=").append(i);
                }
                sb.append(")");
                return sb.toString();
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "Cannot parse Enum8 constant count from property '" + property + "'", e);
            }
        }
        if ("FixedString".equals(dt.typeName)) {
            // property: "len_20" -> FixedString(20), "len_50" -> FixedString(50), "len_100" -> FixedString(100)
            try {
                int len = Integer.parseInt(property.replace("len_", ""));
                return "FixedString(" + len + ")";
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "Cannot parse FixedString length from property '" + property + "'", e);
            }
        }
        return dt.fullTypeDef;
    }

    /**
     * Build table name including data_property and order_by.
     * Format: test_{sanitizedType}_{sanitizedProperty}_{sanitizedPreprocessor}_{sanitizedCodec}_orderby_{orderBy}
     */
    private static String buildTableName(String typeName, String property,
                                         String preprocessor, String codec,
                                         String orderBy) {
        String sanitizedType = sanitizeIdentifier(typeName);
        String sanitizedProperty = sanitizeIdentifier(property);
        String sanitizedPreprocessor = preprocessor.toLowerCase();
        String sanitizedCodec = codec.toLowerCase()
                .replace("(", "")
                .replace(")", "")
                .replace(" ", "");
        return "test_" + sanitizedType + "_" + sanitizedProperty
                + "_" + sanitizedPreprocessor + "_" + sanitizedCodec
                + "_orderby_" + orderBy;
    }

    /**
     * Sanitize an identifier: lowercase, replace non-alphanumeric chars with underscores,
     * collapse multiple underscores, strip leading/trailing underscores.
     */
    private static String sanitizeIdentifier(String s) {
        String result = s.toLowerCase();
        result = result.replaceAll("[^a-z0-9]+", "_");
        result = result.replaceAll("_+", "_");
        result = result.replaceAll("^_|_$", "");
        return result;
    }

    // ---- Error result helper ----

    private static BenchmarkResult createErrorResult(Combination combo) {
        BenchmarkResult r = new BenchmarkResult();
        r.dataType = combo.dataType;
        r.dataProperty = combo.dataProperty + "_orderby_" + combo.orderBy;
        r.preprocessor = combo.preprocessor;
        r.codec = combo.codec;
        r.tableName = combo.tableName;
        r.totalRows = BenchmarkConfig.TOTAL_ROWS;
        r.batchSize = BenchmarkConfig.BATCH_SIZE;
        r.totalBatches = BenchmarkConfig.TOTAL_BATCHES;
        r.totalInsertTimeSec = Double.NaN;
        r.avgBatchTimeMs = Double.NaN;
        r.compressedMb = Double.NaN;
        r.uncompressedMb = Double.NaN;
        r.compressionRatio = Double.NaN;
        r.cpuMinPct = Double.NaN;
        r.cpuMaxPct = Double.NaN;
        r.cpuAvgPct = Double.NaN;
        r.memMinMb = Double.NaN;
        r.memMaxMb = Double.NaN;
        r.memAvgMb = Double.NaN;
        r.memMinPct = Double.NaN;
        r.memMaxPct = Double.NaN;
        r.memAvgPct = Double.NaN;
        r.diskBeforeMb = Double.NaN;
        r.diskAfterMb = Double.NaN;
        r.diskDeltaMb = Double.NaN;
        r.diskUsedPct = Double.NaN;
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
