package com.benchmark.report;

import com.benchmark.model.BenchmarkResult;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

public class CsvReportWriter {

    private static final String[] HEADERS = {
            "data_type", "data_property", "preprocessor", "codec", "order_by",
            "compression_ratio", "compressed_mb", "uncompressed_mb",
            "total_insert_time_sec", "avg_batch_time_ms",
            "cpu_avg_pct", "cpu_min_pct", "cpu_max_pct",
            "mem_avg_mb", "mem_avg_pct", "mem_min_mb", "mem_max_mb", "mem_min_pct", "mem_max_pct",
            "disk_before_mb", "disk_after_mb", "disk_delta_mb", "disk_used_pct",
            "table_name", "total_rows", "batch_size", "total_batches"
    };

    private final String filePath;

    public CsvReportWriter(String filePath) {
        this.filePath = filePath;
    }

    /**
     * Initialize: delete the existing file if present, then create a fresh file with header.
     */
    public void initialize() {
        Path path = Paths.get(filePath);
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            throw new RuntimeException("Failed to delete existing CSV file '" + filePath + "': " + e.getMessage(), e);
        }
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath));
             CSVPrinter printer = new CSVPrinter(writer,
                     CSVFormat.DEFAULT.builder().setHeader(HEADERS).build())) {
            printer.flush();
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize CSV file: " + e.getMessage(), e);
        }
    }

    /**
     * Append one result row to the CSV file.
     */
    public void appendResult(BenchmarkResult result) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true));
             CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT)) {
            printer.printRecord(
                    result.dataType,
                    result.dataProperty,
                    result.preprocessor,
                    result.codec,
                    result.orderBy,
                    fmt(result.compressionRatio),
                    fmt(result.compressedMb),
                    fmt(result.uncompressedMb),
                    fmt(result.totalInsertTimeSec),
                    fmt(result.avgBatchTimeMs),
                    fmt(result.cpuAvgPct),
                    fmt(result.cpuMinPct),
                    fmt(result.cpuMaxPct),
                    fmt(result.memAvgMb),
                    fmt(result.memAvgPct),
                    fmt(result.memMinMb),
                    fmt(result.memMaxMb),
                    fmt(result.memMinPct),
                    fmt(result.memMaxPct),
                    fmt(result.diskBeforeMb),
                    fmt(result.diskAfterMb),
                    fmt(result.diskDeltaMb),
                    fmt(result.diskUsedPct),
                    result.tableName,
                    result.totalRows,
                    result.batchSize,
                    result.totalBatches
            );
            printer.flush();
        } catch (IOException e) {
            throw new RuntimeException("Failed to write CSV row: " + e.getMessage(), e);
        }
    }

    /**
     * Format a double value to 2 decimal places.
     * Returns "-1" for error sentinel values (Double.NaN); negative values are formatted normally.
     */
    private static String fmt(double v) {
        if (Double.isNaN(v)) return "-1";
        return String.format("%.2f", v);
    }

    /**
     * Read existing results and return the set of already-completed table names.
     */
    public Set<String> getCompletedTableNames() {
        Set<String> completed = new HashSet<>();
        Path path = Paths.get(filePath);
        if (!Files.exists(path)) return completed;

        try (Reader reader = new FileReader(filePath);
             CSVParser parser = CSVFormat.DEFAULT.builder()
                     .setHeader()
                     .setSkipHeaderRecord(true)
                     .build()
                     .parse(reader)) {
            for (CSVRecord record : parser) {
                if (record.isMapped("table_name")) {
                    String tableName = record.get("table_name");
                    if (tableName != null && !tableName.isBlank()) {
                        completed.add(tableName);
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Warning: Could not read existing CSV for resumability: " + e.getMessage());
        }
        return completed;
    }
}
