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
            "data_type", "preprocessor", "codec", "table_name",
            "total_rows", "batch_size", "total_batches",
            "total_insert_time_sec", "avg_batch_time_ms",
            "compressed_bytes", "uncompressed_bytes", "compression_ratio",
            "cpu_min_pct", "cpu_max_pct", "cpu_avg_pct",
            "mem_min_bytes", "mem_max_bytes", "mem_avg_bytes",
            "mem_min_pct", "mem_max_pct", "mem_avg_pct",
            "disk_used_before_bytes", "disk_used_after_bytes",
            "disk_delta_bytes", "disk_used_pct"
    };

    private final String filePath;

    public CsvReportWriter(String filePath) {
        this.filePath = filePath;
    }

    /**
     * Initialize: create file with header if it doesn't already exist.
     */
    public void initialize() {
        Path path = Paths.get(filePath);
        if (!Files.exists(path)) {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath));
                 CSVPrinter printer = new CSVPrinter(writer,
                         CSVFormat.DEFAULT.builder().setHeader(HEADERS).build())) {
                printer.flush();
            } catch (IOException e) {
                throw new RuntimeException("Failed to initialize CSV file: " + e.getMessage(), e);
            }
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
                    result.preprocessor,
                    result.codec,
                    result.tableName,
                    result.totalRows,
                    result.batchSize,
                    result.totalBatches,
                    result.totalInsertTimeSec,
                    result.avgBatchTimeMs,
                    result.compressedBytes,
                    result.uncompressedBytes,
                    result.compressionRatio,
                    result.cpuMinPct,
                    result.cpuMaxPct,
                    result.cpuAvgPct,
                    result.memMinBytes,
                    result.memMaxBytes,
                    result.memAvgBytes,
                    result.memMinPct,
                    result.memMaxPct,
                    result.memAvgPct,
                    result.diskUsedBeforeBytes,
                    result.diskUsedAfterBytes,
                    result.diskDeltaBytes,
                    result.diskUsedPct
            );
            printer.flush();
        } catch (IOException e) {
            throw new RuntimeException("Failed to write CSV row: " + e.getMessage(), e);
        }
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
