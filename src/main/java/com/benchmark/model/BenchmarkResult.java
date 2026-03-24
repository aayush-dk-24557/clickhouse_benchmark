package com.benchmark.model;

public class BenchmarkResult {
    public String dataType;
    public String preprocessor;
    public String codec;
    public String tableName;
    public int totalRows;
    public int batchSize;
    public int totalBatches;
    public double totalInsertTimeSec;
    public double avgBatchTimeMs;
    public long compressedBytes;
    public long uncompressedBytes;
    public double compressionRatio;
    public double cpuMinPct;
    public double cpuMaxPct;
    public double cpuAvgPct;
    public double memMinBytes;
    public double memMaxBytes;
    public double memAvgBytes;
    public double memMinPct;
    public double memMaxPct;
    public double memAvgPct;
    public double diskUsedBeforeBytes;
    public double diskUsedAfterBytes;
    public double diskDeltaBytes;
    public double diskUsedPct;
}
