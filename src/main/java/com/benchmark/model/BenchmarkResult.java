package com.benchmark.model;

public class BenchmarkResult {
    public String dataType;
    public String dataProperty;       // value range/characteristic label
    public String preprocessor;
    public String codec;
    public String orderBy;            // "id" or "value"
    public String tableName;
    public int totalRows;
    public int batchSize;
    public int totalBatches;
    public double totalInsertTimeSec;
    public double avgBatchTimeMs;
    public double compressedMb;
    public double uncompressedMb;
    public double compressionRatio;
    public double cpuMinPct;
    public double cpuMaxPct;
    public double cpuAvgPct;
    public double memMinMb;
    public double memMaxMb;
    public double memAvgMb;
    public double memMinPct;
    public double memMaxPct;
    public double memAvgPct;
    public double diskBeforeMb;
    public double diskAfterMb;
    public double diskDeltaMb;
    public double diskUsedPct;
}
