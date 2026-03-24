package com.benchmark.model;

public class Combination {
    public String dataType;     // e.g., "UInt64"
    public String fullTypeDef;  // e.g., "Enum8('v1'=1,...)" - the full SQL type
    public String preprocessor; // "None", "Delta", "DoubleDelta", "T64"
    public String codec;        // "LZ4", "ZSTD(1)"
    public String tableName;    // e.g., "test_uint64_delta_zstd1"

    public Combination(String dataType, String fullTypeDef, String preprocessor,
                       String codec, String tableName) {
        this.dataType = dataType;
        this.fullTypeDef = fullTypeDef;
        this.preprocessor = preprocessor;
        this.codec = codec;
        this.tableName = tableName;
    }
}
