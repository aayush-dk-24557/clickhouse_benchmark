package com.benchmark.model;

public class Combination {
    public String dataType;
    public String dataProperty;    // e.g., "0-20", "ordered_10yr", "50_distinct"
    public String fullTypeDef;
    public String preprocessor;
    public String codec;
    public String tableName;

    public Combination(String dataType, String dataProperty, String fullTypeDef,
                       String preprocessor, String codec, String tableName) {
        this.dataType = dataType;
        this.dataProperty = dataProperty;
        this.fullTypeDef = fullTypeDef;
        this.preprocessor = preprocessor;
        this.codec = codec;
        this.tableName = tableName;
    }
}
