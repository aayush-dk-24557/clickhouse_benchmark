package com.benchmark.model;

public class DataTypeInfo {
    public String typeName;       // e.g., "UInt64"
    public String fullTypeDef;    // e.g., "UInt64" or "Enum8('v1'=1,...,'v10'=10)"
    public boolean supportsDelta;
    public boolean supportsDoubleDelta;
    public boolean supportsT64;
    public long seed;             // fixed seed for data generation

    public DataTypeInfo(String typeName, String fullTypeDef,
                        boolean supportsDelta, boolean supportsDoubleDelta,
                        boolean supportsT64, long seed) {
        this.typeName = typeName;
        this.fullTypeDef = fullTypeDef;
        this.supportsDelta = supportsDelta;
        this.supportsDoubleDelta = supportsDoubleDelta;
        this.supportsT64 = supportsT64;
        this.seed = seed;
    }
}
