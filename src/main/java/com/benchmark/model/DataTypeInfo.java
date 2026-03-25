package com.benchmark.model;

import java.util.List;

public class DataTypeInfo {
    public String typeName;            // e.g., "UInt64"
    public String fullTypeDef;         // e.g., "UInt64" or "Enum8('v1'=1,...,'v10'=10)" -- base definition
    public boolean supportsDelta;
    public boolean supportsDoubleDelta;
    public boolean supportsT64;
    public long seed;
    public List<String> dataProperties; // e.g., ["0-20", "0-100", "0-255"]

    public DataTypeInfo(String typeName, String fullTypeDef,
                        boolean supportsDelta, boolean supportsDoubleDelta,
                        boolean supportsT64, long seed,
                        List<String> dataProperties) {
        this.typeName = typeName;
        this.fullTypeDef = fullTypeDef;
        this.supportsDelta = supportsDelta;
        this.supportsDoubleDelta = supportsDoubleDelta;
        this.supportsT64 = supportsT64;
        this.seed = seed;
        this.dataProperties = dataProperties;
    }
}
