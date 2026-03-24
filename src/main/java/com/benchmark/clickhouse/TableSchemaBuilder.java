package com.benchmark.clickhouse;

public class TableSchemaBuilder {

    /**
     * Build the CREATE TABLE DDL for a benchmark table.
     */
    public static String buildCreateTable(String tableName, String clickHouseType,
                                          String preprocessor, String codec) {
        String codecClause = buildCodecClause(preprocessor, codec);
        return "CREATE TABLE IF NOT EXISTS " + tableName + " (\n" +
                "    id UInt64,\n" +
                "    value " + clickHouseType + " CODEC(" + codecClause + ")\n" +
                ") ENGINE = MergeTree()\n" +
                "ORDER BY id";
    }

    /**
     * Build the CODEC clause contents (without outer parentheses).
     * preprocessor can be null, empty, or "None" for no preprocessor.
     */
    public static String buildCodecClause(String preprocessor, String codec) {
        if (preprocessor == null || preprocessor.isEmpty() || preprocessor.equalsIgnoreCase("None")) {
            return codec;
        }
        return preprocessor + ", " + codec;
    }
}
