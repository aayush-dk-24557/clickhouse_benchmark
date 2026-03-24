package com.benchmark.generator;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class DataGenerator {

    private static final String ALPHANUMERIC =
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    private static final DateTimeFormatter DATE_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter DATETIME_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DATETIME64_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private static final LocalDate DATE_MIN = LocalDate.of(2000, 1, 1);
    private static final LocalDate DATE_MAX = LocalDate.of(2025, 12, 31);
    private static final long DATE_RANGE_DAYS =
            DATE_MIN.until(DATE_MAX, java.time.temporal.ChronoUnit.DAYS);

    private static final LocalDate DATE32_MIN = LocalDate.of(1970, 1, 1);
    private static final LocalDate DATE32_MAX = LocalDate.of(2100, 12, 31);
    private static final long DATE32_RANGE_DAYS =
            DATE32_MIN.until(DATE32_MAX, java.time.temporal.ChronoUnit.DAYS);

    private static final LocalDateTime DT_MIN = LocalDateTime.of(2000, 1, 1, 0, 0, 0);
    private static final long DT_RANGE_SECONDS = 25L * 365 * 24 * 3600; // ~25 years

    // Seed map: type name -> seed value
    private final Map<String, Long> seedMap = new HashMap<>();
    private Random random;

    // Low cardinality pool: key = type name, value = pool of strings
    private final Map<String, List<String>> lcPool = new HashMap<>();

    public DataGenerator() {
        // Pre-populate seed map using hashCode of type name
        String[] types = {
                "UInt8", "UInt16", "UInt32", "UInt64",
                "Int8", "Int16", "Int32", "Int64",
                "Float32", "Float64",
                "Date", "Date32", "DateTime", "DateTime64(3)",
                "Decimal32(2)", "Decimal64(2)",
                "Enum8", "IPv4", "String", "LowCardinality(String)"
        };
        for (String t : types) {
            seedMap.put(t, (long) t.hashCode());
        }
        this.random = new Random();
    }

    /**
     * Reset the random seed for a given data type name.
     * Also clears any cached low-cardinality pool for this type.
     */
    public void resetSeed(String dataTypeName) {
        long seed = seedMap.getOrDefault(dataTypeName, (long) dataTypeName.hashCode());
        this.random = new Random(seed);
        lcPool.remove(dataTypeName);
    }

    /**
     * Generate a batch of values as SQL literals for the given ClickHouse type.
     * The type name passed here is the DataTypeInfo.typeName (e.g., "UInt64", "Enum8", etc.).
     */
    public List<String> generateBatch(String clickHouseType, int batchSize) {
        List<String> values = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            values.add(generateValue(clickHouseType));
        }
        return values;
    }

    private String generateValue(String type) {
        switch (type) {
            case "UInt8":
                return String.valueOf(random.nextInt(256)); // 0-255
            case "UInt16":
                return String.valueOf(random.nextInt(65536)); // 0-65535
            case "UInt32": {
                long val = (long) (random.nextDouble() * 4294967295L);
                return String.valueOf(val);
            }
            case "UInt64": {
                long val = random.nextLong() & Long.MAX_VALUE;
                return String.valueOf(val);
            }
            case "Int8":
                return String.valueOf((byte) (random.nextInt(256) - 128));
            case "Int16":
                return String.valueOf((short) (random.nextInt(65536) - 32768));
            case "Int32":
                return String.valueOf(random.nextInt());
            case "Int64":
                return String.valueOf(random.nextLong());
            case "Float32":
                return String.valueOf(random.nextFloat() * 1000000f - 500000f);
            case "Float64":
                return String.valueOf(random.nextDouble() * 1000000.0 - 500000.0);
            case "Date": {
                long days = (long) (random.nextDouble() * DATE_RANGE_DAYS);
                return "'" + DATE_MIN.plusDays(days).format(DATE_FMT) + "'";
            }
            case "Date32": {
                long days = (long) (random.nextDouble() * DATE32_RANGE_DAYS);
                return "'" + DATE32_MIN.plusDays(days).format(DATE_FMT) + "'";
            }
            case "DateTime": {
                long secs = (long) (random.nextDouble() * DT_RANGE_SECONDS);
                return "'" + DT_MIN.plusSeconds(secs).format(DATETIME_FMT) + "'";
            }
            case "DateTime64(3)": {
                long secs = (long) (random.nextDouble() * DT_RANGE_SECONDS);
                long millis = random.nextInt(1000);
                LocalDateTime dt = DT_MIN.plusSeconds(secs);
                String base = dt.format(DATETIME_FMT);
                String ms = String.format("%03d", millis);
                return "'" + base + "." + ms + "'";
            }
            case "Decimal32(2)": {
                // Range: -99999.99 to 99999.99
                double val = random.nextDouble() * 199999.98 - 99999.99;
                BigDecimal bd = new BigDecimal(val).setScale(2, RoundingMode.HALF_UP);
                return bd.toPlainString();
            }
            case "Decimal64(2)": {
                // Range: -9999999999999.99 to 9999999999999.99
                double val = random.nextDouble() * 19999999999999.98 - 9999999999999.99;
                BigDecimal bd = new BigDecimal(val).setScale(2, RoundingMode.HALF_UP);
                return bd.toPlainString();
            }
            case "Enum8": {
                int n = random.nextInt(10) + 1; // 1-10
                return "'v" + n + "'";
            }
            case "IPv4": {
                int a = random.nextInt(223) + 1; // 1-223
                int b = random.nextInt(256);
                int c = random.nextInt(256);
                int d = random.nextInt(256);
                return "'" + a + "." + b + "." + c + "." + d + "'";
            }
            case "String": {
                int len = 20 + random.nextInt(31); // 20-50
                StringBuilder sb = new StringBuilder(len);
                for (int i = 0; i < len; i++) {
                    sb.append(ALPHANUMERIC.charAt(random.nextInt(ALPHANUMERIC.length())));
                }
                return "'" + sb.toString().replace("'", "''") + "'";
            }
            case "LowCardinality(String)": {
                // Ensure pool is initialized
                if (!lcPool.containsKey(type)) {
                    initLcPool(type);
                }
                List<String> pool = lcPool.get(type);
                String s = pool.get(random.nextInt(pool.size()));
                return "'" + s.replace("'", "''") + "'";
            }
            default:
                throw new IllegalArgumentException("Unknown type: " + type);
        }
    }

    private void initLcPool(String type) {
        List<String> pool = new ArrayList<>(1000);
        for (int i = 0; i < 1000; i++) {
            int len = 20 + random.nextInt(31); // 20-50
            StringBuilder sb = new StringBuilder(len);
            for (int j = 0; j < len; j++) {
                sb.append(ALPHANUMERIC.charAt(random.nextInt(ALPHANUMERIC.length())));
            }
            pool.add(sb.toString());
        }
        lcPool.put(type, pool);
    }
}
