package com.benchmark.generator;

import com.benchmark.config.BenchmarkConfig;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
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

    // Ordered 10-year range constants
    private static final LocalDate ORDERED_START_DATE = LocalDate.of(2015, 1, 1);
    private static final LocalDate ORDERED_END_DATE = LocalDate.of(2024, 12, 31);
    private static final long ORDERED_TOTAL_DAYS =
            ORDERED_START_DATE.until(ORDERED_END_DATE, ChronoUnit.DAYS);

    private static final LocalDateTime ORDERED_START_DT = LocalDateTime.of(2015, 1, 1, 0, 0, 0);
    private static final LocalDateTime ORDERED_END_DT = LocalDateTime.of(2024, 12, 31, 23, 59, 59);
    private static final long ORDERED_TOTAL_SECONDS =
            ORDERED_START_DT.until(ORDERED_END_DT, ChronoUnit.SECONDS);
    private static final long ORDERED_TOTAL_MILLIS = ORDERED_TOTAL_SECONDS * 1000L;

    // Epoch ms start for ordered UInt64
    private static final long EPOCH_MS_START = 1700000000000L;

    private Random random;

    // Stateful counters for ordered generation — reset per resetSeed()
    private long currentIndex = 0;       // row index for Date/DateTime ordered types
    private long currentEpoch = EPOCH_MS_START;  // for UInt64 ordered_epoch_ms

    // Low-cardinality string pool: keyed by pool size
    private final java.util.Map<Integer, List<String>> lcPoolCache = new java.util.HashMap<>();
    private List<String> currentLcPool = null;

    public DataGenerator() {
        this.random = new Random();
    }

    /**
     * Reset the random seed for a given data type + data property combination.
     * Also resets stateful counters (currentIndex, currentEpoch) and clears LC pool reference.
     */
    public void resetSeed(String dataTypeName, String dataProperty) {
        long seed = (long) (dataTypeName + "_" + dataProperty).hashCode();
        this.random = new Random(seed);
        this.currentIndex = 0;
        this.currentEpoch = EPOCH_MS_START;
        this.currentLcPool = null;
    }

    /**
     * Generate a batch of values as SQL literals for the given ClickHouse type + data property.
     */
    public List<String> generateBatch(String clickHouseType, String dataProperty, int batchSize) {
        // For LowCardinality(String), ensure pool is initialized once per type+property combo.
        // The pool is built with a dedicated Random seeded from the type+property so it doesn't
        // consume entries from the main random stream, making the cache safe to reuse.
        if (clickHouseType.equals("LowCardinality(String)") && currentLcPool == null) {
            int poolSize = parseLcPoolSize(dataProperty);
            currentLcPool = lcPoolCache.computeIfAbsent(poolSize, this::buildLcPool);
        }

        List<String> values = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            values.add(generateValue(clickHouseType, dataProperty));
        }
        return values;
    }

    private String generateValue(String type, String property) {
        switch (type) {
            case "UInt8":
                return generateUInt8(property);
            case "UInt16":
                return generateUInt16(property);
            case "UInt32":
                return generateUInt32(property);
            case "UInt64":
                return generateUInt64(property);
            case "Int8":
                return generateInt8(property);
            case "Int16":
                return generateInt16(property);
            case "Int32":
                return generateInt32(property);
            case "Int64":
                return generateInt64(property);
            case "Float32":
                return generateFloat32(property);
            case "Float64":
                return generateFloat64(property);
            case "Date":
                return generateDate(property);
            case "Date32":
                return generateDate32(property);
            case "DateTime":
                return generateDateTime(property);
            case "DateTime64(3)":
                return generateDateTime64(property);
            case "Decimal32(2)":
                return generateDecimal32(property);
            case "Decimal64(2)":
                return generateDecimal64(property);
            case "Enum8":
                return generateEnum8(property);
            case "IPv4":
                return generateIPv4();
            case "String":
                return generateString(property);
            case "LowCardinality(String)":
                return generateLowCardinalityString();
            default:
                throw new IllegalArgumentException("Unknown type: " + type);
        }
    }

    // ---- UInt types ----

    private String generateUInt8(String property) {
        switch (property) {
            case "0-20":   return String.valueOf(random.nextInt(21));
            case "0-100":  return String.valueOf(random.nextInt(101));
            default:       return String.valueOf(random.nextInt(256)); // 0-255
        }
    }

    private String generateUInt16(String property) {
        switch (property) {
            case "0-100":   return String.valueOf(random.nextInt(101));
            case "0-500":   return String.valueOf(random.nextInt(501));
            case "0-5000":  return String.valueOf(random.nextInt(5001));
            default:        return String.valueOf(random.nextInt(65536)); // 0-65535
        }
    }

    private String generateUInt32(String property) {
        switch (property) {
            case "0-5000":   return String.valueOf((long) random.nextInt(5001));
            case "0-50000":  return String.valueOf((long) random.nextInt(50001));
            default:         return String.valueOf((long) (random.nextDouble() * 4294967295L)); // full range
        }
    }

    private String generateUInt64(String property) {
        if ("ordered_epoch_ms".equals(property)) {
            currentEpoch += random.nextInt(100) + 1;
            return String.valueOf(currentEpoch);
        }
        // random
        return String.valueOf(random.nextLong() & Long.MAX_VALUE);
    }

    // ---- Int types ----

    private String generateInt8(String property) {
        switch (property) {
            case "-10_to_10":    return String.valueOf(random.nextInt(21) - 10);
            case "-50_to_50":    return String.valueOf(random.nextInt(101) - 50);
            default:             return String.valueOf((byte) (random.nextInt(256) - 128)); // full range
        }
    }

    private String generateInt16(String property) {
        switch (property) {
            case "-100_to_100":   return String.valueOf(random.nextInt(201) - 100);
            case "-1000_to_1000": return String.valueOf(random.nextInt(2001) - 1000);
            default:              return String.valueOf((short) (random.nextInt(65536) - 32768)); // full range
        }
    }

    private String generateInt32(String property) {
        if ("-50000_to_50000".equals(property)) {
            return String.valueOf(random.nextInt(100001) - 50000);
        }
        // full_range
        return String.valueOf(random.nextInt());
    }

    private String generateInt64(String property) {
        if ("-1000000_to_1000000".equals(property)) {
            return String.valueOf((long) (random.nextDouble() * 2000001) - 1000000);
        }
        // full_range
        return String.valueOf(random.nextLong());
    }

    // ---- Float types ----

    private String generateFloat32(String property) {
        switch (property) {
            case "0.0_to_1.0":    return String.valueOf(random.nextFloat());
            case "-1000_to_1000": return String.valueOf(random.nextFloat() * 2000f - 1000f);
            default:              return String.valueOf(random.nextFloat() * 1000000f - 500000f); // full_range
        }
    }

    private String generateFloat64(String property) {
        switch (property) {
            case "0.0_to_1.0":        return String.valueOf(random.nextDouble());
            case "-100000_to_100000": return String.valueOf(random.nextDouble() * 200000.0 - 100000.0);
            default:                  return String.valueOf(random.nextDouble() * 1000000.0 - 500000.0); // full_range
        }
    }

    // ---- Date/DateTime (ordered, stateful) ----

    private String generateDate(String property) {
        // ordered_10yr: stateful by currentIndex
        long rowIndex = currentIndex++;
        long totalRows = BenchmarkConfig.TOTAL_ROWS;
        long dayOffset = rowIndex * ORDERED_TOTAL_DAYS / totalRows;
        return "'" + ORDERED_START_DATE.plusDays(dayOffset).format(DATE_FMT) + "'";
    }

    private String generateDate32(String property) {
        long rowIndex = currentIndex++;
        long totalRows = BenchmarkConfig.TOTAL_ROWS;
        long dayOffset = rowIndex * ORDERED_TOTAL_DAYS / totalRows;
        return "'" + ORDERED_START_DATE.plusDays(dayOffset).format(DATE_FMT) + "'";
    }

    private String generateDateTime(String property) {
        long rowIndex = currentIndex++;
        long totalRows = BenchmarkConfig.TOTAL_ROWS;
        long secOffset = rowIndex * ORDERED_TOTAL_SECONDS / totalRows;
        return "'" + ORDERED_START_DT.plusSeconds(secOffset).format(DATETIME_FMT) + "'";
    }

    private String generateDateTime64(String property) {
        long rowIndex = currentIndex++;
        long totalRows = BenchmarkConfig.TOTAL_ROWS;
        long millisOffset = rowIndex * ORDERED_TOTAL_MILLIS / totalRows;
        LocalDateTime dt = ORDERED_START_DT.plus(millisOffset, ChronoUnit.MILLIS);
        return "'" + dt.format(DATETIME64_FMT) + "'";
    }

    // ---- Decimal types ----

    private String generateDecimal32(String property) {
        double val;
        switch (property) {
            case "0_to_100":
                val = random.nextDouble() * 100.0;
                break;
            case "-10000_to_10000":
                val = random.nextDouble() * 20000.0 - 10000.0;
                break;
            default: // full_range
                val = random.nextDouble() * 199999.98 - 99999.99;
                break;
        }
        return new BigDecimal(val).setScale(2, RoundingMode.HALF_UP).toPlainString();
    }

    private String generateDecimal64(String property) {
        double val;
        switch (property) {
            case "0_to_10000":
                val = random.nextDouble() * 10000.0;
                break;
            case "-1000000_to_1000000":
                val = random.nextDouble() * 2000000.0 - 1000000.0;
                break;
            default: // full_range
                val = random.nextDouble() * 19999999999999.98 - 9999999999999.99;
                break;
        }
        return new BigDecimal(val).setScale(2, RoundingMode.HALF_UP).toPlainString();
    }

    // ---- Enum8 ----

    private String generateEnum8(String property) {
        // property like "10_constants", "20_constants", etc.
        int count = parseEnumCount(property);
        int n = random.nextInt(count) + 1;
        return "'v" + n + "'";
    }

    private int parseEnumCount(String property) {
        // "10_constants" -> 10
        try {
            return Integer.parseInt(property.replace("_constants", ""));
        } catch (NumberFormatException e) {
            return 10; // default fallback
        }
    }

    // ---- IPv4 ----

    private String generateIPv4() {
        int a = random.nextInt(223) + 1; // 1-223
        int b = random.nextInt(256);
        int c = random.nextInt(256);
        int d = random.nextInt(256);
        return "'" + a + "." + b + "." + c + "." + d + "'";
    }

    // ---- String ----

    private String generateString(String property) {
        int len;
        switch (property) {
            case "len_20_50":   len = 20 + random.nextInt(31);  break;
            case "len_50_100":  len = 50 + random.nextInt(51);  break;
            case "len_100_150": len = 100 + random.nextInt(51); break;
            default:            len = 150 + random.nextInt(101); break; // len_150_250
        }
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            sb.append(ALPHANUMERIC.charAt(random.nextInt(ALPHANUMERIC.length())));
        }
        return "'" + sb.toString().replace("'", "''") + "'";
    }

    // ---- LowCardinality(String) ----

    private String generateLowCardinalityString() {
        String s = currentLcPool.get(random.nextInt(currentLcPool.size()));
        return "'" + s.replace("'", "''") + "'";
    }

    private int parseLcPoolSize(String property) {
        // "50_distinct" -> 50, "10000_distinct" -> 10000
        try {
            return Integer.parseInt(property.replace("_distinct", ""));
        } catch (NumberFormatException e) {
            return 50; // default fallback
        }
    }

    private List<String> buildLcPool(int poolSize) {
        // Use a dedicated Random seeded by pool size so pool contents are deterministic
        // and do not consume entries from the main random stream.
        Random poolRandom = new Random((long) poolSize * 31337L);
        List<String> pool = new ArrayList<>(poolSize);
        for (int i = 0; i < poolSize; i++) {
            int len = 20 + poolRandom.nextInt(31); // 20-50
            StringBuilder sb = new StringBuilder(len);
            for (int j = 0; j < len; j++) {
                sb.append(ALPHANUMERIC.charAt(poolRandom.nextInt(ALPHANUMERIC.length())));
            }
            pool.add(sb.toString());
        }
        return pool;
    }
}
