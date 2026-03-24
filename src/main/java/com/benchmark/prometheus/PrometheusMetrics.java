package com.benchmark.prometheus;

import com.benchmark.config.BenchmarkConfig;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class PrometheusMetrics {

    private static final Logger LOG = Logger.getLogger(PrometheusMetrics.class.getName());

    private final OkHttpClient client;
    private final String baseUrl;

    public PrometheusMetrics() {
        this.client = new OkHttpClient.Builder()
                .connectTimeout(15, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .build();
        this.baseUrl = BenchmarkConfig.PROMETHEUS_URL;
    }

    /**
     * Get CPU usage over time range.
     *
     * @return [minPct, maxPct, avgPct] or [-1,-1,-1] on failure
     */
    public double[] getCpuUsage(long startEpoch, long endEpoch) {
        try {
            String query = "100 - (avg(rate(node_cpu_seconds_total{mode=\"idle\"}[1m])) * 100)";
            List<Double> values = queryRangeSeries(query, startEpoch, endEpoch, "15s");
            if (values.isEmpty()) {
                LOG.warning("No CPU data from Prometheus");
                return new double[]{-1, -1, -1};
            }
            return computeStats(values);
        } catch (Exception e) {
            LOG.warning("Failed to get CPU metrics: " + e.getMessage());
            return new double[]{-1, -1, -1};
        }
    }

    /**
     * Get memory usage over time range.
     *
     * @return [minBytes, maxBytes, avgBytes, minPct, maxPct, avgPct] or [-1,...] on failure
     */
    public double[] getMemoryUsage(long startEpoch, long endEpoch) {
        try {
            // Try primary query first
            String usedQuery = "node_memory_total_bytes - node_memory_available_bytes";
            List<Double> usedValues = queryRangeSeries(usedQuery, startEpoch, endEpoch, "15s");

            if (usedValues.isEmpty()) {
                // macOS fallback
                usedQuery = "node_memory_total_bytes - node_memory_free_bytes";
                usedValues = queryRangeSeries(usedQuery, startEpoch, endEpoch, "15s");
            }

            if (usedValues.isEmpty()) {
                LOG.warning("No memory used data from Prometheus");
                return new double[]{-1, -1, -1, -1, -1, -1};
            }

            // Get total memory for percentage calculation
            String totalQuery = "node_memory_total_bytes";
            List<Double> totalValues = queryRangeSeries(totalQuery, startEpoch, endEpoch, "15s");

            double[] usedStats = computeStats(usedValues);
            double[] result = new double[6];
            result[0] = usedStats[0]; // minBytes
            result[1] = usedStats[1]; // maxBytes
            result[2] = usedStats[2]; // avgBytes

            if (!totalValues.isEmpty()) {
                double[] totalStats = computeStats(totalValues);
                double avgTotal = totalStats[2];
                if (avgTotal > 0) {
                    result[3] = (usedStats[0] / avgTotal) * 100.0; // minPct
                    result[4] = (usedStats[1] / avgTotal) * 100.0; // maxPct
                    result[5] = (usedStats[2] / avgTotal) * 100.0; // avgPct
                } else {
                    result[3] = -1;
                    result[4] = -1;
                    result[5] = -1;
                }
            } else {
                result[3] = -1;
                result[4] = -1;
                result[5] = -1;
            }
            return result;
        } catch (Exception e) {
            LOG.warning("Failed to get memory metrics: " + e.getMessage());
            return new double[]{-1, -1, -1, -1, -1, -1};
        }
    }

    /**
     * Get disk usage before and after the time range.
     *
     * @return [beforeBytes, afterBytes, deltaBytes, usedPct] or [-1,...] on failure
     */
    public double[] getDiskUsage(long startEpoch, long endEpoch) {
        try {
            String usedQuery = "node_filesystem_size_bytes{mountpoint=\"/\"} - node_filesystem_avail_bytes{mountpoint=\"/\"}";
            String totalQuery = "node_filesystem_size_bytes{mountpoint=\"/\"}";

            Double beforeUsed = queryInstant(usedQuery, startEpoch);
            Double afterUsed = queryInstant(usedQuery, endEpoch);
            Double totalBytes = queryInstant(totalQuery, endEpoch);

            double before = beforeUsed != null ? beforeUsed : -1;
            double after = afterUsed != null ? afterUsed : -1;
            double delta = (before >= 0 && after >= 0) ? after - before : -1;
            double pct = (after >= 0 && totalBytes != null && totalBytes > 0)
                    ? (after / totalBytes) * 100.0 : -1;

            return new double[]{before, after, delta, pct};
        } catch (Exception e) {
            LOG.warning("Failed to get disk metrics: " + e.getMessage());
            return new double[]{-1, -1, -1, -1};
        }
    }

    // ---- Private helpers ----

    private List<Double> queryRangeSeries(String query, long start, long end, String step) {
        try {
            String url = baseUrl + "/api/v1/query_range"
                    + "?query=" + encode(query)
                    + "&start=" + start
                    + "&end=" + end
                    + "&step=" + step;

            String json = httpGet(url);
            if (json == null) return new ArrayList<>();

            JsonObject root = JsonParser.parseString(json).getAsJsonObject();
            if (!"success".equals(root.get("status").getAsString())) return new ArrayList<>();

            JsonObject data = root.getAsJsonObject("data");
            JsonArray resultArray = data.getAsJsonArray("result");
            if (resultArray == null || resultArray.size() == 0) return new ArrayList<>();

            List<Double> values = new ArrayList<>();
            // Aggregate all series
            for (JsonElement elem : resultArray) {
                JsonArray pts = elem.getAsJsonObject().getAsJsonArray("values");
                if (pts == null) continue;
                for (JsonElement pt : pts) {
                    JsonArray pair = pt.getAsJsonArray();
                    String valStr = pair.get(1).getAsString();
                    try {
                        double v = Double.parseDouble(valStr);
                        if (Double.isFinite(v)) values.add(v);
                    } catch (NumberFormatException ignored) {
                    }
                }
            }
            return values;
        } catch (Exception e) {
            LOG.warning("Prometheus range query failed: " + e.getMessage());
            return new ArrayList<>();
        }
    }

    private Double queryInstant(String query, long time) {
        try {
            String url = baseUrl + "/api/v1/query"
                    + "?query=" + encode(query)
                    + "&time=" + time;

            String json = httpGet(url);
            if (json == null) return null;

            JsonObject root = JsonParser.parseString(json).getAsJsonObject();
            if (!"success".equals(root.get("status").getAsString())) return null;

            JsonObject data = root.getAsJsonObject("data");
            JsonArray resultArray = data.getAsJsonArray("result");
            if (resultArray == null || resultArray.size() == 0) return null;

            JsonElement first = resultArray.get(0);
            JsonArray value = first.getAsJsonObject().getAsJsonArray("value");
            if (value == null || value.size() < 2) return null;

            String valStr = value.get(1).getAsString();
            return Double.parseDouble(valStr);
        } catch (Exception e) {
            LOG.warning("Prometheus instant query failed: " + e.getMessage());
            return null;
        }
    }

    private String httpGet(String url) {
        Request request = new Request.Builder().url(url).get().build();
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) return null;
            return response.body() != null ? response.body().string() : null;
        } catch (IOException e) {
            LOG.warning("HTTP GET failed for " + url + ": " + e.getMessage());
            return null;
        }
    }

    private static String encode(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    private static double[] computeStats(List<Double> values) {
        double min = Double.MAX_VALUE, max = -Double.MAX_VALUE, sum = 0;
        for (double v : values) {
            if (v < min) min = v;
            if (v > max) max = v;
            sum += v;
        }
        double avg = sum / values.size();
        return new double[]{min, max, avg};
    }
}
