package com.hgx.dbscan.service;

import com.hgx.dbscan.algorithm.DBSCAN;
import com.hgx.dbscan.algorithm.HDBSCAN;
import com.hgx.dbscan.algorithm.STDBSCAN;
import com.hgx.dbscan.model.ClusterResponse;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ClusterService {

    private static final Logger logger = LoggerFactory.getLogger(ClusterService.class);
    // DateTimeFormatter 是不可变且线程安全的，可安全地共享为静态字段
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // Cache for clustered data for download, keyed by a UUID
    private final Map<String, List<Map<String, String>>> clusterResultsCache = new ConcurrentHashMap<>();

    public ClusterResponse cluster(InputStream inputStream, double eps, int minPts, int minClusterSize, String lngField, String latField, String timeField, long timeEps, String algorithm) throws Exception {
        logger.info("Starting {} clustering with eps={}, minPts={}, minClusterSize={}, lngField={}, latField={}, timeField={}, timeEps={}", 
                algorithm, eps, minPts, minClusterSize, lngField, latField, timeField, timeEps);
        
        try {
            // 解析CSV文件
            List<DBSCAN.Point> points = parseCSV(inputStream, lngField, latField, timeField, algorithm);
            logger.info("Parsed {} points from CSV file", points.size());
            
            // 执行聚类算法
            List<List<DBSCAN.Point>> clusters;
            if ("hdbscan".equalsIgnoreCase(algorithm)) {
                HDBSCAN hdbscan = new HDBSCAN(eps, minPts, minClusterSize);
                clusters = hdbscan.cluster(points);
            } else if ("stdbscan".equalsIgnoreCase(algorithm)) {
                STDBSCAN stdbscan = new STDBSCAN(eps, minPts, minClusterSize, timeEps);
                clusters = stdbscan.cluster(points);
            } else {
                // 默认使用DBSCAN
                DBSCAN dbscan = new DBSCAN(eps, minPts, minClusterSize);
                clusters = dbscan.cluster(points);
            }
            logger.info("Clustering completed: {} raw clusters found", clusters.size());
            
            // 构建响应
            ClusterResponse response = buildResponse(points, clusters, minClusterSize);
            logger.info("Response built successfully");

            // Prepare data for download and store in cache
            List<Map<String, String>> dataForDownload = new ArrayList<>();
            for (DBSCAN.Point point : points) {
                dataForDownload.add(point.getExtraData());
            }
            String downloadId = UUID.randomUUID().toString();
            clusterResultsCache.put(downloadId, dataForDownload);
            response.setDownloadId(downloadId);

            return response;
        } catch (Exception e) {
            logger.error("Error during clustering: {}", e.getMessage(), e);
            throw e;
        }
    }

    private List<DBSCAN.Point> parseCSV(InputStream inputStream, String lngField, String latField, String timeField, String algorithm) throws Exception {
        List<DBSCAN.Point> points = new ArrayList<>();
        boolean isSTDBScan = "stdbscan".equalsIgnoreCase(algorithm);
        try (Reader reader = new InputStreamReader(inputStream);
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
            csvParser.forEach(csvRecord -> {
                try {
                    // Always check for lng and lat fields
                    if (!csvRecord.isMapped(lngField) || !csvRecord.isMapped(latField)) {
                        logger.warn("Skipping record due to missing required fields (lngField or latField): {}", csvRecord);
                        return;
                    }

                    double lng = Double.parseDouble(csvRecord.get(lngField));
                    double lat = Double.parseDouble(csvRecord.get(latField));
                    long timestamp = 0L; // Default timestamp for non-ST-DBSCAN or if timeField is not present/needed

                    if (isSTDBScan) {
                        if (!csvRecord.isMapped(timeField)) {
                            logger.warn("Skipping record for ST-DBSCAN due to missing timeField '{}': {}", timeField, csvRecord);
                            return;
                        }
                        try {
                            LocalDateTime ldt = LocalDateTime.parse(csvRecord.get(timeField), DATE_FORMAT);
                            timestamp = ldt.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                        } catch (DateTimeParseException e) {
                            logger.warn("Skipping record for ST-DBSCAN due to timeField '{}' format exception: {}", timeField, csvRecord, e);
                            return;
                        }
                    }

                    double[] coordArray = {lng, lat};
                    points.add(new DBSCAN.Point(coordArray, timestamp, csvRecord.toMap()));
                } catch (NumberFormatException e) {
                    logger.warn("Skipping record due to number format exception in coordinates: {}", csvRecord, e);
                } catch (Exception e) {
                    logger.warn("Skipping invalid record due to unexpected error: {}", csvRecord, e);
                }
            });
        }
        return points;
    }

    private ClusterResponse buildResponse(List<DBSCAN.Point> points, List<List<DBSCAN.Point>> clusters, int minClusterSize) {
        // 1. 统计原始集群大小，并决定哪些集群需要保留
        java.util.Map<Integer, Integer> oldToNewId = new java.util.HashMap<>();
        List<List<DBSCAN.Point>> validClusters = new ArrayList<>();
        int nextNewId = 0;

        for (int i = 0; i < clusters.size(); i++) {
            List<DBSCAN.Point> clusterPoints = clusters.get(i);
            if (clusterPoints.size() >= minClusterSize) {
                oldToNewId.put(i, nextNewId++);
                validClusters.add(clusterPoints);
            } else {
                // 如果集群太小，将其中的点重新标记为噪声 (-1)
                for (DBSCAN.Point p : clusterPoints) {
                    p.setClusterId(-1);
                }
            }
        }

        // 2. 更新所有点的 clusterId（确保与 validClusters 的索引一致）
        for (int i = 0; i < validClusters.size(); i++) {
            for (DBSCAN.Point p : validClusters.get(i)) {
                p.setClusterId(i);
            }
        }

        // Ensure all points have their final clusterId in extraData for download purposes
        for (DBSCAN.Point point : points) {
            point.getExtraData().put("clusterId", String.valueOf(point.getClusterId()));
        }

        // 3. 构建响应对象
        ClusterResponse response = new ClusterResponse();
        response.setTotalClusters(validClusters.size());
        response.setTotalPoints(points.size());
        logger.info("Building response: {} valid clusters after minClusterSize filtering", validClusters.size());

        // 统计噪声点并收集噪声点信息
        int noiseCount = 0;
        List<ClusterResponse.Point> noisePointsList = new ArrayList<>();
        for (DBSCAN.Point point : points) {
            if (point.getClusterId() == -1) {
                noiseCount++;
                noisePointsList.add(new ClusterResponse.Point(point.getCoordinates(), point.getExtraData()));
            }
        }
        response.setNoisePoints(noiseCount);
        response.setNoisePointsList(noisePointsList);

        // 构建最终集群详情
        List<ClusterResponse.Cluster> clusterList = new ArrayList<>();
        for (int i = 0; i < validClusters.size(); i++) {
            ClusterResponse.Cluster cluster = new ClusterResponse.Cluster();
            cluster.setClusterId(i);

            List<ClusterResponse.Point> pointList = new ArrayList<>();
            for (DBSCAN.Point point : validClusters.get(i)) {
                pointList.add(new ClusterResponse.Point(point.getCoordinates(), point.getExtraData()));
            }
            cluster.setPoints(pointList);
            clusterList.add(cluster);
        }
        response.setClusters(clusterList);

        return response;
    }

    public List<Map<String, String>> getClusteredData(String downloadId) {
        return clusterResultsCache.get(downloadId);
    }
}
