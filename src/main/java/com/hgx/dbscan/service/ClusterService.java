package com.hgx.dbscan.service;

import com.hgx.dbscan.algorithm.DBSCAN;
import com.hgx.dbscan.algorithm.HDBSCAN;
import com.hgx.dbscan.model.ClusterResponse;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

@Service
public class ClusterService {

    private static final Logger logger = LoggerFactory.getLogger(ClusterService.class);

    public ClusterResponse cluster(InputStream inputStream, double eps, int minPts, int minClusterSize, String lngField, String latField, String algorithm) throws Exception {
        logger.info("Starting {} clustering with eps={}, minPts={}, minClusterSize={}, lngField={}, latField={}", 
                algorithm, eps, minPts, minClusterSize, lngField, latField);
        
        try {
            // 解析CSV文件
            List<DBSCAN.Point> points = parseCSV(inputStream, lngField, latField);
            logger.info("Parsed {} points from CSV file", points.size());
            
            // 执行聚类算法
            List<List<DBSCAN.Point>> clusters;
            if ("hdbscan".equalsIgnoreCase(algorithm)) {
                HDBSCAN hdbscan = new HDBSCAN(eps, minPts, minClusterSize);
                clusters = hdbscan.cluster(points);
            } else {
                // 默认使用DBSCAN
                DBSCAN dbscan = new DBSCAN(eps, minPts, minClusterSize);
                clusters = dbscan.cluster(points);
            }
            logger.info("Clustering completed: {} clusters found", clusters.size());
            
            // 构建响应
            ClusterResponse response = buildResponse(points, clusters, minClusterSize);
            logger.info("Response built successfully");
            return response;
        } catch (Exception e) {
            logger.error("Error during clustering: {}", e.getMessage(), e);
            throw e;
        }
    }

    private List<DBSCAN.Point> parseCSV(InputStream inputStream, String lngField, String latField) throws Exception {
        List<DBSCAN.Point> points = new ArrayList<>();
        try (Reader reader = new InputStreamReader(inputStream);
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
            // 流式处理CSV记录
            csvParser.forEach(csvRecord -> {
                try {
                    // 尝试根据指定字段名解析经纬度
                    double lng, lat;
                    try {
                        lng = Double.parseDouble(csvRecord.get(lngField));
                        lat = Double.parseDouble(csvRecord.get(latField));
                    } catch (Exception e) {
                        // 如果指定字段不存在或解析失败，尝试解析所有数值字段
                        List<Double> coordinates = new ArrayList<>();
                        for (String value : csvRecord) {
                            try {
                                coordinates.add(Double.parseDouble(value));
                            } catch (NumberFormatException ex) {
                                // 跳过非数值字段
                                logger.debug("Skipping non-numeric field: {}", value);
                            }
                        }
                        
                        if (coordinates.size() >= 2) {
                            double[] coordArray = coordinates.stream().mapToDouble(Double::doubleValue).toArray();
                            points.add(new DBSCAN.Point(coordArray, csvRecord.toMap()));
                        } else {
                            logger.warn("Skipping record with insufficient numeric fields: {}", csvRecord);
                        }
                        return;
                    }
                    
                    // 使用经纬度创建点
                    double[] coordArray = {lng, lat};
                    points.add(new DBSCAN.Point(coordArray, csvRecord.toMap()));
                } catch (Exception e) {
                    logger.warn("Skipping invalid record: {}", csvRecord);
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

        // 3. 构建响应对象
        ClusterResponse response = new ClusterResponse();
        response.setTotalClusters(validClusters.size());
        response.setTotalPoints(points.size());
        
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
}
