package com.hgx.dbscan.algorithm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DBSCAN {

    protected double eps;
    protected int minPts;
    protected int minClusterSize;
    protected DistanceMetric explicitMetric;

    public DBSCAN(double eps, int minPts, int minClusterSize) {
        this(eps, minPts, minClusterSize, null);
    }

    public DBSCAN(double eps, int minPts, int minClusterSize, DistanceMetric metric) {
        if (eps <= 0) {
            throw new IllegalArgumentException("eps must be positive");
        }
        if (minPts < 1) {
            throw new IllegalArgumentException("minPts must be at least 1");
        }
        if (minClusterSize < 1) {
            throw new IllegalArgumentException("minClusterSize must be at least 1");
        }
        this.eps = eps;
        this.minPts = minPts;
        this.minClusterSize = minClusterSize;
        this.explicitMetric = metric;
    }

    public List<List<Point>> cluster(List<Point> points) {
        if (points == null || points.isEmpty()) {
            return new ArrayList<>();
        }

        // 为每个点分配 ID，便于使用 BitSet 跟踪
        for (int i = 0; i < points.size(); i++) {
            points.get(i).setId(i);
            points.get(i).setVisited(false);
            points.get(i).setClusterId(-1);
        }

        // 确定度量标准并执行投影（针对地理坐标）
        DistanceMetric metric = explicitMetric;
        if (metric == null) {
            if (points.size() > 0 && isGeoCoordinates(points.get(0).getCoordinates(), points.get(0).getCoordinates())) {
                projectPointsToKm(points);
                metric = new DistanceMetric.EuclideanMetric();
            } else {
                metric = new DistanceMetric.EuclideanMetric();
            }
        }

        // 构建KD树
        KDTree kdTree = new KDTree(points.get(0).getCoordinates().length, metric);
        kdTree.build(points);

        List<List<Point>> clusters = Collections.synchronizedList(new ArrayList<>());
        java.util.concurrent.atomic.AtomicInteger clusterIdCounter = new java.util.concurrent.atomic.AtomicInteger(0);

        // 改为顺序流处理种子点，避免多个线程竞争同一个点导致集群分裂。
        // 真正的性能瓶颈在于 KD-Tree 查询，而非主循环逻辑。
        for (Point point : points) {
            if (!point.isVisited()) {
                point.setVisited(true);
                List<Point> neighbors = kdTree.rangeQuery(point, eps);

                if (neighbors.size() >= minPts) {
                    List<Point> cluster = new ArrayList<>();
                    int currentClusterId = clusterIdCounter.getAndIncrement();
                    expandCluster(kdTree, point, neighbors, cluster, currentClusterId);
                    clusters.add(cluster);
                }
            }
        }

        // 过滤掉小于最小聚类大小的集群
        List<List<Point>> filteredClusters = new ArrayList<>();
        for (List<Point> cluster : clusters) {
            if (cluster.size() >= minClusterSize) {
                filteredClusters.add(cluster);
            } else {
                cluster.forEach(p -> p.setClusterId(-1));
            }
        }

        return filteredClusters;
    }

    /**
     * 城市级投影优化：将经纬度投影到以公里为单位的平面，以提高欧氏距离计算的精度和速度
     */
    protected void projectPointsToKm(List<Point> points) {
        if (points.isEmpty()) return;

        // 借鉴 Rust 版：使用全量数据的平均纬度作为投影基准，减少畸变
        double sumLat = 0;
        for (Point p : points) {
            sumLat += p.getCoordinates()[1];
        }
        double avgLat = sumLat / points.size();

        double latKmPerDegree = 111.32;
        double lngKmPerDegree = 111.32 * Math.cos(Math.toRadians(avgLat));

        for (Point p : points) {
            double[] coords = p.getCoordinates();
            double[] projected = new double[2];
            projected[0] = coords[0] * lngKmPerDegree; // X (Longitude to KM)
            projected[1] = coords[1] * latKmPerDegree; // Y (Latitude to KM)
            p.setProjectedCoordinates(projected);
        }
    }

    protected DistanceMetric determineMetric(List<Point> points) {
        if (points.size() > 0) {
            double[] coords = points.get(0).getCoordinates();
            if (coords.length == 2 && isGeoCoordinates(coords, coords)) {
                return new DistanceMetric.HaversineMetric();
            }
        }
        return new DistanceMetric.EuclideanMetric();
    }

    protected boolean isGeoCoordinates(double[] coord1, double[] coord2) {
        // 检查坐标是否在经纬度范围内
        return Math.abs(coord1[0]) <= 180 && Math.abs(coord1[1]) <= 90 &&
               Math.abs(coord2[0]) <= 180 && Math.abs(coord2[1]) <= 90;
    }

    private void expandCluster(KDTree kdTree, Point point, List<Point> neighbors, List<Point> cluster, int clusterId) {
        cluster.add(point);
        point.setClusterId(clusterId);

        java.util.BitSet inQueue = new java.util.BitSet();
        for (Point n : neighbors) {
            inQueue.set(n.getId());
        }

        List<Point> queue = new ArrayList<>(neighbors);
        int index = 0;
        while (index < queue.size()) {
            Point neighbor = queue.get(index++);
            
            if (!neighbor.isVisited()) {
                neighbor.setVisited(true);
                List<Point> neighborNeighbors = kdTree.rangeQuery(neighbor, eps);
                if (neighborNeighbors.size() >= minPts) {
                    for (Point nn : neighborNeighbors) {
                        if (!inQueue.get(nn.getId())) {
                            inQueue.set(nn.getId());
                            queue.add(nn);
                        }
                    }
                }
            }
            
            if (neighbor.getClusterId() == -1) {
                cluster.add(neighbor);
                neighbor.setClusterId(clusterId);
            }
        }
    }

    private double calculateDistance(Point p1, Point p2) {
        DistanceMetric metric = determineMetric(java.util.Arrays.asList(p1, p2));
        return metric.calculate(p1.getClusteringCoordinates(), p2.getClusteringCoordinates());
    }

    public static class Point {
        private double[] coordinates;
        private double[] projectedCoordinates;
        private boolean visited;
        private int clusterId;
        private int id;
        private long timestamp; // 新增时间戳字段
        private java.util.Map<String, String> extraData;

        public Point(double[] coordinates) {
            this(coordinates, 0, null); // 默认时间戳为0
        }

        public Point(double[] coordinates, long timestamp) {
            this(coordinates, timestamp, null);
        }

        public Point(double[] coordinates, java.util.Map<String, String> extraData) {
            this(coordinates, 0, extraData); // 默认时间戳为0
        }

        public Point(double[] coordinates, long timestamp, java.util.Map<String, String> extraData) {
            this.coordinates = coordinates;
            this.timestamp = timestamp;
            this.visited = false;
            this.clusterId = -1;
            this.extraData = extraData;
        }

        public java.util.Map<String, String> getExtraData() {
            return extraData;
        }

        public void setExtraData(java.util.Map<String, String> extraData) {
            this.extraData = extraData;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public double[] getCoordinates() {
            return coordinates;
        }

        public double[] getOriginalCoordinates() {
            return coordinates;
        }

        /**
         * 算法内部使用：返回用于计算距离的坐标（可能是投影后的公里坐标）
         */
        public double[] getClusteringCoordinates() {
            return projectedCoordinates != null ? projectedCoordinates : coordinates;
        }

        public void setProjectedCoordinates(double[] projectedCoordinates) {
            this.projectedCoordinates = projectedCoordinates;
        }

        public double getX() {
            return coordinates.length > 0 ? coordinates[0] : 0;
        }

        public double getY() {
            return coordinates.length > 1 ? coordinates[1] : 0;
        }

        public boolean isVisited() {
            return visited;
        }

        public void setVisited(boolean visited) {
            this.visited = visited;
        }

        public int getClusterId() {
            return clusterId;
        }

        public void setClusterId(int clusterId) {
            this.clusterId = clusterId;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("Point{");
            double[] coords = getOriginalCoordinates();
            for (int i = 0; i < coords.length; i++) {
                sb.append("coord").append(i).append("=");
                sb.append(coords[i]);
                if (i < coords.length - 1) {
                    sb.append(", ");
                }
            }
            sb.append(", clusterId=").append(clusterId).append('}');
            return sb.toString();
        }
    }
}
