package com.hgx.dbscan.algorithm;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class STDBSCAN extends DBSCAN {

    private final long timeEps; // 时间阈值（毫秒）
    private KDTree kdTree; // 空间索引
    private Point[] timeIndex; // 时间排序索引

    public STDBSCAN(double eps, int minPts, int minClusterSize, long timeEps) {
        super(eps, minPts, minClusterSize);
        if (timeEps <= 0) {
            throw new IllegalArgumentException("timeEps must be positive");
        }
        this.timeEps = timeEps;
    }

    public STDBSCAN(double eps, int minPts, int minClusterSize, long timeEps, DistanceMetric metric) {
        super(eps, minPts, minClusterSize, metric);
        if (timeEps <= 0) {
            throw new IllegalArgumentException("timeEps must be positive");
        }
        this.timeEps = timeEps;
    }

    @Override
    public List<List<Point>> cluster(List<Point> points) {
        if (points == null || points.isEmpty()) {
            return new ArrayList<>();
        }

        for (int i = 0; i < points.size(); i++) {
            points.get(i).setId(i);
            points.get(i).setVisited(false);
            points.get(i).setClusterId(-1);
        }

        DistanceMetric metric = explicitMetric;
        if (metric == null) {
            double[] coords = points.get(0).getCoordinates();
            if (isGeoCoordinates(coords, coords)) {
                projectPointsToKm(points);
            }
            metric = new DistanceMetric.EuclideanMetric();
        }

        int dims = points.get(0).getClusteringCoordinates().length;
        kdTree = new KDTree(dims, metric);
        kdTree.build(points);

        // 时间排序索引
        timeIndex = points.toArray(new Point[0]);
        Arrays.sort(timeIndex, Comparator.comparingLong(Point::getTimestamp));

        final double spatialThreshold = metric.isSquared() ? eps * eps : eps;
        final DistanceMetric finalMetric = metric;

        List<List<Point>> clusters = new ArrayList<>();
        int clusterIdCounter = 0;

        for (Point point : points) {
            if (!point.isVisited()) {
                point.setVisited(true);
                List<Point> neighbors = rangeQueryST(point, spatialThreshold, finalMetric);

                if (neighbors.size() >= minPts) {
                    List<Point> cluster = new ArrayList<>();
                    expandClusterST(point, neighbors, cluster, clusterIdCounter++, spatialThreshold, finalMetric);
                    clusters.add(cluster);
                }
            }
        }

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
     * 时空范围查询
     *
     * <p>优化点：
     * <ul>
     *   <li>时间过滤 + 空间KD-Tree交集</li>
     *   <li>小规模候选集线性扫描，避免KD-Tree开销</li>
     *   <li>利用时间单调性：二分定位 + 有界扫描</li>
     * </ul>
     */
    private List<Point> rangeQueryST(Point centerPoint, double spatialThreshold, DistanceMetric metric) {
        long tMin = centerPoint.getTimestamp() - timeEps;
        long tMax = centerPoint.getTimestamp() + timeEps;

        int lo = lowerBound(timeIndex, tMin);
        int hi = upperBound(timeIndex, tMax);
        int candidateCount = hi - lo;

        // 小规模候选集：直接线性扫描（连续内存访问，缓存友好）
        if (candidateCount <= 32) {
            return linearScanCandidates(lo, hi, centerPoint, spatialThreshold, metric);
        }

        // 大规模候选集：KD-Tree + 时间交集
        Set<Integer> candidateIds = new HashSet<>(candidateCount);
        for (int i = lo; i < hi; i++) {
            candidateIds.add(timeIndex[i].getId());
        }

        List<Point> spatialCandidates = kdTree.rangeQuery(centerPoint, eps);

        List<Point> stNeighbors = new ArrayList<>();
        for (Point sp : spatialCandidates) {
            if (candidateIds.contains(sp.getId())) {
                stNeighbors.add(sp);
            }
        }

        return stNeighbors;
    }

    /**
     * 小规模候选集线性扫描
     */
    private List<Point> linearScanCandidates(int lo, int hi, Point centerPoint,
                                            double spatialThreshold, DistanceMetric metric) {
        List<Point> stNeighbors = new ArrayList<>(hi - lo);
        double[] centerCoords = centerPoint.getClusteringCoordinates();
        for (int i = lo; i < hi; i++) {
            Point candidate = timeIndex[i];
            double dist = metric.calculateDistanceOrSquared(centerCoords, candidate.getClusteringCoordinates());
            if (dist <= spatialThreshold) {
                stNeighbors.add(candidate);
            }
        }
        return stNeighbors;
    }

    /**
     * 集群扩展
     *
     * <p>优化：
     * <ul>
     *   <li>HashSet替代BitSet：避免大数组预分配，减少内存压力</li>
     *   <li>ArrayDeque作为FIFO队列</li>
     * </ul>
     */
    private void expandClusterST(Point point, List<Point> neighbors,
                                  List<Point> cluster, int clusterId,
                                  double spatialThreshold, DistanceMetric metric) {
        cluster.add(point);
        point.setClusterId(clusterId);

        Set<Integer> inQueue = new HashSet<>();
        for (Point n : neighbors) {
            inQueue.add(n.getId());
        }

        Deque<Point> queue = new ArrayDeque<>(neighbors);
        while (!queue.isEmpty()) {
            Point neighbor = queue.poll();

            if (!neighbor.isVisited()) {
                neighbor.setVisited(true);
                List<Point> neighborNeighbors = rangeQueryST(neighbor, spatialThreshold, metric);
                if (neighborNeighbors.size() >= minPts) {
                    for (Point nn : neighborNeighbors) {
                        if (!inQueue.contains(nn.getId())) {
                            inQueue.add(nn.getId());
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

    private int lowerBound(Point[] arr, long target) {
        int lo = 0, hi = arr.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (arr[mid].getTimestamp() < target) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    private int upperBound(Point[] arr, long target) {
        int lo = 0, hi = arr.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (arr[mid].getTimestamp() <= target) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }
}
