package com.hgx.dbscan.algorithm;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;

public class ST_DBSCAN extends DBSCAN {

    private final long timeEps; // 时间阈值（毫秒）

    public ST_DBSCAN(double eps, int minPts, int minClusterSize, long timeEps) {
        super(eps, minPts, minClusterSize);
        if (timeEps <= 0) {
            throw new IllegalArgumentException("timeEps must be positive");
        }
        this.timeEps = timeEps;
    }

    public ST_DBSCAN(double eps, int minPts, int minClusterSize, long timeEps, DistanceMetric metric) {
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

        // 为每个点分配 ID，初始化访问状态
        for (int i = 0; i < points.size(); i++) {
            points.get(i).setId(i);
            points.get(i).setVisited(false);
            points.get(i).setClusterId(-1);
        }

        // 确定度量标准并执行投影（针对地理坐标）
        // 修正：检查坐标范围使用第一个点自身，isGeoCoordinates 只需要一组坐标
        DistanceMetric metric = explicitMetric;
        if (metric == null) {
            double[] coords = points.get(0).getCoordinates();
            if (isGeoCoordinates(coords, coords)) {
                projectPointsToKm(points);
            }
            metric = new DistanceMetric.EuclideanMetric();
        }

        // 优化 1：构建时间排序索引（O(n log n)，一次性预处理）
        // 后续每次查询通过二分搜索在 O(log n) 内找到时间候选点
        Point[] sortedByTime = points.toArray(new Point[0]);
        Arrays.sort(sortedByTime, Comparator.comparingLong(Point::getTimestamp));

        // 预计算空间距离阈值（根据 metric 是否返回平方值）
        final double spatialThreshold = metric.isSquared() ? eps * eps : eps;
        final DistanceMetric finalMetric = metric;

        // 优化 2：使用普通 ArrayList + int 计数器（主循环是单线程的，无需线程安全容器）
        List<List<Point>> clusters = new ArrayList<>();
        int clusterIdCounter = 0;

        for (Point point : points) {
            if (!point.isVisited()) {
                point.setVisited(true);
                List<Point> neighbors = rangeQueryST(sortedByTime, point, spatialThreshold, finalMetric);

                if (neighbors.size() >= minPts) {
                    List<Point> cluster = new ArrayList<>();
                    expandClusterST(sortedByTime, point, neighbors, cluster, clusterIdCounter++, spatialThreshold, finalMetric);
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
     * 优化的 ST-DBSCAN 时空邻居查询：时间优先（Time-First）策略
     *
     * <p>策略对比：
     * <ul>
     *   <li>原策略（空间优先）：KDTree 返回 S 个空间邻居 → 时间过滤留 T 个（T ≤ S）
     *       代价 O(log n + S)，当 S >> T 时大量计算被浪费</li>
     *   <li>新策略（时间优先）：二分搜索找 T 个时间候选 → 验证空间距离
     *       代价 O(log n + T)，典型 GPS 场景（时间窗口紧、空间密集）性能显著更优</li>
     * </ul>
     *
     * @param sortedByTime   按 timestamp 升序排列的点数组（预处理索引）
     * @param centerPoint    查询中心点
     * @param spatialThreshold 空间距离阈值（已根据 metric.isSquared() 做平方处理）
     * @param metric         距离度量
     * @return 时间和空间均满足条件的邻居列表
     */
    private List<Point> rangeQueryST(Point[] sortedByTime, Point centerPoint,
                                     double spatialThreshold, DistanceMetric metric) {
        long tMin = centerPoint.getTimestamp() - timeEps;
        long tMax = centerPoint.getTimestamp() + timeEps;

        // 二分搜索：找出时间窗口 [tMin, tMax] 在排序数组中的范围
        int lo = lowerBound(sortedByTime, tMin);
        int hi = upperBound(sortedByTime, tMax);

        List<Point> stNeighbors = new ArrayList<>();
        for (int i = lo; i < hi; i++) {
            Point candidate = sortedByTime[i];
            double dist = metric.calculateDistanceOrSquared(
                    centerPoint.getClusteringCoordinates(),
                    candidate.getClusteringCoordinates());
            if (dist <= spatialThreshold) {
                stNeighbors.add(candidate);
            }
        }
        return stNeighbors;
    }

    /**
     * ST-DBSCAN 集群扩展
     *
     * <p>优化：使用 {@link ArrayDeque} 替换 ArrayList 模拟队列。
     * ArrayDeque.poll() 会移除队首元素，允许 GC 及时回收已处理节点，
     * 在大型集群（数万点）下可显著降低内存峰值。
     */
    private void expandClusterST(Point[] sortedByTime, Point point, List<Point> neighbors,
                                  List<Point> cluster, int clusterId,
                                  double spatialThreshold, DistanceMetric metric) {
        cluster.add(point);
        point.setClusterId(clusterId);

        java.util.BitSet inQueue = new java.util.BitSet();
        for (Point n : neighbors) {
            inQueue.set(n.getId());
        }

        // 优化：ArrayDeque 作为真正的 FIFO 队列，已处理元素可被 GC 回收
        Deque<Point> queue = new ArrayDeque<>(neighbors);
        while (!queue.isEmpty()) {
            Point neighbor = queue.poll();

            if (!neighbor.isVisited()) {
                neighbor.setVisited(true);
                List<Point> neighborNeighbors = rangeQueryST(sortedByTime, neighbor, spatialThreshold, metric);
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

    /**
     * 二分搜索 Lower Bound：找到第一个 timestamp >= target 的下标。
     * 若所有元素都 < target，则返回 arr.length。
     */
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

    /**
     * 二分搜索 Upper Bound：找到第一个 timestamp > target 的下标（排他性结束索引）。
     * 若所有元素都 <= target，则返回 arr.length。
     */
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
