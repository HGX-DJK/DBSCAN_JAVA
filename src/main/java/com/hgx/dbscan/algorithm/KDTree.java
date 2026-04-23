package com.hgx.dbscan.algorithm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

public class KDTree {

    private Node root;
    private final int k;
    private final DistanceMetric metric;

    public KDTree(int k) {
        this(k, new DistanceMetric.EuclideanMetric());
    }

    public KDTree(int k, DistanceMetric metric) {
        this.k = k;
        this.metric = metric;
    }

    public void build(List<DBSCAN.Point> points) {
        root = buildTree(new ArrayList<>(points), 0);
    }

    private Node buildTree(List<DBSCAN.Point> points, int depth) {
        if (points.isEmpty()) {
            return null;
        }

        int axis = depth % k;
        
        // 使用快速选择算法找到中位数，代替全排序，复杂度从 O(n log^2 n) 降为 O(n log n)
        int medianIdx = points.size() / 2;
        select(points, 0, points.size() - 1, medianIdx, axis);

        Node node = new Node(points.get(medianIdx), axis);

        node.left = buildTree(points.subList(0, medianIdx), depth + 1);
        node.right = buildTree(points.subList(medianIdx + 1, points.size()), depth + 1);

        return node;
    }

    /**
     * Quickselect 算法，用于在 O(n) 时间内找到第 n 小的元素
     */
    private void select(List<DBSCAN.Point> points, int left, int right, int n, int axis) {
        while (left < right) {
            int pivotIndex = partition(points, left, right, axis);
            if (pivotIndex == n) {
                return;
            } else if (pivotIndex < n) {
                left = pivotIndex + 1;
            } else {
                right = pivotIndex - 1;
            }
        }
    }

    private int partition(List<DBSCAN.Point> points, int left, int right, int axis) {
        DBSCAN.Point pivot = points.get(right);
        int i = left;
        for (int j = left; j < right; j++) {
            if (points.get(j).getClusteringCoordinates()[axis] < pivot.getClusteringCoordinates()[axis]) {
                Collections.swap(points, i, j);
                i++;
            }
        }
        Collections.swap(points, i, right);
        return i;
    }

    public List<DBSCAN.Point> rangeQuery(DBSCAN.Point queryPoint, double radius) {
        List<DBSCAN.Point> result = new ArrayList<>();
        double searchRadius = metric.isSquared() ? radius * radius : radius;
        double coordDelta = radius;
        
        // 如果是地理坐标系，需要将公里单位转换为经纬度近似值进行剪枝
        if (metric instanceof DistanceMetric.HaversineMetric) {
            // 纬度 1 度约 111.32 km
            // 经度 1 度约 111.32 * cos(lat) km
            // 这里取一个安全的保守估计值，使用 110.0 作为分母来确保搜索范围略大于实际需求
            coordDelta = radius / 110.0; 
        } else if (metric.isSquared()) {
            coordDelta = radius; // 对于平方欧氏距离，剪枝使用的是原始 radius
        }
        
        rangeQuery(root, queryPoint, searchRadius, coordDelta, result);
        return result;
    }

    private void rangeQuery(Node node, DBSCAN.Point queryPoint, double searchRadius, double coordDelta, List<DBSCAN.Point> result) {
        if (node == null) {
            return;
        }

        double distance = metric.calculateDistanceOrSquared(node.point.getClusteringCoordinates(), queryPoint.getClusteringCoordinates());
        if (distance <= searchRadius) {
            result.add(node.point);
        }

        int axis = node.axis;
        double queryCoord = queryPoint.getClusteringCoordinates()[axis];
        double nodeCoord = node.point.getClusteringCoordinates()[axis];

        // 剪枝逻辑：使用转换后的坐标差值 coordDelta
        if (queryCoord - coordDelta <= nodeCoord) {
            rangeQuery(node.left, queryPoint, searchRadius, coordDelta, result);
        }
        if (queryCoord + coordDelta >= nodeCoord) {
            rangeQuery(node.right, queryPoint, searchRadius, coordDelta, result);
        }
    }

    /**
     * k-最近邻搜索，HDBSCAN 核心距离计算需要此功能
     */
    public List<DBSCAN.Point> kNearestNeighbors(DBSCAN.Point queryPoint, int k) {
        PriorityQueue<Neighbor> pq = new PriorityQueue<>(Collections.reverseOrder());
        knn(root, queryPoint, k, pq);
        
        List<DBSCAN.Point> result = new ArrayList<>();
        while (!pq.isEmpty()) {
            result.add(0, pq.poll().point);
        }
        return result;
    }

    private void knn(Node node, DBSCAN.Point queryPoint, int k, PriorityQueue<Neighbor> pq) {
        if (node == null) {
            return;
        }

        double dist = metric.calculateDistanceOrSquared(node.point.getClusteringCoordinates(), queryPoint.getClusteringCoordinates());
        if (pq.size() < k) {
            pq.add(new Neighbor(node.point, dist));
        } else if (dist < pq.peek().distance) {
            pq.poll();
            pq.add(new Neighbor(node.point, dist));
        }

        int axis = node.axis;
        double queryCoord = queryPoint.getClusteringCoordinates()[axis];
        double nodeCoord = node.point.getClusteringCoordinates()[axis];

        Node first = queryCoord < nodeCoord ? node.left : node.right;
        Node second = queryCoord < nodeCoord ? node.right : node.left;

        knn(first, queryPoint, k, pq);

        double planeDist = Math.abs(queryCoord - nodeCoord);
        double checkDist = metric.isSquared() ? planeDist * planeDist : planeDist;

        if (pq.size() < k || checkDist < pq.peek().distance) {
            knn(second, queryPoint, k, pq);
        }
    }

    private static class Node {
        private final DBSCAN.Point point;
        private final int axis;
        private Node left;
        private Node right;

        public Node(DBSCAN.Point point, int axis) {
            this.point = point;
            this.axis = axis;
        }
    }

    private static class Neighbor implements Comparable<Neighbor> {
        DBSCAN.Point point;
        double distance;

        Neighbor(DBSCAN.Point point, double distance) {
            this.point = point;
            this.distance = distance;
        }

        @Override
        public int compareTo(Neighbor o) {
            return Double.compare(this.distance, o.distance);
        }
    }
}