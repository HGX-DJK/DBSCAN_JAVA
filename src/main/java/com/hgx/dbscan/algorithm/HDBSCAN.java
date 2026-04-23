package com.hgx.dbscan.algorithm;

import java.util.*;

public class HDBSCAN {

    private int minClusterSize;
    private int minPts;
    private DistanceMetric explicitMetric;
    private DistanceMetric metric;

    public HDBSCAN(double eps, int minPts, int minClusterSize) {
        this(eps, minPts, minClusterSize, null);
    }

    public HDBSCAN(double eps, int minPts, int minClusterSize, DistanceMetric metric) {
        this.minPts = minPts;
        this.minClusterSize = minClusterSize;
        this.explicitMetric = metric;
        // 注意：HDBSCAN 通常不需要 epsilon，但为了保持接口一致性，我们可以保留它或作为软上限
    }

    public List<List<DBSCAN.Point>> cluster(List<DBSCAN.Point> points) {
        if (points == null || points.isEmpty() || points.size() < minPts) {
            return new ArrayList<>();
        }

        // 1. 初始化点 ID 和度量标准（如果是地理坐标则执行城市级投影）
        for (int i = 0; i < points.size(); i++) {
            points.get(i).setId(i);
        }
        
        this.metric = explicitMetric;
        if (this.metric == null) {
            if (points.size() > 0 && isGeoCoordinates(points.get(0).getCoordinates())) {
                projectPointsToKm(points);
                this.metric = new DistanceMetric.EuclideanMetric();
            } else {
                this.metric = new DistanceMetric.EuclideanMetric();
            }
        }

        // 2. 构建 KD-Tree 并计算核心距离 (Core Distances)
        KDTree kdTree = new KDTree(points.get(0).getClusteringCoordinates().length, metric);
        kdTree.build(points);
        
        double[] coreDistances = new double[points.size()];
        for (int i = 0; i < points.size(); i++) {
            // k-NN 搜索，k = minPts
            // 注意：KDTree 返回的列表可能包含查询点本身
            List<DBSCAN.Point> neighbors = kdTree.kNearestNeighbors(points.get(i), minPts);
            if (neighbors.isEmpty()) {
                coreDistances[i] = 0;
            } else {
                // 取第 k 个邻居的距离作为核心距离
                DBSCAN.Point kthNeighbor = neighbors.get(neighbors.size() - 1);
                coreDistances[i] = metric.calculate(points.get(i).getClusteringCoordinates(), kthNeighbor.getClusteringCoordinates());
            }
        }

        // 3. 构建最小生成树 (MST) - 使用 Prim 算法
        // 在 HDBSCAN 中，边权是互达距离: max(coreDist(a), coreDist(b), dist(a,b))
        MSTEdge[] mst = constructMST(points, coreDistances);

        // 4. 构建聚类层次结构 (Dendrogram)
        Arrays.sort(mst);
        List<HierarchyNode> hierarchy = buildHierarchy(points.size(), mst);

        // 5. 提取稳定集群
        return extractClusters(points, hierarchy);
    }

    private MSTEdge[] constructMST(List<DBSCAN.Point> points, double[] coreDistances) {
        int n = points.size();
        MSTEdge[] mst = new MSTEdge[n - 1];
        double[] minDistances = new double[n];
        Arrays.fill(minDistances, Double.MAX_VALUE);
        int[] nearestNode = new int[n];
        boolean[] inMST = new boolean[n];

        minDistances[0] = 0;
        for (int i = 0; i < n; i++) {
            int u = -1;
            for (int v = 0; v < n; v++) {
                if (!inMST[v] && (u == -1 || minDistances[v] < minDistances[u])) {
                    u = v;
                }
            }

            inMST[u] = true;
            if (i > 0) {
                mst[i - 1] = new MSTEdge(nearestNode[u], u, minDistances[u]);
            }

            for (int v = 0; v < n; v++) {
                if (!inMST[v]) {
                    double dist = metric.calculate(points.get(u).getClusteringCoordinates(), points.get(v).getClusteringCoordinates());
                    double mreach = Math.max(Math.max(coreDistances[u], coreDistances[v]), dist);
                    if (mreach < minDistances[v]) {
                        minDistances[v] = mreach;
                        nearestNode[v] = u;
                    }
                }
            }
        }
        return mst;
    }

    private List<HierarchyNode> buildHierarchy(int n, MSTEdge[] mst) {
        // 关键修复：Prim 算法生成的边不是按权重排序的。
        // 构建层次树（单连接层次聚类）必须严格按照边权重升序排列！
        Arrays.sort(mst, (a, b) -> Double.compare(a.weight, b.weight));

        UnionFind uf = new UnionFind(n);
        HierarchyNode[] currentNodes = new HierarchyNode[n];
        for (int i = 0; i < n; i++) {
            currentNodes[i] = new HierarchyNode(i);
        }

        List<HierarchyNode> allParentNodes = new ArrayList<>();
        for (MSTEdge edge : mst) {
            int root1 = uf.find(edge.u);
            int root2 = uf.find(edge.v);
            
            if (root1 != root2) {
                HierarchyNode parent = new HierarchyNode(-1);
                parent.left = currentNodes[root1];
                parent.right = currentNodes[root2];
                // parent.lambda 是这个簇“分裂”成 left 和 right 的密度（死亡密度）
                // 修复：如果距离为0，意味着完全重合的点，其分裂密度应该趋于无穷大
                parent.lambda = edge.weight > 1e-12 ? 1.0 / edge.weight : Double.MAX_VALUE;
                
                // 设置子簇的“出生”密度为父簇的“分裂”密度
                parent.left.parentLambda = parent.lambda;
                parent.right.parentLambda = parent.lambda;
                
                parent.size = parent.left.size + parent.right.size;
                
                int newRoot = uf.union(root1, root2);
                currentNodes[newRoot] = parent;
                allParentNodes.add(parent);
            }
        }
        
        // 最后一次合并生成的 parent 节点就是整棵树的根节点
        return allParentNodes;
    }

    private List<List<DBSCAN.Point>> extractClusters(List<DBSCAN.Point> points, List<HierarchyNode> hierarchy) {
        HierarchyNode root = hierarchy.get(hierarchy.size() - 1);
        computeStability(root);
        finalizeNode(root); // 最终确认根节点是否应该被拆分
        
        List<HierarchyNode> selectedNodes = new ArrayList<>();
        findBestClusters(root, selectedNodes);
        
        List<List<DBSCAN.Point>> result = new ArrayList<>();
        // 先重置所有点的 clusterId 为 -1
        for (DBSCAN.Point p : points) p.setClusterId(-1);
        
        for (int i = 0; i < selectedNodes.size(); i++) {
            HierarchyNode node = selectedNodes.get(i);
            List<DBSCAN.Point> cluster = new ArrayList<>();
            collectPoints(node, points, cluster, i);
            result.add(cluster);
        }
        return result;
    }

    private void computeStability(HierarchyNode node) {
        if (node.isLeaf()) {
            node.clusterStability = 0;
            node.bestSubtreeStability = 0;
            node.isStable = false;
            return;
        }

        computeStability(node.left);
        computeStability(node.right);

        double lambdaBirth = node.parentLambda;
        double lambdaDeath = node.lambda;
        if (lambdaDeath < lambdaBirth) lambdaDeath = lambdaBirth; // 防御性保护

        // 当前层级跨度下的“质量过剩”积分
        double survivalContribution = (lambdaDeath - lambdaBirth) * node.size;

        if (node.left.size >= minClusterSize && node.right.size >= minClusterSize) {
            // 1. 显著分裂：此时子节点是它们各自压缩分支的顶点，必须在这里做终极 PK
            finalizeNode(node.left);
            finalizeNode(node.right);
            
            // 当前节点作为一个新压缩分支的底端，重新开始积累
            node.clusterStability = survivalContribution;
            node.bestSubtreeStability = node.left.bestSubtreeStability + node.right.bestSubtreeStability;
            node.isStable = false; 
        } else {
            // 2. 非显著分裂：当前分支在继续延伸
            HierarchyNode significantChild = node.left.size >= minClusterSize ? node.left : (node.right.size >= minClusterSize ? node.right : null);
            
            if (significantChild != null) {
                // 累加当前分支的稳定性
                node.clusterStability = survivalContribution + significantChild.clusterStability;
                node.bestSubtreeStability = significantChild.bestSubtreeStability;
            } else {
                // 如果这是簇的尽头（全部散成噪声）
                node.clusterStability = survivalContribution;
                node.bestSubtreeStability = 0;
            }
            node.isStable = false;
        }
    }

    private void finalizeNode(HierarchyNode node) {
        // HDBSCAN 终极定理：如果保持为一个簇的稳定性 > 拆分成子簇的稳定性总和，则不拆分
        if (node.clusterStability > node.bestSubtreeStability) {
            node.isStable = true;
            node.bestSubtreeStability = node.clusterStability; // 向上汇报时，代表整个子树的最优解就是自己
        } else {
            node.isStable = false;
        }
    }

    private void findBestClusters(HierarchyNode node, List<HierarchyNode> selected) {
        if (node == null) return;
        if (node.isStable) {
            selected.add(node);
        } else {
            findBestClusters(node.left, selected);
            findBestClusters(node.right, selected);
        }
    }

    private void collectPoints(HierarchyNode node, List<DBSCAN.Point> allPoints, List<DBSCAN.Point> result, int clusterId) {
        if (node.isLeaf()) {
            DBSCAN.Point p = allPoints.get(node.pointId);
            p.setClusterId(clusterId);
            result.add(p);
            return;
        }
        collectPoints(node.left, allPoints, result, clusterId);
        collectPoints(node.right, allPoints, result, clusterId);
    }

    /**
     * 城市级投影优化：将经纬度投影到以公里为单位的平面
     */
    private void projectPointsToKm(List<DBSCAN.Point> points) {
        if (points.isEmpty()) return;

        // 借鉴 Rust 版：使用全量数据的平均纬度作为投影基准，减少畸变
        double sumLat = 0;
        for (DBSCAN.Point p : points) {
            sumLat += p.getCoordinates()[1];
        }
        double avgLat = sumLat / points.size();

        double latKmPerDegree = 111.32;
        double lngKmPerDegree = 111.32 * Math.cos(Math.toRadians(avgLat));

        for (DBSCAN.Point p : points) {
            double[] coords = p.getCoordinates();
            double[] projected = new double[2];
            projected[0] = coords[0] * lngKmPerDegree;
            projected[1] = coords[1] * latKmPerDegree;
            p.setProjectedCoordinates(projected);
        }
    }

    private boolean isGeoCoordinates(double[] coords) {
        return coords.length == 2 && Math.abs(coords[0]) <= 180 && Math.abs(coords[1]) <= 90;
    }

    private DistanceMetric determineMetric(List<DBSCAN.Point> points) {
        double[] coords = points.get(0).getCoordinates();
        if (isGeoCoordinates(coords)) {
            return new DistanceMetric.HaversineMetric();
        }
        return new DistanceMetric.EuclideanMetric();
    }

    private static class MSTEdge implements Comparable<MSTEdge> {
        int u, v;
        double weight;

        MSTEdge(int u, int v, double weight) {
            this.u = u;
            this.v = v;
            this.weight = weight;
        }

        @Override
        public int compareTo(MSTEdge o) {
            return Double.compare(this.weight, o.weight);
        }
    }

    private static class HierarchyNode {
        int pointId; // 仅叶子节点有效
        int size;
        double lambda; // 分裂密度
        double parentLambda; // 出生密度
        double clusterStability; // 当前压缩分支作为独立簇的稳定性
        double bestSubtreeStability; // 子树的最优拆分稳定性之和
        boolean isStable;
        HierarchyNode left, right;

        HierarchyNode(int pointId) {
            this.pointId = pointId;
            this.size = 1;
            this.lambda = 0;
            this.parentLambda = 0;
        }

        boolean isLeaf() {
            return left == null && right == null;
        }
    }

    private static class UnionFind {
        int[] parent;
        UnionFind(int n) {
            parent = new int[n];
            for (int i = 0; i < n; i++) parent[i] = i;
        }
        int find(int i) {
            if (parent[i] == i) return i;
            return parent[i] = find(parent[i]);
        }
        int union(int i, int j) {
            int rootI = find(i);
            int rootJ = find(j);
            if (rootI != rootJ) {
                parent[rootI] = rootJ;
                return rootJ;
            }
            return rootI;
        }
    }
}