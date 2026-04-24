package com.hgx.dbscan.algorithm;

import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

public class ClusteringTest {

    /**
     * ST-DBSCAN 测试：验证时间窗口过滤的正确性
     *
     * <p>场景说明：
     * <ul>
     *   <li>Cluster A：3 个点，空间相邻，时间戳相近（在 timeEps 内）→ 应聚合</li>
     *   <li>Cluster B 候选：2 个点，空间与 Cluster A 相邻，但时间戳超出 timeEps → 不应与 A 合并，
     *       且自身点数不足 minPts，最终成为噪声</li>
     *   <li>孤立噪声点：时间和空间均孤立 → 噪声</li>
     * </ul>
     */
    @Test
    public void testST_DBSCAN_TimeWindowFilter() {
        long baseTime = 1_000_000L;
        long timeEps  = 60_000L; // 60 秒

        List<DBSCAN.Point> points = new ArrayList<>();
        // Cluster A：空间相邻 + 时间在窗口内
        points.add(new DBSCAN.Point(new double[]{1000.0, 1000.0}, baseTime));
        points.add(new DBSCAN.Point(new double[]{1000.1, 1000.1}, baseTime + 10_000L));
        points.add(new DBSCAN.Point(new double[]{1000.2, 1000.0}, baseTime + 20_000L));

        // 时间超出窗口的点（空间上与 Cluster A 相邻，但时间差 > timeEps）
        // 这些点不应被纳入 Cluster A，也因数量不足 minPts(3) 而成为噪声
        points.add(new DBSCAN.Point(new double[]{1000.05, 1000.05}, baseTime + 120_000L)); // +2 分钟
        points.add(new DBSCAN.Point(new double[]{1000.15, 1000.15}, baseTime + 130_000L)); // +2.1 分钟

        // 孤立噪声点
        points.add(new DBSCAN.Point(new double[]{9999.0, 9999.0}, baseTime));

        STDBSCAN stdbscan = new STDBSCAN(1.5, 3, 3, timeEps);
        List<List<DBSCAN.Point>> clusters = stdbscan.cluster(points);

        // 只有 Cluster A（3 个点）应该成为有效集群
        assertEquals(1, clusters.size(), "只应存在 1 个有效集群（Cluster A）");
        assertEquals(3, clusters.get(0).size(), "Cluster A 应包含 3 个点");
    }

    /**
     * ST-DBSCAN 测试：验证时间和空间都满足时，点可正确聚合
     */
    @Test
    public void testST_DBSCAN_CorrectClustering() {
        long baseTime = 0L;
        long timeEps  = 3_600_000L; // 1 小时

        List<DBSCAN.Point> points = new ArrayList<>();
        // Cluster 1
        points.add(new DBSCAN.Point(new double[]{1000.0, 1000.0}, baseTime));
        points.add(new DBSCAN.Point(new double[]{1000.1, 1000.0}, baseTime + 600_000L));
        points.add(new DBSCAN.Point(new double[]{1000.0, 1000.1}, baseTime + 1_200_000L));

        // Cluster 2（空间远离 Cluster 1）
        points.add(new DBSCAN.Point(new double[]{5000.0, 5000.0}, baseTime));
        points.add(new DBSCAN.Point(new double[]{5000.1, 5000.0}, baseTime + 300_000L));
        points.add(new DBSCAN.Point(new double[]{5000.0, 5000.1}, baseTime + 900_000L));

        STDBSCAN stdbscan = new STDBSCAN(1.5, 3, 3, timeEps);
        List<List<DBSCAN.Point>> clusters = stdbscan.cluster(points);

        assertEquals(2, clusters.size(), "应发现 2 个集群");
        assertTrue(clusters.stream().allMatch(c -> c.size() == 3), "每个集群应包含 3 个点");
    }

    /**
     * ST-DBSCAN 测试：全为噪声的情况
     */
    @Test
    public void testST_DBSCAN_AllNoise() {
        long timeEps = 1000L;
        List<DBSCAN.Point> points = new ArrayList<>();
        // 每个点时间相差很大，空间相邻也无法形成集群
        points.add(new DBSCAN.Point(new double[]{0.0, 0.0}, 0L));
        points.add(new DBSCAN.Point(new double[]{0.1, 0.0}, 10_000L));
        points.add(new DBSCAN.Point(new double[]{0.0, 0.1}, 20_000L));

        STDBSCAN stdbscan = new STDBSCAN(1.5, 3, 3, timeEps);
        List<List<DBSCAN.Point>> clusters = stdbscan.cluster(points);

        assertEquals(0, clusters.size(), "时间间距超出 timeEps，应无集群");
    }

    @Test
    public void testDBSCAN() {
        List<DBSCAN.Point> points = new ArrayList<>();
        // 使用超出经纬度范围的坐标，确保使用欧氏距离进行测试
        // Cluster 1
        points.add(new DBSCAN.Point(new double[]{1000.0, 1000.0}));
        points.add(new DBSCAN.Point(new double[]{1000.1, 1000.1}));
        points.add(new DBSCAN.Point(new double[]{1000.2, 1000.2}));
        
        // Cluster 2
        points.add(new DBSCAN.Point(new double[]{2000.0, 2000.0}));
        points.add(new DBSCAN.Point(new double[]{2000.1, 2000.1}));
        
        // Noise
        points.add(new DBSCAN.Point(new double[]{5000.0, 5000.0}));

        DBSCAN dbscan = new DBSCAN(1.5, 2, 2);
        List<List<DBSCAN.Point>> clusters = dbscan.cluster(points);

        assertEquals(2, clusters.size());
        assertTrue(clusters.stream().anyMatch(c -> c.size() == 3));
        assertTrue(clusters.stream().anyMatch(c -> c.size() == 2));
    }

    @Test
    public void testHDBSCAN() {
        List<DBSCAN.Point> points = new ArrayList<>();
        // 密集集群
        points.add(new DBSCAN.Point(new double[]{1.0, 1.0}));
        points.add(new DBSCAN.Point(new double[]{1.1, 1.1}));
        points.add(new DBSCAN.Point(new double[]{1.2, 1.2}));
        points.add(new DBSCAN.Point(new double[]{1.3, 1.3}));
        
        // 稀疏集群
        points.add(new DBSCAN.Point(new double[]{10.0, 10.0}));
        points.add(new DBSCAN.Point(new double[]{12.0, 12.0}));
        points.add(new DBSCAN.Point(new double[]{14.0, 14.0}));
        
        HDBSCAN hdbscan = new HDBSCAN(0, 2, 2);
        List<List<DBSCAN.Point>> clusters = hdbscan.cluster(points);

        // HDBSCAN 应该能发现这两个不同密度的集群
        assertTrue(clusters.size() >= 1);
        System.out.println("Found " + clusters.size() + " clusters with HDBSCAN");
    }
}
