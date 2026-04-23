package com.hgx.dbscan.algorithm;

import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

public class ClusteringTest {

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
