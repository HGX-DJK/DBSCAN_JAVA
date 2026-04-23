package com.hgx.dbscan.algorithm;

public interface DistanceMetric {
    double calculate(double[] c1, double[] c2);
    
    /**
     * 对于欧几里得距离，返回平方值可以避免 Math.sqrt()，提高性能。
     * 对于非欧几里得距离（如 Haversine），则返回原始距离。
     */
    double calculateDistanceOrSquared(double[] c1, double[] c2);
    
    boolean isSquared();

    class EuclideanMetric implements DistanceMetric {
        @Override
        public double calculate(double[] c1, double[] c2) {
            return Math.sqrt(calculateDistanceOrSquared(c1, c2));
        }

        @Override
        public double calculateDistanceOrSquared(double[] c1, double[] c2) {
            double sum = 0;
            int n = Math.min(c1.length, c2.length);
            for (int i = 0; i < n; i++) {
                double diff = c1[i] - c2[i];
                sum += diff * diff;
            }
            return sum;
        }

        @Override
        public boolean isSquared() {
            return true;
        }
    }

    class HaversineMetric implements DistanceMetric {
        private static final double R = 6371.0; // 地球半径 (km)

        @Override
        public double calculate(double[] c1, double[] c2) {
            double lon1 = c1[0], lat1 = c1[1];
            double lon2 = c2[0], lat2 = c2[1];
            
            double lat1Rad = Math.toRadians(lat1);
            double lat2Rad = Math.toRadians(lat2);
            double deltaLat = Math.toRadians(lat2 - lat1);
            double deltaLon = Math.toRadians(lon2 - lon1);
            
            double a = Math.sin(deltaLat / 2) * Math.sin(deltaLat / 2) +
                       Math.cos(lat1Rad) * Math.cos(lat2Rad) *
                       Math.sin(deltaLon / 2) * Math.sin(deltaLon / 2);
            double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
            
            return R * c;
        }

        @Override
        public double calculateDistanceOrSquared(double[] c1, double[] c2) {
            return calculate(c1, c2);
        }

        @Override
        public boolean isSquared() {
            return false;
        }
    }
}
