package com.hgx.dbscan.model;

import java.util.List;

public class ClusterResponse {
    private int totalClusters;
    private int totalPoints;
    private int noisePoints;
    private List<Cluster> clusters;
    private List<Point> noisePointsList;

    public int getTotalClusters() {
        return totalClusters;
    }

    public void setTotalClusters(int totalClusters) {
        this.totalClusters = totalClusters;
    }

    public int getTotalPoints() {
        return totalPoints;
    }

    public void setTotalPoints(int totalPoints) {
        this.totalPoints = totalPoints;
    }

    public int getNoisePoints() {
        return noisePoints;
    }

    public void setNoisePoints(int noisePoints) {
        this.noisePoints = noisePoints;
    }

    public List<Cluster> getClusters() {
        return clusters;
    }

    public void setClusters(List<Cluster> clusters) {
        this.clusters = clusters;
    }

    public List<Point> getNoisePointsList() {
        return noisePointsList;
    }

    public void setNoisePointsList(List<Point> noisePointsList) {
        this.noisePointsList = noisePointsList;
    }

    public static class Cluster {
        private int clusterId;
        private List<Point> points;

        public int getClusterId() {
            return clusterId;
        }

        public void setClusterId(int clusterId) {
            this.clusterId = clusterId;
        }

        public List<Point> getPoints() {
            return points;
        }

        public void setPoints(List<Point> points) {
            this.points = points;
        }
    }

    public static class Point {
        private double[] coordinates;
        private java.util.Map<String, String> extraData;

        public Point(double[] coordinates) {
            this(coordinates, null);
        }

        public Point(double[] coordinates, java.util.Map<String, String> extraData) {
            this.coordinates = coordinates;
            this.extraData = extraData;
        }

        public java.util.Map<String, String> getExtraData() {
            return extraData;
        }

        public void setExtraData(java.util.Map<String, String> extraData) {
            this.extraData = extraData;
        }

        public double[] getCoordinates() {
            return coordinates;
        }

        public void setCoordinates(double[] coordinates) {
            this.coordinates = coordinates;
        }

        public double getX() {
            return coordinates.length > 0 ? coordinates[0] : 0;
        }

        public double getY() {
            return coordinates.length > 1 ? coordinates[1] : 0;
        }
    }
}
