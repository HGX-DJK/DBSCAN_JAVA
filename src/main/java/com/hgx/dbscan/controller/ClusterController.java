package com.hgx.dbscan.controller;

import com.hgx.dbscan.model.ClusterResponse;
import com.hgx.dbscan.service.ClusterService;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class ClusterController {

    private static final Logger logger = LoggerFactory.getLogger(ClusterController.class);

    @Autowired
    private ClusterService clusterService;

    @PostMapping("/cluster")
    public ResponseEntity<?> cluster(
            @RequestParam("file") MultipartFile file,
            @RequestParam(value = "eps", defaultValue = "0.0") double eps, // Default to 0.0, as it might not be used by all algorithms
            @RequestParam("minPts") int minPts,
            @RequestParam("minClusterSize") int minClusterSize,
            @RequestParam(value = "lngField", defaultValue = "lng") String lngField,
            @RequestParam(value = "latField", defaultValue = "lat") String latField,
            @RequestParam(value = "timeField", defaultValue = "timestamp") String timeField,
            @RequestParam(value = "timeEps", defaultValue = "0") long timeEps,
            @RequestParam(value = "algorithm", defaultValue = "dbscan") String algorithm) {
        try {
            // 验证文件
            if (file.isEmpty()) {
                Map<String, String> error = new HashMap<>();
                error.put("error", "File is empty");
                return new ResponseEntity<>(error, HttpStatus.BAD_REQUEST);
            }

            // 验证参数
            if (eps <= 0) {
                Map<String, String> error = new HashMap<>();
                error.put("error", "eps must be positive");
                return new ResponseEntity<>(error, HttpStatus.BAD_REQUEST);
            }

            if (minPts < 1) {
                Map<String, String> error = new HashMap<>();
                error.put("error", "minPts must be at least 1");
                return new ResponseEntity<>(error, HttpStatus.BAD_REQUEST);
            }

            if (minClusterSize < 1) {
                Map<String, String> error = new HashMap<>();
                error.put("error", "minClusterSize must be at least 1");
                return new ResponseEntity<>(error, HttpStatus.BAD_REQUEST);
            }

            // 执行聚类
            ClusterResponse response = clusterService.cluster(file.getInputStream(), eps, minPts, minClusterSize, lngField, latField, timeField, timeEps, algorithm);
            return new ResponseEntity<>(response, HttpStatus.OK);
        } catch (IllegalArgumentException e) {
            logger.warn("Invalid parameters: {}", e.getMessage());
            Map<String, String> error = new HashMap<>();
            error.put("error", e.getMessage());
            return new ResponseEntity<>(error, HttpStatus.BAD_REQUEST);
        } catch (Exception e) {
            logger.error("Error processing request: {}", e.getMessage(), e);
            Map<String, String> error = new HashMap<>();
            error.put("error", "Internal server error");
            return new ResponseEntity<>(error, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping("/cluster/download")
    public ResponseEntity<byte[]> downloadClusteredData(
            @RequestParam("downloadId") String downloadId) {
        try {
            List<Map<String, String>> clusteredData = clusterService.getClusteredData(downloadId);

            if (clusteredData == null || clusteredData.isEmpty()) {
                logger.warn("Download request: No clustered data found for downloadId: {}", downloadId);
                return new ResponseEntity<>(HttpStatus.NOT_FOUND);
            }

            // Generate CSV
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            CSVPrinter csvPrinter = null;
            try {
                // Get headers from the first map (assuming all maps have the same keys)
                List<String> headers = new ArrayList<>(clusteredData.get(0).keySet());
                // Ensure "clusterId" is the last header
                if (headers.contains("clusterId")) {
                    headers.remove("clusterId");
                    headers.add("clusterId");
                }

                csvPrinter = new CSVPrinter(new java.io.OutputStreamWriter(bos), CSVFormat.DEFAULT.withHeader(headers.toArray(new String[0])));

                for (Map<String, String> row : clusteredData) {
                    List<String> rowValues = new ArrayList<>();
                    for (String header : headers) {
                        rowValues.add(row.getOrDefault(header, ""));
                    }
                    csvPrinter.printRecord(rowValues);
                }
                csvPrinter.flush();
            } finally {
                if (csvPrinter != null) {
                    csvPrinter.close();
                }
            }

            byte[] csvBytes = bos.toByteArray();

            HttpHeaders httpHeaders = new HttpHeaders();
            httpHeaders.setContentType(MediaType.parseMediaType("text/csv"));
            httpHeaders.setContentDispositionFormData("attachment", "clustered_data.csv");

            return new ResponseEntity<>(csvBytes, httpHeaders, HttpStatus.OK);

        } catch (Exception e) {
            logger.error("Error during clustered data download for downloadId {}: {}", downloadId, e.getMessage(), e);
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
