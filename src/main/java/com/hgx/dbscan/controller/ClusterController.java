package com.hgx.dbscan.controller;

import com.hgx.dbscan.model.ClusterResponse;
import com.hgx.dbscan.service.ClusterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.util.HashMap;
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
            @RequestParam("eps") double eps,
            @RequestParam("minPts") int minPts,
            @RequestParam("minClusterSize") int minClusterSize,
            @RequestParam(value = "lngField", defaultValue = "lng") String lngField,
            @RequestParam(value = "latField", defaultValue = "lat") String latField,
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
            ClusterResponse response = clusterService.cluster(file.getInputStream(), eps, minPts, minClusterSize, lngField, latField, algorithm);
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
}
