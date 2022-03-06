package com.example.Wuzzuf;

import org.apache.spark.sql.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

@Service
public class JobService implements JobDAO{
    @Autowired
    private SparkSession sparkSession;

    @Autowired
    private Dataset<Row> wuzzufJobs;


    @Override
    public ResponseEntity<String> getSample() {
        return ResponseEntity.ok(wuzzufJobs.showString(25, 0, true));
    }
}
