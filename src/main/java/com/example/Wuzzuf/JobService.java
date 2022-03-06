package com.example.Wuzzuf;

import org.apache.spark.sql.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class JobService implements JobDAO{
    @Autowired
    private SparkSession sparkSession;

    @Autowired
    private Dataset<Job> wuzzufJobs;


    @Override
    public ResponseEntity<List<Job>> getSample() {
        return ResponseEntity.ok(wuzzufJobs.takeAsList(25));
    }
}
