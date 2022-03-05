package com.example.Wuzzuf;

import org.apache.spark.sql.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class JobService implements JobDAO{
    @Autowired
    private SparkSession sparkSession;

    @Autowired
    private Dataset<Job> wuzzufJobs;


    @Override
    public Dataset<Job> getSample() {
        return wuzzufJobs;
    }
}
