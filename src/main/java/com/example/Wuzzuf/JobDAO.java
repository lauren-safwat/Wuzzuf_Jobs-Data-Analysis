package com.example.Wuzzuf;

import org.apache.spark.sql.Dataset;

public interface JobDAO {
    Dataset<Job> getSample();
}
