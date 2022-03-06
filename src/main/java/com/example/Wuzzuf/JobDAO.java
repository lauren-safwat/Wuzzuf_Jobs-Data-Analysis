package com.example.Wuzzuf;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.http.ResponseEntity;

public interface JobDAO {
    ResponseEntity<String> getSample();
}
