package com.example.Wuzzuf;

import org.springframework.http.ResponseEntity;

import java.util.List;

public interface JobDAO {
    String getSample();
    String getSchema();
    String cleanData();
    String jobsPerCompany();
}
