package com.example.Wuzzuf;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = "api/wuzzuf")
public class JobController {
    @Autowired
    private JobService jobService;

    @GetMapping
    public ResponseEntity<String> getSample(){
        return jobService.getSample();
    }
}
