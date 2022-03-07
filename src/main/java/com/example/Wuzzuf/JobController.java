package com.example.Wuzzuf;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping(path = "api/wuzzuf_jobs")
public class JobController {
    @Autowired
    private JobService jobService;

    @GetMapping(path = "sample")
    public String showSample(){
        return jobService.getSample();
    }

    @GetMapping(path = "schema")
    public String showSchema(){
        return jobService.getSchema();
    }

    @GetMapping(path = "clean")
    public String cleanData(){
        return jobService.cleanData();
    }
}
