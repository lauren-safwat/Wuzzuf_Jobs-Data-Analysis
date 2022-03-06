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

    @GetMapping(path = "show")
    public ResponseEntity<List<Job>> getSample(){
        return jobService.getSample();
    }
}
