package com.example.Wuzzuf;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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

    @GetMapping(path = "jobs_per_company")
    public String jobsPerCompany(){
        return jobService.jobsPerCompany();
    }

    @GetMapping(path = "popular_job_titles")
    public String mostPopularJobTitles(){
        return jobService.mostPopularJobTitles();
    }

    @GetMapping(path = "popular_areas")
    public String mostPopularAreas(){
        return jobService.mostPopularAreas();
    }
}
