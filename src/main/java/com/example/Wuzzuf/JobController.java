package com.example.Wuzzuf;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StreamUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

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

    @GetMapping(path = "most_popular_job_titles")
    public String mostPopularJobTitles(){
        return jobService.mostPopularJobTitles();
    }

    @GetMapping(path = "most_popular_areas")
    public String mostPopularAreas(){
        return jobService.mostPopularAreas();
    }

    @GetMapping(path = "jobs_per_company_chart")
    public ResponseEntity<byte[]> jobsPerCompanyPieChart()throws IOException {
        //jobService.jobsPerCompanyPieChart();
        ClassPathResource imageFile = new ClassPathResource("/pieChart.png");
        byte[] imageBytes = StreamUtils.copyToByteArray(imageFile.getInputStream());
        return ResponseEntity.ok().contentType(MediaType.IMAGE_PNG).body(imageBytes);
    }

    @GetMapping(path = "most_popular_job_titles_chart")
    public ResponseEntity<byte[]> mostPopularJobsBarChart()throws IOException {
        //jobService.mostPopularJobsBarChart();
        ClassPathResource imageFile = new ClassPathResource("/jobsBarChart.png");
        byte[] imageBytes = StreamUtils.copyToByteArray(imageFile.getInputStream());
        return ResponseEntity.ok().contentType(MediaType.IMAGE_PNG).body(imageBytes);
    }

    @GetMapping(path = "most_popular_areas_chart")
    public ResponseEntity<byte[]> mostPopularAreasBarChart()throws IOException {
        //jobService.mostPopularAreasBarChart();
        ClassPathResource imageFile = new ClassPathResource("/areasBarChart.png");
        byte[] imageBytes = StreamUtils.copyToByteArray(imageFile.getInputStream());
        return ResponseEntity.ok().contentType(MediaType.IMAGE_PNG).body(imageBytes);
    }

    @GetMapping(path = "most_required_skills")
    public String mostRequiredSkills(){
        return jobService.mostRequiredSkills();
    }
}
