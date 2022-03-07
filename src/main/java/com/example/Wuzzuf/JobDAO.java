package com.example.Wuzzuf;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface JobDAO {
    String getSample();
    String getSchema();
    String cleanData();
    String jobsPerCompany();
    String mostPopularJobTitles();
    String mostPopularAreas();
    void jobsPerCompanyPieChart();
    void createBarChart(Dataset<Row> df, String title, String xLabel, String yLabel, String path);
    void mostPopularJobsBarChart();
    void mostPopularAreasBarChart();
    String mostRequiredSkills();
}
