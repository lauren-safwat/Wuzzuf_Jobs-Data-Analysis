package com.example.Wuzzuf;

import org.apache.spark.sql.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.w3c.dom.html.HTMLTableElement;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class JobService implements JobDAO{
    @Autowired
    private SparkSession sparkSession;

    @Autowired
    private Dataset<Job> wuzzufJobs;


    @Override
    public String getSample() {
        String response = String.format("<h1 style=\"text-align:center; padding-top: 8px;\">%s</h1>", "Sample From Wuzzuf Jobs Data") +
                          "<style>\n" +
                              "table {width: 100%; top: 0;}\n" +
                              "th {background: #99BBCB; position: sticky;}\n" +
                              "th,td {padding: 8px 15px;}\n" +
                              "table, th, td {border-collapse: collapse; border: 2px solid black;}\n" +
                          "</style>\n" +
                          "<table>\n" +
                              "<tr><th>Title</th> <th>Company</th> <th>Location</th> <th>Type</th> <th>Level</th> <th>YearsExp</th> <th>Country</th> <th>Skills</th></tr>" + wuzzufJobs.takeAsList(50).stream().map(Job::toString).collect(Collectors.joining("")) +
                          "</table>";
        return response;
    }

    @Override
    public String getSchema() {
        return wuzzufJobs.schema().prettyJson();
    }

    @Override
    public String cleanData() {
        StringBuilder response = new StringBuilder("<h1>Cleaning the data: </h1>");
        response.append("<h3>- Number of records before cleaning: ").append(wuzzufJobs.count()).append("</h3>");
        long nDuplicates = wuzzufJobs.count() - wuzzufJobs.distinct().count();
        long nNull = wuzzufJobs.count();
        wuzzufJobs = wuzzufJobs.na().drop().as(Encoders.bean(Job.class));
        nNull -= wuzzufJobs.count();
        wuzzufJobs = wuzzufJobs.dropDuplicates();
        response.append("<h3>- Number of duplicates: ").append(nDuplicates).append("</h3>");
        response.append("<h3>- Number of records containing null values: ").append(nNull).append("</h3>");
        response.append("<h3>- Number of records after cleaning: ").append(wuzzufJobs.count()).append("</h3>");
        return response.toString();
    }

    @Override
    public String jobsPerCompany() {
        Dataset<Row> jobsPerCompany = wuzzufJobs.groupBy("Company").agg(functions.count("Title").as("Number of jobs")).sort(functions.desc("Number of jobs"));
        return null;
    }
}
