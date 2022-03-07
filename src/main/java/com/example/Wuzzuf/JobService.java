package com.example.Wuzzuf;

import org.apache.spark.sql.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

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
        Dataset<Row> jobsPerCompany = wuzzufJobs.groupBy("Company").agg(functions.count("Title").as("Number of Jobs")).sort(functions.desc("Number of Jobs"));
        List<String> data = jobsPerCompany.map(row -> row.mkString("<tr><td>", "</td><td>", "</td></tr>"), Encoders.STRING()).limit(15).collectAsList();

        String response = String.format("<h1 style=\"text-align:center; padding-top: 8px;\">%s</h1>", "Most demanding companies for jobs") +
                "<style>\n" +
                    "table {width: 70%; top: 0; margin-left: auto; margin-right: auto;}\n" +
                    "th {background: #99BBCB; position: sticky;}\n" +
                    "th,td {padding: 8px 15px;}\n" +
                    "table, th, td {border-collapse: collapse; border: 2px solid black;}\n" +
                "</style>\n" +
                "<table>\n" +
                    "<tr> <th>Company</th> <th>Number of Jobs</th> </tr>" +
                    String.join("", data) +
                "</table>";

        return response;
    }

    @Override
    public String mostPopularJobTitles() {
        Dataset<Row> jobsPerCompany = wuzzufJobs.groupBy("Title").agg(functions.count("Title").as("Count")).sort(functions.desc("Count"));
        List<String> data = jobsPerCompany.map(row -> row.mkString("<tr><td>", "</td><td>", "</td></tr>"), Encoders.STRING()).limit(15).collectAsList();

        String response = String.format("<h1 style=\"text-align:center; padding-top: 8px;\">%s</h1>", "Most Popular Job Titles") +
                "<style>\n" +
                    "table {width: 70%; top: 0; margin-left: auto; margin-right: auto;}\n" +
                    "th {background: #99BBCB; position: sticky;}\n" +
                    "th,td {padding: 8px 15px;}\n" +
                    "table, th, td {border-collapse: collapse; border: 2px solid black;}\n" +
                "</style>\n" +
                "<table>\n" +
                    "<tr> <th>Job Title</th> <th>Number of Vacancies</th> </tr>" +
                    String.join("", data) +
                "</table>";

        return response;
    }

    @Override
    public String mostPopularAreas() {
        Dataset<Row> jobsPerCompany = wuzzufJobs.groupBy("Location").agg(functions.count("Title").as("Count")).sort(functions.desc("Count"));
        List<String> data = jobsPerCompany.map(row -> row.mkString("<tr><td>", "</td><td>", "</td></tr>"), Encoders.STRING()).limit(15).collectAsList();

        String response = String.format("<h1 style=\"text-align:center; padding-top: 8px;\">%s</h1>", "Most Popular Areas") +
                "<style>\n" +
                    "table {width: 70%; top: 0; margin-left: auto; margin-right: auto;}\n" +
                    "th {background: #99BBCB; position: sticky;}\n" +
                    "th,td {padding: 8px 15px;}\n" +
                    "table, th, td {border-collapse: collapse; border: 2px solid black;}\n" +
                "</style>\n" +
                "<table>\n" +
                    "<tr> <th>Area</th> <th>Number of Vacancies</th> </tr>" +
                    String.join("", data) +
                "</table>";

        return response;
    }

    @Override
    public String displayPieChart() {
//        PieChart pieChart = new PieChartBuilder().width(1200).height(500).
//                title("Demanding Companies for Jobs").
//                theme(Styler.ChartTheme.GGPlot2).build();
////
////            // Customize Chart
////            pieChartChart.getStyler().setDefaultSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Scatter);
//        pieChart.getStyler().setChartTitleVisible(true);
//        pieChart.getStyler().setLegendPosition(Styler.LegendPosition.OutsideS);
//        pieChart.getStyler().setMarkerSize(13);
////            Color[] sliceColors = new Color[]{new Color (180, 68, 50), new Color (130, 105, 120),
////                    new Color (80, 143, 160)};
////            pieChart.getStyler ().setSeriesColors (sliceColors);
////
////            // Series
////            Column job = jobDemandingCompanies.col("Job");
////            Column com = jobDemandingCompanies.col("Company");
//        for (Row r : job_Dem_Com) {
//            pieChart.addSeries((String) r.get(0), (Number) r.get(1));
//        }
//
////            // Display
//        new SwingWrapper(pieChart).displayChart();
        return "";
    }
}
