package com.example.Wuzzuf;

import org.apache.spark.sql.*;
import org.knowm.xchart.*;
import org.knowm.xchart.style.Styler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
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
        Dataset<Row> jobsPerCompany = wuzzufJobs.groupBy("Company").agg(functions.count("Title").as("Count")).sort(functions.desc("Count"));
        List<String> data = jobsPerCompany.map(row -> row.mkString("<tr><td>", "</td><td>", "</td></tr>"), Encoders.STRING()).collectAsList();

        String response = String.format("<h1 style=\"text-align:center; padding-top: 8px;\">%s</h1>", "Most Demanding Companies for Jobs") +
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
        Dataset<Row> mostPopularJobs = wuzzufJobs.groupBy("Title").agg(functions.count("Title").as("Count")).sort(functions.desc("Count"));
        List<String> data = mostPopularJobs.map(row -> row.mkString("<tr><td>", "</td><td>", "</td></tr>"), Encoders.STRING()).collectAsList();

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
        Dataset<Row> mostPopularAreas = wuzzufJobs.groupBy("Location").agg(functions.count("Title").as("Count")).sort(functions.desc("Count"));
        List<String> data = mostPopularAreas.map(row -> row.mkString("<tr><td>", "</td><td>", "</td></tr>"), Encoders.STRING()).collectAsList();

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
    public void jobsPerCompanyPieChart() {
        Dataset<Row> jobsPerCompany = wuzzufJobs.groupBy("Company").agg(functions.count("Title").as("Count")).sort(functions.desc("Count"));

        PieChart pieChart = new PieChartBuilder().width(1200).height(700).title("Most Demanding Companies for Jobs Pie Chart").theme(Styler.ChartTheme.GGPlot2).build();

        pieChart.getStyler().setLegendPosition(Styler.LegendPosition.OutsideS);

        jobsPerCompany.limit(10).collectAsList().forEach(row -> pieChart.addSeries(row.getString(0), row.getLong(1)));

        try {
            BitmapEncoder.saveBitmap(pieChart, "src/main/resources/pieChart.png", BitmapEncoder.BitmapFormat.PNG);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void createBarChart(Dataset<Row> df, String title, String xLabel, String yLabel, String path) {
        CategoryChart barChart = new CategoryChartBuilder().width(1200).height(700).title(title).xAxisTitle(xLabel).yAxisTitle(yLabel).build();

        barChart.getStyler().setXAxisLabelRotation(45);

        barChart.addSeries(xLabel, df.limit(10).map(job -> job.getString(0), Encoders.STRING()).collectAsList(), df.limit(10).map(job -> job.getLong(1), Encoders.LONG()).collectAsList());

        try {
            BitmapEncoder.saveBitmap(barChart, path, BitmapEncoder.BitmapFormat.PNG);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void mostPopularJobsBarChart() {
        Dataset<Row> mostPopularJobs = wuzzufJobs.groupBy("Title").agg(functions.count("Title").as("Count")).sort(functions.desc("Count"));
        createBarChart(mostPopularJobs, "Most Popular Job Titles Bar Chart", "Job Titles", "Number of Vacancies", "src/main/resources/jobsBarChart.png");
    }

    @Override
    public void mostPopularAreasBarChart() {
        Dataset<Row> mostPopularAreas = wuzzufJobs.groupBy("Location").agg(functions.count("Title").as("Count")).sort(functions.desc("Count"));
        createBarChart(mostPopularAreas, "Most Popular Areas Bar Chart", "Area", "Number of Vacancies", "src/main/resources/areasBarChart.png");
    }

    @Override
    public String mostRequiredSkills() {
        wuzzufJobs.createOrReplaceTempView("WuzzufView");
        Dataset<Row> skills = sparkSession.sql("SELECT skills FROM WuzzufView");
        skills.createOrReplaceTempView("SkillsView");

        Dataset<Row> mostRequiredSkills = skills.sqlContext().sql(
                "SELECT Skill, count(Skill) AS Count\n" +
                        "FROM (SELECT EXPLODE(SPLIT(skills, ',')) AS Skill FROM SkillsView)\n" +
                        "GROUP BY Skill\n" +
                        "ORDER BY Count DESC"
        );

        List<String> data = mostRequiredSkills.map(row -> row.mkString("<tr><td>", "</td><td>", "</td></tr>"), Encoders.STRING()).collectAsList();

        String response = String.format("<h1 style=\"text-align:center; padding-top: 8px;\">%s</h1>", "Most Important Skills Required") +
                "<style>\n" +
                    "table {width: 70%; top: 0; margin-left: auto; margin-right: auto;}\n" +
                    "th {background: #99BBCB; position: sticky;}\n" +
                    "th,td {padding: 8px 15px;}\n" +
                    "table, th, td {border-collapse: collapse; border: 2px solid black;}\n" +
                "</style>\n" +
                "<table>\n" +
                    "<tr> <th>Skill</th> <th>Count</th> </tr>" +
                    String.join("", data) +
                "</table>";

        return response;
    }
}
