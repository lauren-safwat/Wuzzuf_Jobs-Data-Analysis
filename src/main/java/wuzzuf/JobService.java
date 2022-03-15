package wuzzuf;

import org.apache.spark.sql.*;
import org.knowm.xchart.*;
import org.knowm.xchart.style.Styler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;
import java.io.IOException;
import static org.apache.spark.sql.functions.regexp_replace;


@Service
public class JobService implements JobDAO{
    @Autowired
    private SparkSession sparkSession;

    @Autowired
    private Dataset<Job> wuzzufJobs;

    @Override
    public Dataset<Job> getSample() {
        return wuzzufJobs.limit(50);
    }

    @Override
    public String getSchema() {
        StringBuilder response = new StringBuilder("<div style='height: 600px; overflow-y: auto; left: -30px;'><table style='width: 45%;'><tr><th>Column</th><th>Data Type</th><th>Count</th><th>Unique Values</tr>");

        for (Tuple2<String, String> dtype : wuzzufJobs.dtypes()) {
            response.append(String.format("<tr><td>%s</td>", dtype._1));
            response.append(String.format("<td>%s</td>", dtype._2));
            response.append(String.format("<td>%d</td>", wuzzufJobs.select(dtype._1).count()));
            response.append(String.format("<td>%d</td></tr>", wuzzufJobs.select(dtype._1).distinct().count()));
        }

        response.append("</table></div>");
        return response.toString();
    }

    @Override
    public String cleanData() {
        StringBuilder response = new StringBuilder("<h2>- Number of records before cleaning: ");
        response.append(wuzzufJobs.count()).append("</h2>");

        long nDuplicates = wuzzufJobs.count() - wuzzufJobs.distinct().count();

        long nNull = wuzzufJobs.count();
        wuzzufJobs = wuzzufJobs.withColumn("YearsExp", regexp_replace(wuzzufJobs.col("YearsExp"), " Yrs of Exp", "")).as(Encoders.bean(Job.class));
        wuzzufJobs = wuzzufJobs.filter(wuzzufJobs.col("YearsExp").notEqual("null"));
        nNull -= wuzzufJobs.count();

        wuzzufJobs = wuzzufJobs.dropDuplicates();

        response.append("<h2>- Number of duplicates: ").append(nDuplicates).append("</h2>");
        response.append("<h2>- Number of records containing null values: ").append(nNull).append("</h2>");
        response.append("<h2>- Number of records after cleaning: ").append(wuzzufJobs.count()).append("</h2>");
        return response.toString();
    }

    @Override
    public Dataset<Row> jobsPerCompany() {
        return wuzzufJobs.groupBy("Company").agg(functions.count("Title").as("Number of vacancies")).sort(functions.desc("Number of vacancies"));
    }

    @Override
    public Dataset<Row> mostPopularJobTitles() {
        return wuzzufJobs.groupBy("Title").agg(functions.count("Title").as("Number of vacancies")).sort(functions.desc("Number of vacancies"));
    }

    @Override
    public Dataset<Row> mostPopularAreas() {
        return wuzzufJobs.groupBy("Location").agg(functions.count("Title").as("Number of vacancies")).sort(functions.desc("Number of vacancies"));
    }

    public void createPieChart(Dataset<Row> dataset, String title, String path) {
        PieChart pieChart = new PieChartBuilder().width(800).height(500).title(title).theme(Styler.ChartTheme.GGPlot2).build();

        pieChart.getStyler().setLegendPosition(Styler.LegendPosition.OutsideS);

        dataset.limit(8).collectAsList().forEach(row -> pieChart.addSeries(row.getString(0), row.getLong(1)));

        try {
            BitmapEncoder.saveBitmap(pieChart, "src/main/resources/static/img/" + path, BitmapEncoder.BitmapFormat.PNG);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void createBarChart(Dataset<Row> dataset, String title, String xLabel, String yLabel, String path) {
        CategoryChart barChart = new CategoryChartBuilder().width(800).height(500).title(title).xAxisTitle(xLabel).yAxisTitle(yLabel).build();

        barChart.getStyler().setXAxisLabelRotation(45);

        barChart.addSeries(xLabel, dataset.limit(10).map(job -> job.getString(0), Encoders.STRING()).collectAsList(), dataset.limit(10).map(job -> job.getLong(1), Encoders.LONG()).collectAsList());

        try {
            BitmapEncoder.saveBitmap(barChart, "src/main/resources/static/img/" + path, BitmapEncoder.BitmapFormat.PNG);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Dataset<Row> mostRequiredSkills() {
        wuzzufJobs.createOrReplaceTempView("WuzzufView");
        Dataset<Row> skills = sparkSession.sql("SELECT skills FROM WuzzufView");
        skills.createOrReplaceTempView("SkillsView");

        Dataset<Row> mostRequiredSkills = skills.sqlContext().sql(
                "SELECT Skill, count(Skill) AS Count\n" +
                        "FROM (SELECT EXPLODE(SPLIT(skills, ',')) AS Skill FROM SkillsView)\n" +
                        "GROUP BY Skill\n" +
                        "ORDER BY Count DESC"
        );

        return mostRequiredSkills;
    }
}
