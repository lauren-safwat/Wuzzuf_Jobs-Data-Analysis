package wuzzuf;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface JobDAO {
    Dataset<Job> getSample();
    String getSchema();
    String cleanData();
    Dataset<Row> jobsPerCompany();
    Dataset<Row> mostPopularJobTitles();
    Dataset<Row> mostPopularAreas();
    Dataset<Row> mostRequiredSkills();
}
