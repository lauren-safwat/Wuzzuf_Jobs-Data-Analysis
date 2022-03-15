package wuzzuf;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

@Configuration
public class SparkConfig {

    @Value("${spark.app.name}")
    private String appName;
    @Value("${spark.master}")
    private String masterUri;

    @Bean
    public SparkConf sparkConf() {
        return new SparkConf().setAppName(appName).setMaster(masterUri).set( "spark.driver.host", "localhost" );
    }

    @Bean
    public JavaSparkContext javaSparkContext() {
        return new JavaSparkContext(sparkConf());
    }

    @Bean
    public SparkSession sparkSession() {
        Logger.getLogger("org.apache").setLevel(Level.OFF);
        return SparkSession.builder().sparkContext(javaSparkContext().sc()).getOrCreate();
    }

    @Bean
    public Dataset<Job> readData(SparkSession sparkSession){
        return sparkSession.read().option("header","true").csv("src/main/resources/Wuzzuf_Jobs.csv").as(Encoders.bean(Job.class));
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }

}
