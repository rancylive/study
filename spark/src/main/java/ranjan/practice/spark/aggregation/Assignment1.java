package ranjan.practice.spark.aggregation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class Assignment1 {
    public static void main( String[] args )
    {
        SparkSession sparkSession = SparkSession.builder().appName("Assignment-1").enableHiveSupport().getOrCreate();
        Dataset result = sparkSession.sql("SELECT `actiongeo_countrycode`, count(globaleventid) as number_of_events FROM `csv`.`gdelt2` GROUP BY `actiongeo_countrycode`");
        result.write().mode(SaveMode.Overwrite).saveAsTable("csv.GDELT_out_1");
        System.out.println("Report has been saved to table csv.GDELT_out_1");
    }
}
