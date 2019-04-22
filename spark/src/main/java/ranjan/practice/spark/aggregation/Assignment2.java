package ranjan.practice.spark.aggregation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class Assignment2 {
    public static void main( String[] args )
    {
        SparkSession sparkSession = SparkSession.builder().appName("Assignment-1").enableHiveSupport().getOrCreate();
        Dataset result = sparkSession.sql("SELECT `monthyear`, `actiongeo_countrycode`, count(`globaleventid`) as number_of_events FROM `csv`.`gdelt2` GROUP BY `monthyear`,`actiongeo_countrycode`");
        result.write().mode(SaveMode.Overwrite).saveAsTable("csv.GDELT_out_2");
        System.out.println("Report has been saved to table csv.GDELT_out_2");
    }


}
