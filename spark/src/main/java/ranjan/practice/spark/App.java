package ranjan.practice.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        SparkSession sparkSession = SparkSession.builder().appName("Assignment-1").getOrCreate();
        Dataset result = sparkSession.sql("SELECT `actiongeo_countrycode`, count(DISTINCT `globaleventid`) FROM `csv`.`gdelt2` GROUP BY `actiongeo_countrycode`");
        result.write().saveAsTable("csv.GDELT_out_1");
        System.out.println("Report has been saved to table csv.GDELT_out_1");
    }
}
