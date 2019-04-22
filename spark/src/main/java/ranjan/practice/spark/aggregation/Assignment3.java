package ranjan.practice.spark.aggregation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class Assignment3 {
    public static void main( String[] args )
    {
        SparkSession sparkSession = SparkSession.builder().appName("Assignment-1").enableHiveSupport().getOrCreate();
        Dataset result = sparkSession.sql("select * from gdelt2 gd inner join (select monthyear,count(globaleventid) as gc from gdelt2 group by monthyear sort by gc desc limit 1) as gdd on gdd.monthyear=gd.monthyear limit 10");
        result.write().mode(SaveMode.Overwrite).saveAsTable("csv.GDELT_out_3");
        System.out.println("Report has been saved to table csv.GDELT_out_3");
    }


}
