package ranjan.practice.spark.rdd;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Map_flatMap {
	public static void main(String[] args) {
		SparkSession spark=SparkSession.builder().master("local").getOrCreate();
		Dataset dataset=spark.createDataset(Arrays.asList("Roses are red"), Encoders.STRING());
		Dataset out = dataset.flatMap(new ExampleFMap(), Encoders.INT());
		Dataset out1=out.flatMap(new FlatMapFunction<String, Integer>() {
			@Override
			public Iterator<Integer> call(String t) throws Exception {
				return Arrays.asList(t.length()).iterator();
			}
		}, Encoders.STRING());
		out.show();
		out1.show();
	}
	
	static FlatMapFunction<String, String> fun(Object wd) {
		return new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String t) throws Exception {
				return (Iterator<String>) Arrays.asList(t.split(" "));
			}};
		
	}
}
