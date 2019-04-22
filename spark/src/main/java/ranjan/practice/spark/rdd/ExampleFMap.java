package ranjan.practice.spark.rdd;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;

public class ExampleFMap implements FlatMapFunction<String, String>{

	@Override
	public Iterator<String> call(String t) throws Exception {
		return Arrays.asList(t.split(" ")).iterator();
	}

}
