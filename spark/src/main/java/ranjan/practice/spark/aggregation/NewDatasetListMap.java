package ranjan.practice.spark.aggregation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class NewDatasetListMap {
	public static void main(String[] args) {
		SparkSession session = SparkSession.builder().enableHiveSupport().getOrCreate();
		List result = new ArrayList();
		CustomBean customBean=new CustomBean((Map) new HashMap().put("sdkdk", "dfjhier"));
		Map<String, String> map=new HashMap<String, String>();
		map.put("key", "valule");
		customBean.setMap(map);
		result.add(customBean);
//		result.add(new HashMap().put("Month Year Count", "monthyear=value"));
//		result.add(new HashMap().put("Actor1Geo_CountryCode Count","actor1geo_countrycode=value"));
//		result.add(new HashMap().put("Event Type Count", "eventcode=value"));
//		result.add(new HashMap().put("Quad Class Count", "quadclass=value"));
		Dataset outputSet = session.createDataset(result, Encoders.bean(CustomBean.class));
		outputSet.show();
	}
}
