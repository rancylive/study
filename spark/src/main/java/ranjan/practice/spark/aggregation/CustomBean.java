package ranjan.practice.spark.aggregation;

import java.util.HashMap;
import java.util.Map;

public class CustomBean {
	Map<String, String> map=new HashMap<String, String>();
	public CustomBean(Map map) {
		this.map=map;
	}
	public Map<String, String> getMap() {
		return map;
	}
	public void setMap(Map<String, String> map) {
		this.map = map;
	}
	
}
