package pubsubapp.subscriber;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.pubsub.PubsubUtils;
import org.apache.spark.streaming.pubsub.SparkGCPCredentials;
import org.apache.spark.streaming.pubsub.SparkPubsubMessage;
import org.springframework.stereotype.Component;

import com.google.cloud.ServiceOptions;

@Component
public class CustomSparkStreamingSubscriber {
	private static final String PROJECT_ID = ServiceOptions.getDefaultProjectId();

	public void subscribe(String topic_id) {
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Pubshub");
		SparkSession session = SparkSession.builder().config(conf).getOrCreate();
		JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(session.sparkContext());

		JavaStreamingContext javaStreamingContext = new JavaStreamingContext(javaSparkContext, Durations.seconds(60));

		SparkGCPCredentials gcpCredentials = new SparkGCPCredentials.Builder()
				.jsonServiceAccount("/home/rchoudhury/google_app_cred/spark-4b7b6175d073.json").build();
		System.out.println("gcpCredentials: " + gcpCredentials);

		JavaReceiverInputDStream<SparkPubsubMessage> lines = PubsubUtils.createStream(javaStreamingContext, PROJECT_ID,
				topic_id, gcpCredentials, StorageLevel.MEMORY_AND_DISK_2());

		lines.foreachRDD(line -> {
			List<SparkPubsubMessage> content = line.collect();
			content.forEach(msg -> {
				String query = new String(msg.getData());
				System.out.println("PubSub Message: " + query);
				Dataset<Row> dataset = session.sql(query);
				dataset.write().mode(SaveMode.Append).format("csv").saveAsTable("csv.pubsub_out");
			});
		});
		/* JavaDStream<Object> msgs = lines.map(msg -> new String(msg.getData())); */
		System.out.println("Printed");
		javaStreamingContext.start();

	}
}
