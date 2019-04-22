package pubsubapp.service;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.pubsub.v1.ReceivedMessage;

import pubsubapp.publisher.PubsubPublisher;
import pubsubapp.subscriber.CustomSparkStreamingSubscriber;
import pubsubapp.subscriber.CustomSubscriber;
import pubsubapp.subscriber.CustomSynSubscriber;
import pubsubapp.vo.Query;

@Service
public class QueryService {
	@Autowired
	private PubsubPublisher publisher;

	@Autowired
	private CustomSubscriber customSubscriber;
	
	@Autowired
	private CustomSynSubscriber customSynSubscriber;
	
	@Autowired
	private CustomSparkStreamingSubscriber customSparkStreamingSubscriber;
	
	private static final String topic_id = "test";
	
	public String publishQuery(Query query) throws Exception {
		List<String> messages = Arrays.asList(query.getFisrtQuery(),query.getSecondQuery());
		publisher.publish(messages,topic_id);
		return "Message published for topic "+topic_id;
	}
	
	public List<String> subscribeQueryOutput() throws FileNotFoundException, IOException {
		List<String> response=new ArrayList<String>();
		//customSubscriber.subscribe(topic_id);
		/*List<ReceivedMessage> messages = customSynSubscriber.subscribe(topic_id, 2);
		for(ReceivedMessage message: messages) {
			String msg=message.getMessage().getData().toStringUtf8();
			response.add(msg);
			System.out.println(msg);
		}*/
		customSparkStreamingSubscriber.subscribe(topic_id);
		return response;
	}
}
