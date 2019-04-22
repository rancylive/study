package pubsubapp.publisher;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;

@Component
public class PubsubPublisher {
	private static final String PROJECT_ID = ServiceOptions.getDefaultProjectId();

	public void publish(List<String> messages, String topic_id) throws Exception {
		ProjectTopicName topicName = ProjectTopicName.of(PROJECT_ID, topic_id);
		Publisher publisher = null;
		GoogleCredentials credentials=GoogleCredentials.fromStream(new FileInputStream("/home/rchoudhury/google_app_cred/spark-4b7b6175d073.json")).createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
		CredentialsProvider credentialsProvider=FixedCredentialsProvider.create(credentials);
		List<ApiFuture<String>> futures = new ArrayList<ApiFuture<String>>();
		try {
			publisher = Publisher.newBuilder(topicName).setCredentialsProvider(credentialsProvider).build();
			for (String message : messages) {
				ByteString data = ByteString.copyFromUtf8(message);
				PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
				ApiFuture<String> future = publisher.publish(pubsubMessage);
				futures.add(future);
			}
		} finally {
			List<String> messageIds = ApiFutures.allAsList(futures).get();
			for (String messageId : messageIds) {
				System.out.println(messageId);
			}
			if (publisher != null) {
				publisher.shutdown();
			}
		}

	}
}
