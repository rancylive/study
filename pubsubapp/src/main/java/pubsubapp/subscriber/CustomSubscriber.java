package pubsubapp.subscriber;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.springframework.stereotype.Component;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.common.collect.Lists;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;

@Component
public class CustomSubscriber {
	private static final String PROJECT_ID = ServiceOptions.getDefaultProjectId();

	public void subscribe(String topic_id) throws FileNotFoundException, IOException {
		ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(PROJECT_ID, topic_id);
		System.out.println("Subscription:" + subscriptionName);
		MessageReceiver receiver = new MessageReceiver() {
			String result;

			public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
				result = message.getData().toStringUtf8();
				System.out.println("Message ID:" + message.getMessageId());
				System.out.println("Data: " + message.getData().toStringUtf8());
				consumer.ack();
			}
		};
		Subscriber subscriber = null;
		try {
			GoogleCredentials credentials = GoogleCredentials
					.fromStream(new FileInputStream("/home/rchoudhury/google_app_cred/spark-4b7b6175d073.json"))
					.createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
			CredentialsProvider credentialsProvider = FixedCredentialsProvider.create(credentials);
			System.out.println("credentialsProvider: "+credentialsProvider+"credentials: "+credentials);
			subscriber = Subscriber.newBuilder(subscriptionName, receiver).setCredentialsProvider(credentialsProvider)
					.build();
			System.out.println("Subscriber: "+subscriber.getSubscriptionNameString());
			subscriber.startAsync();
		} finally {
			if (subscriber != null) {
				subscriber.stopAsync();
			}
		}
	}
}
