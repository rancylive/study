package pubsubapp.subscriber;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.pubsub.PubsubUtils;
import org.apache.spark.streaming.pubsub.SparkGCPCredentials;
import org.apache.spark.streaming.pubsub.SparkPubsubMessage;
import org.springframework.stereotype.Component;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.common.collect.Lists;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;

@Component
public class CustomSynSubscriber {
	private static final String PROJECT_ID = ServiceOptions.getDefaultProjectId();

	public List<ReceivedMessage> subscribe(String topic_id, int numOfMessages)
			throws FileNotFoundException, IOException {
		GoogleCredentials credentials = GoogleCredentials
				.fromStream(new FileInputStream("/home/rchoudhury/google_app_cred/spark-4b7b6175d073.json"))
				.createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
		CredentialsProvider credentialsProvider = FixedCredentialsProvider.create(credentials);
		SubscriberStubSettings subscriberStubSettings = SubscriberStubSettings.newBuilder()
				.setCredentialsProvider(credentialsProvider).build();
		try {
			SubscriberStub subscriber = GrpcSubscriberStub.create(subscriberStubSettings);
			String subscriptionName = ProjectSubscriptionName.format(PROJECT_ID, topic_id);
			PullRequest pullRequest = PullRequest.newBuilder().setMaxMessages(numOfMessages).setReturnImmediately(false) // return
																															// immediately
																															// if
																															// messages
																															// are
																															// not
																															// available
					.setSubscription(subscriptionName).build();

			// use pullCallable().futureCall to asynchronously perform this operation
			PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);
			List<String> ackIds = new ArrayList();
			for (ReceivedMessage message : pullResponse.getReceivedMessagesList()) {
				// handle received message
				// ...
				ackIds.add(message.getAckId());
			}
			// acknowledge received messages
			AcknowledgeRequest acknowledgeRequest = AcknowledgeRequest.newBuilder().setSubscription(subscriptionName)
					.addAllAckIds(ackIds).build();
			// use acknowledgeCallable().futureCall to asynchronously perform this operation
			subscriber.acknowledgeCallable().call(acknowledgeRequest);
			return pullResponse.getReceivedMessagesList();
		} finally {
		}
	}
}
