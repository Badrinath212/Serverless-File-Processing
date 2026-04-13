package com.badri.handlers;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification.S3EventNotificationRecord;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

public class MessageSenderHandler implements RequestHandler<S3Event, Void> {
	
	SqsClient SQSCLIENT = SqsClient.builder()
			.endpointOverride(URI.create("http://host.docker.internal:4566"))
			 .region(Region.AP_SOUTH_1)
			 .credentialsProvider(
					 StaticCredentialsProvider.create(
							 AwsBasicCredentials.create("test","test")
					 )
			  )
			 .build();

	@Override
	public Void handleRequest(S3Event event, Context context) {
		context.getLogger().log("-----------senderhandler started----------");
		try {
			if(event == null) {
				context.getLogger().log("Event is null");
				return null;
			}
			if(event.getRecords().size() == 0) {
				context.getLogger().log("Event size is zere");
				return null;
			}
			
			for(S3EventNotificationRecord record: event.getRecords()) {
				String bucketName = record.getS3().getBucket().getName();
				String s3Key = URLDecoder.decode(record.getS3().getObject().getKey(),StandardCharsets.UTF_8);
				
				Map<String,String> map = new HashMap<>();
				
				map.put("key", s3Key);
				map.put("bucket", bucketName);
				
				ObjectMapper mapper = new ObjectMapper();
				
				String jsonBody = mapper.writeValueAsString(map);
				
				context.getLogger().log("json body: " + jsonBody);
				
				String queueUrl = "http://host.docker.internal:4566/000000000000/file-processing-queue";
				SendMessageRequest request = SendMessageRequest.builder()
						.queueUrl(queueUrl)
						.messageBody(jsonBody)
						.build();
				
				SQSCLIENT.sendMessage(request);
				
				context.getLogger().log("Message sent to queue");
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
		return null;
	}

}
