package com.badri.handlers;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

public class ProcessHandler implements RequestHandler<S3Event, Void>{
	
	private static final S3Client S3CLIENT = S3Client.builder()
			 .endpointOverride(URI.create("http://localstack:4566"))
			 .region(Region.AP_SOUTH_1)
			 .credentialsProvider(
					 StaticCredentialsProvider.create(
							 AwsBasicCredentials.create("test","test")
					 )
			  ).serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
			 .build();
	
	private static final DynamoDbClient ddc = DynamoDbClient.builder()
			.endpointOverride(URI.create("http://localhost:4566"))
			 .region(Region.AP_SOUTH_1)
			 .credentialsProvider(
					 StaticCredentialsProvider.create(
							 AwsBasicCredentials.create("test","test")
					 )
			  )
			 .build();

	@Override
	public Void handleRequest(S3Event event, Context context) {
		try {
			context.getLogger().log("------process handler started--------");
			if(event == null) {
				context.getLogger().log("Event is null");
				return null;
			}
			if(event.getRecords() == null) {
				context.getLogger().log("Event records are null");
				return null;
			}
			context.getLogger().log("Records size:" + event.getRecords().size());
			
			for(S3EventNotification.S3EventNotificationRecord record: event.getRecords()) {
				String bucketName = record.getS3().getBucket().getName();
				
				String objKey = java.net.URLDecoder.decode(
	                    record.getS3().getObject().getKey(),
	                    java.nio.charset.StandardCharsets.UTF_8
	            );
				
				GetObjectRequest request = GetObjectRequest.builder()
						.bucket(bucketName)
						.key(objKey)
						.build();
				
				ResponseBytes<GetObjectResponse> response = S3CLIENT.getObject(request, ResponseTransformer.toBytes());
				byte[] fileBytes = response.asByteArray();
				
				String content = new String(fileBytes, StandardCharsets.UTF_8);
				
				int lineCount = 0;
				for (String line : content.split("\\r?\\n")) {
				    if (!line.isEmpty()) {
				        lineCount++;
				    }
				}
				
				String fileId = objKey.split("/")[1].split("-")[0];
				
				UpdateItemRequest req = UpdateItemRequest.builder()
						.key(Map.of("fileId",AttributeValue.builder().s(fileId).build()))
						.updateExpression("SET status = :status, lineCount = :lc")
						//.expressionAttributeNames(Map.of("#s","status"))
						.expressionAttributeValues(Map.of(":status",AttributeValue.builder().s("processed").build(),":lc", 
								AttributeValue.builder().n(String.valueOf(lineCount)).build()))
						.build();
				
				ddc.updateItem(req);
				context.getLogger().log("file proccessed data updadted to table");
				
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

}
