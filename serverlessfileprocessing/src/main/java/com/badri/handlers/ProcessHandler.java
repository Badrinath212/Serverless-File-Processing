package com.badri.handlers;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

public class ProcessHandler implements RequestHandler<SQSEvent, Void>{
	
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
			.endpointOverride(URI.create("http://localstack:4566"))
			 .region(Region.AP_SOUTH_1)
			 .credentialsProvider(
					 StaticCredentialsProvider.create(
							 AwsBasicCredentials.create("test","test")
					 )
			  )
			 .build();

	@Override
	public Void handleRequest(SQSEvent event, Context context) {
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
			
			for(SQSMessage record: event.getRecords()) {
				String body = record.getBody();
				
				ObjectMapper mapper = new ObjectMapper();
				
				Map<String,String> data = mapper.readValue(body, Map.class);
				
				String bucketName = data.get("bucket");
				
				String objKey = data.get("key");
				
				context.getLogger().log("bucket name: " + bucketName);
				context.getLogger().log("bucket key: " + objKey);
				
				
				try {
					long currentTime = System.currentTimeMillis() / 1000;
					long fiveMinutesAgo = currentTime-300;
					UpdateItemRequest locReq = UpdateItemRequest.builder()
							.tableName("FileMetaData")
							.key(Map.of("s3Key", AttributeValue.builder().s(objKey).build()))
							.updateExpression("SET #s = :processing, lastUpdatedAt = :lastUpdatedAt")
							.conditionExpression("#s = :unprocessed OR #s = :failed OR (#s = :processing AND lastUpdatedAt < :expiry)")
							.expressionAttributeNames(Map.of("#s","status"))
							.expressionAttributeValues(Map.of(
									":processing", AttributeValue.builder().s("PROCESSING").build(),
									":unprocessed", AttributeValue.builder().s("UNPROCESSED").build(),
									":failed", AttributeValue.builder().s("FAILED").build(),
									":lastUpdatedAt", AttributeValue.builder().n(String.valueOf(currentTime)).build(),
									":expiry", AttributeValue.builder().n(String.valueOf(fiveMinutesAgo)).build()
									))
							.build();
					ddc.updateItem(locReq); 
				} catch(ConditionalCheckFailedException e) {
					context.getLogger().log("Another lambda is processing, skipping");
					continue;
				}
				
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
				
				String fileId = objKey.substring(0, objKey.lastIndexOf("-"));
				
				context.getLogger().log("Before updateitemrequest ");
				
				try {
					UpdateItemRequest req = UpdateItemRequest.builder()
							.key(Map.of("s3Key",AttributeValue.builder().s(objKey).build()))
							.tableName("FileMetaData")
							.updateExpression("SET #s = :status, lineCount = :lc")
							.conditionExpression("#s <> :status")
							.expressionAttributeNames(Map.of("#s","status"))
							.expressionAttributeValues(Map.of(":status",AttributeValue.builder().s("PROCESSED").build(),":lc", 
									AttributeValue.builder().n(String.valueOf(lineCount)).build()))
							.build();
					
					ddc.updateItem(req);
					context.getLogger().log("file proccessed data updadted to table");
				} catch (Exception e) {
					UpdateItemRequest falReq = UpdateItemRequest.builder()
							.key(Map.of("s3Key",AttributeValue.builder().s(objKey).build()))
							.tableName("FileMetaData")
							.updateExpression("SET #s = :status")
							.expressionAttributeNames(Map.of("#s","status"))
							.expressionAttributeValues(Map.of(":status",AttributeValue.builder().s("FAILED").build()))
							.build();
					
					ddc.updateItem(falReq);
					context.getLogger().log("processing is failed");
					throw e;
				}
				
				
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

}
