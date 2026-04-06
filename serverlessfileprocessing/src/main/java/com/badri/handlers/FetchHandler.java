package com.badri.handlers;

import java.net.URI;
import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

public class FetchHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent>{
	
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
	
	private static final ObjectMapper mapper = new ObjectMapper();

	@Override
	public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent request, Context context) {
		try {
			context.getLogger().log("---------fetchhandler started---------");
			Map<String, String> pathParameters = request.getPathParameters();
			
			if(!pathParameters.containsKey("fileId")) {
				return new APIGatewayProxyResponseEvent()
						.withStatusCode(400)
						.withBody("fileId is missing in path");
			}
			
			String fileId = pathParameters.get("fileId");
			
			GetItemRequest itemRequest = GetItemRequest.builder()
											.key(Map.of("fileId",AttributeValue.builder().s(fileId).build()))
											.tableName("FileMetaData")
											.attributesToGet("bucketName", "s3Key")
											.build();
			
			GetItemResponse itemResponse = ddc.getItem(itemRequest);
			
			Map<String, AttributeValue> data = itemResponse.item();
			
			String bucketName = data.get("bucketName").s();
			String s3Key = data.get("s3Key").s();
			
			GetObjectRequest objRequest = GetObjectRequest.builder()
					.bucket(bucketName)
					.key(s3Key)
					.build();
			
			ResponseBytes<GetObjectResponse> objResponse = S3CLIENT.getObject(objRequest, ResponseTransformer.toBytes());
					
			byte[] fileBytes = objResponse.asByteArray();
			
			return new APIGatewayProxyResponseEvent()
					.withBody(new String(fileBytes))
					.withStatusCode(200);
			
		} catch(Exception e) {
			return new APIGatewayProxyResponseEvent()
					.withStatusCode(500)
					.withBody("Something went wrong");
		}
		return null;
	}

}
