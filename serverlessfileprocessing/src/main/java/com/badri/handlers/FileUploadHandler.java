package com.badri.handlers;

import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.AddPermissionRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.Event;
import software.amazon.awssdk.services.s3.model.LambdaFunctionConfiguration;
import software.amazon.awssdk.services.s3.model.NotificationConfiguration;
import software.amazon.awssdk.services.s3.model.PutBucketNotificationConfigurationRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

@SuppressWarnings("unchecked")
public class FileUploadHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent>{
	
	private static final ObjectMapper mapper = new ObjectMapper();
	
	private static final LambdaClient LAMBDACLIENT =  LambdaClient.builder()
			.endpointOverride(URI.create("http://localstack:4566"))
			.region(Region.AP_SOUTH_1)
			.credentialsProvider(
					StaticCredentialsProvider.create(
							AwsBasicCredentials.create("test","test")
			))
			.fipsEnabled(false)
			.build();
	
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
	public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent request, Context context) {
		try {
			
			context.getLogger().log("----Upload handler called-----");

			String jsonBody = request.getBody();
			
			Map<String,String> data = mapper.readValue(jsonBody,Map.class);
			
			String fileName = data.get("fileName");
			
			byte[] fileBytes = Base64.getDecoder().decode(data.get("fileContent"));
			
			String fileId = generateFileId(fileBytes);
			
			String keyVal = fileId + "-" + fileName;
			
			String bucketName = "file-bucket";
			
			PutObjectRequest req = PutObjectRequest.builder()
					.bucket(bucketName)
					.key(keyVal)
					.build();
			
			S3CLIENT.putObject(req, RequestBody.fromBytes(fileBytes));
			
			context.getLogger().log("File uploaded to s3");
			
			
			String tableName = "FileMetaData";
			
			String createdAt = Instant.now().toString();
			
			String status = "unprocessed";
			
			int lineCount = 0;
			
			context.getLogger().log("Before uploading to filemetadata table");
			
			putItemInTable(ddc, tableName, bucketName, keyVal, createdAt, fileId, status, lineCount);
			
			
			context.getLogger().log("meta data uploaded to the dynamodb table");
			
			
			return new APIGatewayProxyResponseEvent()
					.withStatusCode(201)
					.withBody("data is uploaded to the cloud");
		} catch(ConditionalCheckFailedException e) {
			return new APIGatewayProxyResponseEvent()
					.withBody("File already there")
					.withStatusCode(400);
		
		} catch(Exception e) {
			return new APIGatewayProxyResponseEvent()
					.withStatusCode(500)
					.withBody("Something went wrong. please try again");
		}
	}
	

	
	public void putItemInTable(DynamoDbClient ddc, String tableName, String bucketName, String s3Key, String createdAt, String fileId, String status,int lineCount) {
			HashMap<String, AttributeValue> itemValues = new HashMap<>();
			
			itemValues.put("fileId", AttributeValue.builder().s(fileId).build());
			itemValues.put("bucketName", AttributeValue.builder().s(bucketName).build());
			itemValues.put("s3Key", AttributeValue.builder().s(s3Key).build());
			itemValues.put("createdAt", AttributeValue.builder().s(createdAt).build());
			itemValues.put("status", AttributeValue.builder().s(status).build());
			itemValues.put("lineCount", AttributeValue.builder().n(String.valueOf(lineCount)).build());
			PutItemRequest request = PutItemRequest.builder()
					.item(itemValues)
					.tableName(tableName)
					.conditionExpression("attribute_not_exists(s3Key)")
					.build();
			
			ddc.putItem(request);
			
	}
	
	public String generateFileId(byte[] fileBytes) throws NoSuchAlgorithmException {
	    MessageDigest digest = MessageDigest.getInstance("SHA-256");
	    byte[] hashBytes = digest.digest(fileBytes);

	    StringBuilder hexString = new StringBuilder();
	    for (byte b : hashBytes) {
	        String hex = Integer.toHexString(0xff & b);
	        if (hex.length() == 1) hexString.append('0');
	        hexString.append(hex);
	    }
	    return hexString.toString();
	}

	
}
