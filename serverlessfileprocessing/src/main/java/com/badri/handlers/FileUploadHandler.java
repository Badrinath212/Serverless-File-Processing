package com.badri.handlers;

import java.net.URI;
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
			.endpointOverride(URI.create("http://localhost:4566"))
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
			.endpointOverride(URI.create("http://localhost:4566"))
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
			
			context.getLogger().log("----Upload hanlder called-----");
			String jsonBody = request.getBody();
			
			Map<String,String> data = mapper.readValue(jsonBody,Map.class);
			
			String fileName = data.get("fileName");
			
			UUID uuid = UUID.randomUUID();
			
			String fileId = uuid.toString();
			
			String keyVal = fileId + "-" + fileName;
			
			String bucketName = "file-bucket";
			
			byte[] fileBytes = Base64.getDecoder().decode(data.get("fileContent"));
			
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
			
			putItemInTable(ddc, tableName, bucketName, fileName, createdAt, fileId, status, lineCount);
			
			context.getLogger().log("meta data uploaded to the dynamodb table");
			
			allowS3ToTriggerLambda(LAMBDACLIENT);
			
			createS3Trigger("lksljflsjkflsdjflsdfjlsd");  // need to update this
			
			return new APIGatewayProxyResponseEvent()
					.withStatusCode(201)
					.withBody("data is uploaded to the clount");
		
		} catch(Exception e) {
			return new APIGatewayProxyResponseEvent()
					.withStatusCode(500)
					.withBody("Something went wrong. please try again");
		}
		return null;
	}
	
	public void allowS3ToTriggerLambda(LambdaClient client) {
		AddPermissionRequest request = AddPermissionRequest.builder()
				.functionName("FileProcessingLambda")
				.statementId("s3-trigger-1")
				.action("lambda:InvokeFunction")
				.principal("s3.amazonaws.com")
				.sourceArn("arn:aws:s3:::file-bucket")
				.build();
		
		client.addPermission(request);
		
		System.out.println("permission granted for the s3 to trigger lambda function");
	}
	
	public void createS3Trigger(String lambdaArn) {
			
			// 1. Setup a filter so it only looks for .pdf files
//			S3KeyFilter keyFilter = S3KeyFilter.builder()
//					.filterRules(FilterRule.builder().name(FilterRuleName.SUFFIX).value(".pdf").build())
//					.build();
//			
//			NotificationConfigurationFilter filter = NotificationConfigurationFilter.builder()
//					.key(keyFilter)
//					.build();
			
			// 2. Point to your Lambda
			LambdaFunctionConfiguration lambdaConfig = LambdaFunctionConfiguration.builder()
					.lambdaFunctionArn(lambdaArn)
					.events(Event.S3_OBJECT_CREATED_PUT)
					.build();
			
			// 3. Apply the config to the bucket
			PutBucketNotificationConfigurationRequest request = PutBucketNotificationConfigurationRequest.builder()
					.bucket("file-bucket")
					.notificationConfiguration(NotificationConfiguration.builder().lambdaFunctionConfigurations(lambdaConfig).build())
					.build();
			
			S3CLIENT.putBucketNotificationConfiguration(request);
			
			System.out.println("Trigger Created");
		}
	
	public void putItemInTable(DynamoDbClient ddc, String tableName, String bucketName, String fileName, String createdAt, String fileId, String status,int lineCount) {
		try {
			
			HashMap<String, AttributeValue> itemValues = new HashMap<>();
			
			itemValues.put("fileId", AttributeValue.builder().s(fileId).build());
			itemValues.put("bucketName", AttributeValue.builder().s(bucketName).build());
			itemValues.put("s3Key", AttributeValue.builder().s(fileName).build());
			itemValues.put("createdAt", AttributeValue.builder().s(createdAt).build());
			itemValues.put("status", AttributeValue.builder().s(status).build());
			itemValues.put("lineCount", AttributeValue.builder().n(String.valueOf(lineCount)));
			PutItemRequest request = PutItemRequest.builder()
					.item(itemValues)
					.tableName(tableName)
					.build();
			
			ddc.putItem(request);
			
			
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

	
}
