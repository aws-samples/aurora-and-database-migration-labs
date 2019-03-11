// Copyright <YEAR> Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.rdsdata.AWSRDSData;
import com.amazonaws.services.rdsdata.AWSRDSDataClientBuilder;
import com.amazonaws.services.rdsdata.model.ExecuteSqlRequest;
import com.amazonaws.services.rdsdata.model.ExecuteSqlResult;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CityVoteHandler implements RequestHandler<SNSEvent, String> {

    private final static String DATABASE_ARN = System.getenv("AURORA_ARN");

    private final static String SECRET_STORE_ARN =  System.getenv("SECRET_ARN");

    private final static String ENDPOINT = System.getenv("DATA_API_ENDPOINT");

    private final static String REGION = System.getenv("REGION_NAME");

    private AWSRDSData awsRDSDataAPI;

    @Override
    public String handleRequest(SNSEvent event, Context context) {
        // Helper objects
        SMSSender smsSender = new SMSSender();
        Validator validator = new Validator();

        awsRDSDataAPI = AWSRDSDataClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(ENDPOINT, REGION))
                .build();

        String messageBodyJson = event.getRecords().get(0).getSNS().getMessage();
        Map<String,String> messageMap = parseToMap(messageBodyJson);

        // Parse the city and phone number
        String originationNumber = messageMap.get("originationNumber");
        String city = messageMap.get("messageBody");

        // Validation
        city = validator.validateAndTitleCaseCity(city);

        // Store the city in the Serverless database using Data API
        storeCity(city);

        // Return number of people from DB using Data API
        int numOtherPeopleFromSameCity = getNumberOfPeopleFromCity(city).intValue() - 1;

        String message = String.format("Thank you for attending PSA Offsite 2019. " +
                "We’ve heard from %d others, who are also interested in having 2020 PSA Offsite in %s (so far)!" , numOtherPeopleFromSameCity, city);

        smsSender.sendMessage(originationNumber, message);

        return "Done";
    }

    public void storeCity(String city) {
        String insertSQL = String.format("INSERT INTO Demo.Cities VALUES(\'%s\')", city);

        ExecuteSqlRequest sqlRequest = createExecuteSqlRequest(insertSQL);
        awsRDSDataAPI.executeSql(sqlRequest);
    }

    public Long getNumberOfPeopleFromCity(String city) {
        String countSQL = String.format("Select count(*) from Demo.Cities where City = \'%s\' group by City;", city);

        ExecuteSqlRequest sqlRequest = createExecuteSqlRequest(countSQL);
        ExecuteSqlResult result = awsRDSDataAPI.executeSql(sqlRequest);

        // From the result, get result of first SQL, and first column from first Record
        return result.getSqlStatementResults().get(0).getResultFrame().getRecords().get(0).getValues().get(0).getBigIntValue();
    }

    private ExecuteSqlRequest createExecuteSqlRequest(String sql) {
        ExecuteSqlRequest request = new ExecuteSqlRequest();
        request.setAwsSecretStoreArn(SECRET_STORE_ARN);
        request.setDbClusterOrInstanceArn(DATABASE_ARN);
        request.setSqlStatements(sql);
        return request;
    }

    // Helper method to parse some JSON
    public Map<String, String> parseToMap(String messageBodyJson) {
        ObjectMapper objectMapper = new ObjectMapper();
        TypeReference<HashMap<String,String>> typeRef = new TypeReference<HashMap<String,String>>() {};
        Map<String,String> messageMap;

        try {
            messageMap = objectMapper.readValue(messageBodyJson, typeRef);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        return messageMap;
    }

}
