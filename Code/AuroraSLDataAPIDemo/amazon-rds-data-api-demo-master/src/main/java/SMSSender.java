// Copyright <YEAR> Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

import com.amazonaws.services.pinpoint.AmazonPinpoint;
import com.amazonaws.services.pinpoint.AmazonPinpointClient;
import com.amazonaws.services.pinpoint.model.AddressConfiguration;
import com.amazonaws.services.pinpoint.model.ChannelType;
import com.amazonaws.services.pinpoint.model.DirectMessageConfiguration;
import com.amazonaws.services.pinpoint.model.MessageRequest;
import com.amazonaws.services.pinpoint.model.MessageType;
import com.amazonaws.services.pinpoint.model.SMSMessage;
import com.amazonaws.services.pinpoint.model.SendMessagesRequest;

import java.util.HashMap;
import java.util.Map;

/**
 * Helper class to send SMS
 */
public class SMSSender {

    private final static String LONG_CODE = System.getenv("LONG_CODE");
    private final static String APPLICATION_ID = System.getenv("PINPOINT_PROJECT_ID");
    private final static String REGION = System.getenv("REGION_NAME");


    public void sendMessage(String number, String messageText) {
        AmazonPinpoint amazonPinpoint = AmazonPinpointClient.builder().withRegion(REGION).build();

        SMSMessage message = new SMSMessage();
        message.setOriginationNumber(LONG_CODE);
        message.setMessageType(MessageType.TRANSACTIONAL);
        message.setBody(messageText);
        message.setSenderId(LONG_CODE);

        DirectMessageConfiguration directMessageConfiguration = new DirectMessageConfiguration();
        directMessageConfiguration.setSMSMessage(message);

        Map<String, AddressConfiguration> map = new HashMap<>();
        map.put(number, new AddressConfiguration().withChannelType(ChannelType.SMS));

        MessageRequest messageRequest = new MessageRequest();
        messageRequest.setAddresses(map);
        messageRequest.setMessageConfiguration(directMessageConfiguration);

        SendMessagesRequest request = new SendMessagesRequest();
        request.setMessageRequest(messageRequest);
        request.setApplicationId(APPLICATION_ID);

        amazonPinpoint.sendMessages(request);
    }
}
