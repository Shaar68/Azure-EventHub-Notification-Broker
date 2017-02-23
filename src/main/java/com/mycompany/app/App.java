package com.mycompany.app;

import java.io.IOException;

import com.google.gson.Gson;
import com.microsoft.azure.eventhubs.*;
import com.microsoft.azure.servicebus.*;
import com.windowsazure.messaging.*;
import org.apache.commons.codec.binary.Base64;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.time.*;
import java.util.function.*;


/**
 * A simple broker which listen to event from Event hub and send message to end device (subscriber of notification hub) as a publisher
 *
 * 1. Simple EventHub listener implementation following the tutorial of https://docs.microsoft.com/zh-tw/azure/iot-hub/iot-hub-java-java-getstarted
 * 2. Simple Azure notification hub following to https://github.com/Azure/azure-notificationhubs-samples/tree/master/notificationhubs-rest-java
 */

public class App {


    private static String connStr = "{connection endpoint string get from event hub}";

    public static void main(String[] args) throws IOException {

        System.out.println("Start listening to EventHub");
        EventHubClient client0 = receiveMessages("0");
        EventHubClient client1 = receiveMessages("1");
        EventHubClient client2 = receiveMessages("2");
        EventHubClient client3 = receiveMessages("3");
//        send_fake_notification();
        System.out.println("Press ENTER to exit.");
        System.in.read();
        try {
            client0.closeSync();
            client1.closeSync();
            client2.closeSync();
            client3.closeSync();
            System.exit(0);
        } catch (ServiceBusException sbe) {
            System.exit(1);
        }
    }

    @SuppressWarnings("Since15")
    private static EventHubClient receiveMessages(final String partitionId) {
        EventHubClient client = null;
        try {
            client = EventHubClient.createFromConnectionStringSync(connStr);
        } catch (Exception e) {
            System.out.println("Failed to create client: " + e.getMessage());
            System.exit(1);
        }
        try {
            client.createReceiver(
                    EventHubClient.DEFAULT_CONSUMER_GROUP_NAME,
                    partitionId,
                    Instant.now()).thenAccept(new Consumer<PartitionReceiver>() {
                public void accept(PartitionReceiver receiver) {
                    System.out.println("** Created receiver on partition " + partitionId);
                    try {
                        while (true) {
                            Iterable<EventData> receivedEvents = receiver.receive(100).get();
                            int batchSize = 0;
                            if (receivedEvents != null) {
                                for (EventData receivedEvent : receivedEvents) {
                                    System.out.println(String.format("Offset: %s, SeqNo: %s, EnqueueTime: %s",
                                            receivedEvent.getSystemProperties().getOffset(),
                                            receivedEvent.getSystemProperties().getSequenceNumber(),
                                            receivedEvent.getSystemProperties().getEnqueuedTime()));
                                    String rawString = new String(receivedEvent.getBody());
                                    System.out.println(String.format("| Device ID: %s", receivedEvent.getSystemProperties().get("iothub-connection-device-id")));
                                    System.out.println(String.format("| Message Payload: %s", rawString,
                                            Charset.defaultCharset()));
                                    batchSize++;
                                    send_notification(rawString);
                                }
                            }
                            System.out.println(String.format("Partition: %s, ReceivedBatch Size: %s", partitionId, batchSize));
                        }
                    } catch (Exception e) {
                        System.out.println("Failed to receive messages: " + e.getMessage());
                    }
                }
            });
        } catch (Exception e) {
            System.out.println("Failed to create receiver: " + e.getMessage());
        }
        return client;
    }


    public static void send_notification(String json) {
        String format = "{ \"data\": { \"message\": %s }}";
        NotificationHub hub = new NotificationHub("Endpoint=sb://unahackpush.servicebus.windows.net/;SharedAccessKeyName=DefaultFullSharedAccessSignature;SharedAccessKey=+hpOxMAjP0m5FOswGoUTztpakEJ1HrV/K0xgKRYJuCc=", "pushandroid");

        String newString = String.format(format, json);
        System.out.println("send msg to notification service:" + newString);
        Notification n = Notification.createGcmNotifiation(newString);

        // broadcast
        try {
            hub.sendNotification(n);
        } catch (NotificationHubsException e) {
            e.printStackTrace();
        }
    }

    public static void send_fake_notification() {
        String fake_json = "{\"device\" : \"2D22F6\",\"data\" : \"00000001\",\"time\" : 1487417758,\"duplicate\" : false,\"snr\" : 15.70,\"station\" : \"577B\",\"avgSignal\" : 15.36,\"lat\" : 25.0,\"lng\" : 122.0,\"rssi\" : -53.00,\"seqNumber\" : 354}";
        String format = "{ \"data\": { \"message\": %s }}";

        NotificationHub hub = new NotificationHub("Endpoint=sb://unahackpush.servicebus.windows.net/;SharedAccessKeyName=DefaultFullSharedAccessSignature;SharedAccessKey=+hpOxMAjP0m5FOswGoUTztpakEJ1HrV/K0xgKRYJuCc=", "pushandroid");
        String newString = String.format(format, fake_json);
        System.out.println("send msg to notification service:" + newString);
        Notification n = Notification.createGcmNotifiation(newString);

        // broadcast
        try {
            hub.sendNotification(n);
        } catch (NotificationHubsException e) {
            e.printStackTrace();
        }

    }
}
