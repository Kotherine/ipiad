package org.example;

import com.rabbitmq.client.*;

public class MessageProduser {

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("kotherine");
        factory.setPassword("12345");
        factory.setVirtualHost("/");
        factory.setHost("127.0.0.1");
        factory.setPort(5672);
        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();
        String exchangeName = "myExchange";
        String routingKey = "testRoute";

        boolean durable = true;
        channel.exchangeDeclare(exchangeName, "direct", durable);

        for (int i=1; i<21; i++) {
            byte[] messageBodyBytes = ("test "+i).getBytes();
            channel.basicPublish(exchangeName, routingKey, MessageProperties.PERSISTENT_TEXT_PLAIN, messageBodyBytes);
        }
        channel.close();
        conn.close();
    }

}
