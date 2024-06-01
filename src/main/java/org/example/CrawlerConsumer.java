package org.example;

import com.rabbitmq.client.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class CrawlerConsumer {

    private static final String RESULT_QUEUE_NAME = "resultQueue";
    private static final Logger logger = LogManager.getLogger(CrawlerConsumer.class);

    public static void main(String[] args) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("Kotherina");
        factory.setPassword("123");
        factory.setVirtualHost("/");
        factory.setHost("127.0.0.1");
        factory.setPort(5672);

        Connection conn = null;
        Channel channel = null;
        ElasticSearchUtil dbConnector = null;

        try {
            conn = factory.newConnection();
            channel = conn.createChannel();

            channel.queueDeclare(RESULT_QUEUE_NAME, true, false, false, null);

            dbConnector = new ElasticSearchUtil(logger);

            while (true) {
                GetResponse response = channel.basicGet(RESULT_QUEUE_NAME, false);
                if (response == null) {
                    Thread.sleep(1000);
                    continue;
                }

                String message = new String(response.getBody(), StandardCharsets.UTF_8);
                String[] parts = message.split(";", -1);
                String hash = parts[0];
                String url = parts[1];
                String title = parts[2];
                String publicationDate = parts[3];
                String category = parts[4];

                System.out.println("News: " + String.format("\nTitle: %s\nPublication Date: %s\nCategory: %s\nURL: %s", title, publicationDate, category, url));

                News existingNews = dbConnector.readSingleDocument(hash);
                if (existingNews == null || existingNews.getTitle().isEmpty()) {
                    News news = new News(title, publicationDate, category, url, hash);
                    dbConnector.indexSingleDocument(hash, news);
                }

                channel.basicAck(response.getEnvelope().getDeliveryTag(), false);
            }
        } catch (Exception e) {
            logger.error("Error in consumer: " + e.getMessage(), e);
        } finally {
            try {
                if (dbConnector != null) {
                    dbConnector.close();
                }
                if (channel != null) {
                    channel.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (IOException | TimeoutException e) {
                logger.error("Error closing resources: " + e.getMessage(), e);
            }
        }
    }
}
