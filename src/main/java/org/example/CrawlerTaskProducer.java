package org.example;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class CrawlerTaskProducer {

    private static final String TASK_QUEUE_NAME = "taskQueue";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("kotherine");
        factory.setPassword("12345");
        factory.setVirtualHost("/");
        factory.setHost("127.0.0.1");
        factory.setPort(5672);
        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();

        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);

        // Получение ссылок из RSS-ленты
        Document rssFeed = Jsoup.connect("https://sosenskoe-omsu.ru/news/feed/").get();
        Elements items = rssFeed.select("item");

        for (Element item : items) {
            String link = item.select("link").text();
            channel.basicPublish("", TASK_QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, link.getBytes());
        }

        channel.close();
        conn.close();
    }
}
