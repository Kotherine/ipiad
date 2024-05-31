package org.example;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class CrawlerTaskProducer {

    private static final String TASK_QUEUE_NAME = "taskQueue";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("Kotherina");
        factory.setPassword("123");
        factory.setVirtualHost("/");
        factory.setHost("127.0.0.1");
        factory.setPort(5672);

        try (Connection conn = factory.newConnection(); Channel channel = conn.createChannel()) {
            channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);

            Document rssFeed = Jsoup.connect("https://sosenskoe-omsu.ru/news/feed/").userAgent("\tMozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0").get();
            Elements items = rssFeed.select("item");

            for (Element item : items) {
                String link = item.select("link").text();
                String title = item.select("title").text();
                String hash = computeHash(link + title);

                String message = String.format("%s;%s;%s", hash, link, title);
                channel.basicPublish("", TASK_QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
                System.out.println("Sent '" + message + "'");
            }
        }
    }

    private static String computeHash(String text) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] array = md.digest(text.getBytes());
            StringBuilder sb = new StringBuilder();
            for (byte b : array) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
