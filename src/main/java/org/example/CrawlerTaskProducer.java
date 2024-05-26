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

    // Основной метод, запускающий производителя задач
    public static void main(String[] args) throws Exception {
        // Настройка соединений RabbitMQ
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("kotherine");
        factory.setPassword("12345");
        factory.setVirtualHost("/");
        factory.setHost("127.0.0.1");
        factory.setPort(5672);

        // Создание соединения и канала RabbitMQ
        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();

        // Объявление очереди задач
        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);

        // Получение ссылок из RSS-ленты и отправка их в очередь задач
        Document rssFeed = Jsoup.connect("https://sosenskoe-omsu.ru/news/feed/").get();
        Elements items = rssFeed.select("item");

        for (Element item : items) {
            String link = item.select("link").text();
            String title = item.select("title").text();
            String hash = computeHash(link + title);

            String message = String.format("%s;%s;%s", hash, link, title);
            channel.basicPublish("", TASK_QUEUE_NAME, null, message.getBytes());
            System.out.println("Sent '" + message + "'");
        }

        // Закрытие канала и соединения RabbitMQ
        channel.close();
        conn.close();
    }

    // Метод для вычисления хэша MD5 для заданного текста
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
