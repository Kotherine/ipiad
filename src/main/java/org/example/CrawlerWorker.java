package org.example;

import com.rabbitmq.client.*;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.HashSet;

public class CrawlerWorker {

    private static final String TASK_QUEUE_NAME = "taskQueue";
    private static final String RESULT_QUEUE_NAME = "resultQueue";
    private static final HashSet<String> seenHashes = new HashSet<>();
    private final RestHighLevelClient elasticsearchClient;
    public CrawlerWorker(RestHighLevelClient elasticsearchClient) {
        this.elasticsearchClient = elasticsearchClient;
    }

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("kotherine");
        factory.setPassword("12345");
        factory.setVirtualHost("/");
        factory.setHost("127.0.0.1");
        factory.setPort(5672);
        Connection conn = factory.newConnection();
        Channel taskChannel = conn.createChannel();
        Channel resultChannel = conn.createChannel();

        taskChannel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
        resultChannel.queueDeclare(RESULT_QUEUE_NAME, true, false, false, null);

        RestHighLevelClient elasticsearchClient = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));

        CrawlerWorker crawlerWorker = new CrawlerWorker(elasticsearchClient);

        ExecutorService executor = Executors.newFixedThreadPool(10);

        Consumer consumer = new DefaultConsumer(taskChannel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                String[] parts = message.split(";");
                String hash = parts[0];
                String url = parts[1];
                String title = parts[2];

                if (seenHashes.contains(hash)) {
                    taskChannel.basicAck(envelope.getDeliveryTag(), false);
                    return;
                }
                seenHashes.add(hash);

                executor.submit(() -> {
                    try {
                        // Скачиваем и парсим страницу
                        Document doc = Jsoup.connect(url).get();
                        Element pubDate = doc.selectFirst("date_rt_news");
                        String publicationDate = "";
                        if (pubDate != null) {
                            publicationDate = pubDate.text();
                        } else {
                            Elements pubDates = doc.getElementsByClass("date_rt_news");
                            for (Element date : pubDates) {
                                publicationDate = date.text();
                            }
                        }

                        Element newsTagsDiv = doc.selectFirst("div.news_tags");
                        StringBuilder tagsList = new StringBuilder();
                        if (newsTagsDiv != null) {
                            Elements tags = newsTagsDiv.select("a");
                            for (Element tag : tags) {
                                if (tagsList.length() > 0) {
                                    tagsList.append(", ");
                                }
                                tagsList.append(tag.text());
                            }
                        }

                        String result = String.format("\nTitle: %s\nPublication Date: %s\nTags: %s\nURL: %s", title, publicationDate, tagsList.toString(), url);

                        // Отправляем результат в очередь результатов
                        resultChannel.basicPublish("", RESULT_QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, result.getBytes());

                        // Подтверждаем сообщение
                        taskChannel.basicAck(envelope.getDeliveryTag(), false);

                        // Сохранение документа в Elasticsearch
                        ElasticSearchUtil.saveDocument(elasticsearchClient, hash, title, publicationDate, result, url, "Unknown Author");
                    } catch (Exception e) {
                        e.printStackTrace();
                        // В случае ошибки откладываем задачу для повторной обработки
                        try {
                            taskChannel.basicNack(envelope.getDeliveryTag(), false, true);
                        } catch (IOException ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                });
            }
        };

        taskChannel.basicConsume(TASK_QUEUE_NAME, false, consumer);

        System.out.println("Worker is waiting for messages. Press Ctrl+C to exit.");
    }

}
