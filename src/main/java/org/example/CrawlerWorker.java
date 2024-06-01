package org.example;

import com.rabbitmq.client.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class CrawlerWorker {

    private static final String TASK_QUEUE_NAME = "taskQueue";
    private static final String RESULT_QUEUE_NAME = "resultQueue";
    private static final HashSet<String> seenHashes = new HashSet<>();
    private static final Logger logger = LogManager.getLogger(CrawlerWorker.class);

    public static void main(String[] args) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("Kotherina");
        factory.setPassword("123");
        factory.setVirtualHost("/");
        factory.setHost("127.0.0.1");
        factory.setPort(5672);

        ExecutorService executor = Executors.newFixedThreadPool(10);
        ElasticSearchUtil dbConnector = null;
        Connection conn = null;
        Channel taskChannel = null;
        Channel resultChannel = null;

        try {
            conn = factory.newConnection();
            taskChannel = conn.createChannel();
            resultChannel = conn.createChannel();

            taskChannel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
            resultChannel.queueDeclare(RESULT_QUEUE_NAME, true, false, false, null);

            dbConnector = new ElasticSearchUtil(logger);

            while (true) {
                GetResponse response = taskChannel.basicGet(TASK_QUEUE_NAME, false);
                if (response == null) {
                    Thread.sleep(1000);
                    continue;
                }

                String message = new String(response.getBody(), StandardCharsets.UTF_8);
                String[] parts = message.split(";");
                String hash = parts[0];
                String url = parts[1];
                String title = parts[2];

                if (seenHashes.contains(hash)) {
                    taskChannel.basicAck(response.getEnvelope().getDeliveryTag(), false);
                    continue;
                }
                seenHashes.add(hash);

                Channel finalResultChannel = resultChannel;
                ElasticSearchUtil finalDbConnector = dbConnector;
                Channel finalTaskChannel = taskChannel;
                executor.submit(() -> {
                    try {
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

                        News news = new News(title, publicationDate, tagsList.toString(), url, hash);
                        finalDbConnector.indexSingleDocument(hash, news);

                        String result = String.format("%s;%s;%s;%s;%s", hash, url, title, publicationDate, tagsList.toString());
                        finalResultChannel.basicPublish("", RESULT_QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, result.getBytes(StandardCharsets.UTF_8));
                        logger.info("Published result: " + result);
                    } catch (IOException e) {
                        logger.error("Error processing message: " + e.getMessage(), e);
                    } finally {
                        try {
                            finalTaskChannel.basicAck(response.getEnvelope().getDeliveryTag(), false);
                        } catch (IOException e) {
                            logger.error("Error acknowledging message: " + e.getMessage(), e);
                        }
                    }
                });
            }
        } catch (Exception e) {
            logger.error("Error in worker: " + e.getMessage(), e);
        } finally {
            // Closing resources in a finally block to ensure they are closed on exit
            executor.shutdown();
            try {
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                    if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                        logger.error("Executor did not terminate");
                    }
                }
            } catch (InterruptedException ie) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }

            if (dbConnector != null) {
                dbConnector.close();
            }
            try {
                if (taskChannel != null) {
                    taskChannel.close();
                }
                if (resultChannel != null) {
                    resultChannel.close();
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
