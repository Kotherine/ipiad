package org.example;

import org.apache.logging.log4j.Logger;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.IOException;

/**
 * Утилитарный класс для взаимодействия с Elasticsearch.
 */
public class ElasticSearchUtil {
    // Экземпляр логгера для регистрации событий
    private final Logger _logger;
    // REST-клиент для Elasticsearch
    private final RestClient _restClient;
    // Транспортный уровень для Elasticsearch
    private final ElasticsearchTransport _transport;
    // Клиент Elasticsearch
    private final ElasticsearchClient _elasticClient;
    // Имя индекса Elasticsearch
    private final String _indexName = "news";

    /**
     * Конструктор инициализирует клиент Elasticsearch с помощью указанного логгера.
     *
     * @param logger Экземпляр логгера для регистрации событий
     */
    public ElasticSearchUtil(Logger logger) {
        _logger = logger;
        _restClient = RestClient.builder(HttpHost.create("http://localhost:9200")).build();
        _transport = new RestClientTransport(_restClient, new JacksonJsonpMapper());
        _elasticClient = new ElasticsearchClient(_transport);
    }

    /**
     * Индексирует одиночный документ в Elasticsearch.
     *
     * @param id  ID документа
     * @param doc Документ для индексации
     */
    public void indexSingleDocument(String id, News doc) {
        try {
            IndexResponse response = _elasticClient.index(i -> i
                    .index(_indexName)
                    .id(id)
                    .document(doc));
            if (response.result().toString().equals("Created") || response.result().toString().equals("Updated")) {
                _logger.info("[Result: " + response.result() + "] Successfully saved document with id = " + id + " to database");
            }
        } catch (IOException e) {
            _logger.error(e.getClass() + " was caught while indexing document with id = " + id + " to database: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * Считывает одиночный документ из Elasticsearch по ID.
     *
     * @param id ID документа
     * @return Документ, если найден, в противном случае пустой объект News
     */
    public News readSingleDocument(String id) {
        try {
            GetResponse<News> response = _elasticClient.get(g -> g
                    .index(_indexName)
                    .id(id), News.class);
            if (response.found()) {
                News document = response.source();
                _logger.info("[Result: Found] Successfully got document with id = " + id + " from database");
                return document;
            } else {
                _logger.info("[Result: NotFound] Unable to find document with id = " + id);
                return new News("", "", "", "", "");
            }
        } catch (IOException e) {
            _logger.error(e.getClass() + " was caught while getting document with id = " + id + " from database: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * Удаляет одиночный документ из Elasticsearch по ID.
     *
     * @param id ID документа
     */
    public void deleteSingleDocument(String id) {
        try {
            DeleteResponse response = _elasticClient.delete(i -> i
                    .index(_indexName)
                    .id(id));
            if (response.result().toString().equals("Deleted")) {
                _logger.info("[Result: " + response.result() + "] Successfully deleted document from database");
            }
        } catch (IOException e) {
            _logger.error(e.getClass() + " was caught while deleting document with id = " + id +
                    " from database: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * Закрывает клиент Elasticsearch и связанные ресурсы.
     */
    public void close() {
        try {
            _transport.close();
            _restClient.close();
        } catch (IOException e) {
            _logger.error(e.getClass() + " was caught while trying to close ElasticSearchClient: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
