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

public class ElasticSearchUtil {
    private Logger _logger;
    private RestClient _restClient;
    private ElasticsearchTransport _transport;
    private ElasticsearchClient _elasticClient;
    private String _indexName = "news";

    // Конструктор класса
    ElasticSearchUtil(Logger logger) {
        _logger = logger;
        // Инициализация клиента Elasticsearch
        _restClient = RestClient
                .builder(HttpHost.create("http://localhost:9200"))
                .build();
        // Инициализация транспорта для Elasticsearch
        _transport = new RestClientTransport(_restClient, new JacksonJsonpMapper());
        // Создание клиента Elasticsearch
        _elasticClient = new ElasticsearchClient(_transport);
    }

    // Метод для индексации одного документа в Elasticsearch
    public void indexSingleDocument(String id, News doc) {
        try {
            IndexResponse response = _elasticClient.index(i -> i
                    .index(_indexName)
                    .id(id)
                    .document(doc));
            // Вывод информации об успешной индексации документа
            if (response.result().toString().equals("Created") || response.result().toString().equals("Updated")) {
                _logger.info("[Result: " + response.result() + "] Successfully saved document with id = " + id + " to database");
            }
        }
        catch (IOException e) {
            // Вывод ошибки при индексации документа
            _logger.error(e.getClass() + " was caught while indexing document with id = " + id + "to database: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    // Метод для чтения одного документа из Elasticsearch
    public News readSingleDocument(String id) {
        try {
            GetResponse<News> response = _elasticClient.get(g -> g
                    .index(_indexName)
                    .id(id), News.class);
            // Если документ найден, возвращается его содержимое
            if (response.found()) {
                News document = response.source();
                _logger.info("[Result: Found] Successfully got document with id = " + id + " from database");
                return document;
            }
            // Если документ не найден, возвращается пустой документ
            else {
                _logger.info("[Result: NotFound] Unable to find document with id = " + id);
                return new News("","","","","");
            }
        }
        catch (IOException e) {
            // Вывод ошибки при чтении документа
            _logger.error(e.getClass() + " was caught while getting document with id = " + id + "from database: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    // Метод для удаления одного документа из Elasticsearch
    public void deleteSingleDocument(String id) {
        try {
            DeleteResponse response = _elasticClient.delete(i -> i
                    .index(_indexName)
                    .id(id));
            // Вывод информации об успешном удалении документа
            if (response.result().toString().equals("Deleted")) {
                _logger.info("[Result: " + response.result() + "] Successfully deleted document from database");
            }
        } catch (IOException e) {
            // Вывод ошибки при удалении документа
            _logger.error(e.getClass() + " was caught while deleting document with id = " + id + "from database: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    // Метод для закрытия соединения с Elasticsearch
    public void close() {
        try {
            _transport.close();
            _restClient.close();
        }
        catch (IOException e) {
            // Вывод ошибки при закрытии соединения с Elasticsearch
            _logger.error(e.getClass() + " was caught while trying to close ElasticSearchClient: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

}
