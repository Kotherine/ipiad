package org.example;

import co.elastic.clients.elasticsearch._types.aggregations.Aggregation;
import co.elastic.clients.elasticsearch.core.*;
import org.apache.logging.log4j.Logger;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.List;

/**
 * Утилитарный класс для взаимодействия с Elasticsearch.
 */
public class ElasticSearchUtil {
    // Экземпляр логгера для регистрации событий
    private Logger _logger;
    // REST-клиент для Elasticsearch
    private final RestClient _restClient;
    // Транспортный уровень для Elasticsearch
    private final ElasticsearchTransport _transport;
    // Клиент Elasticsearch
    private ElasticsearchClient _elasticClient;
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
     * Индексирует документ в Elasticsearch.
     *
     * @param id  ID документа
     * @param doc Документ для индексации
     */
    public void indexDocument(String id, News doc) {
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
     * Считывает документ из Elasticsearch по ID.
     *
     * @param id ID документа
     * @return Документ, если найден, в противном случае пустой объект News
     */
    public News readDocument(String id) {
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
     * Выполняет поиск документов в Elasticsearch.
     *
     * @param query1 Поисковый запрос 1
     * @param query2 Поисковый запрос 2
     * @param query3 Поисковый запрос 3
     * @return Список документов, соответствующих запросу
     * @throws IOException В случае ошибки ввода/вывода
     */
    public List<News> searchDocuments(String query1, String query2, String query3) throws IOException {
        // Создание объекта запроса поиска
        SearchRequest request = SearchRequest.of(s -> s
                // Указание индекса, в котором будет выполняться поиск
                .index("news")
                // Создание основного тела запроса с условиями
                .query(q -> q
                        // Использование логического запроса (bool query) для комбинирования условий
                        .bool(b -> b
                                // Обязательное условие (must) - аналог оператора AND
                                .must(m -> m.match(mm -> mm.field("title").query(query1)))
                                //.must(m -> m.term(mm -> mm.field("title").value(query1)))
                                // Необязательное условие (should) - аналог оператора OR
                                .should(s1 -> s1.match(sm -> sm.field("tags").query(query2)))
                                // Фильтр (filter) - не влияет на релевантность, но ограничивает результаты
                                .filter(f -> f.match(fm -> fm.field("date").query(query3)))
                        )
                )
        );

        // Выполнение запроса поиска и получение ответа
        SearchResponse<News> response = _elasticClient.search(request, News.class);

        // Преобразование результатов поиска в список объектов News
        return response.hits().hits().stream()
                .map(Hit::source)
                .collect(Collectors.toList());
    }

    /**
     * Выполняет агрегацию документов по датам.
     */
    public void termsAggregation(){
        try {
            Aggregation agg = Aggregation.of(a -> a.terms(t -> t
                            .field("date.keyword")
                    )
            );
            SearchResponse Aggregation = _elasticClient.search(s -> s
                            .index("news")
                            .aggregations("dates", agg),
                    News.class
            );
            prettyPrintJson(Aggregation.aggregations().get("dates").toString());
            _logger.debug(String.valueOf(Aggregation));
        } catch (IOException e) {
            _logger.error(e.getClass() + " was caught while indexing document with indexName = " + _indexName + " to database: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * Выводит JSON в удобном для чтения формате.
     *
     * @param jsonString JSON-строка для форматирования
     */
    public static void prettyPrintJson(String jsonString) {
        int indentLevel = 0;
        StringBuilder formattedJson = new StringBuilder();

        for (char c : jsonString.toCharArray()) {
            switch (c) {
                case '{':
                case '[':
                    formattedJson.append(c).append("\n").append(getIndentString(++indentLevel));
                    break;
                case '}':
                case ']':
                    formattedJson.append("\n").append(getIndentString(--indentLevel)).append(c);
                    break;
                case ',':
                    formattedJson.append(c).append("\n").append(getIndentString(indentLevel));
                    break;
                default:
                    formattedJson.append(c);
            }
        }
        System.out.println(formattedJson.toString());
    }

    /**
     * Возвращает строку с отступом для форматирования JSON.
     *
     * @param indentLevel Уровень отступа
     * @return Строка с отступом
     */
    private static String getIndentString(int indentLevel) {
        StringBuilder indentString = new StringBuilder();
        for (int i = 0; i < indentLevel; i++) {
            indentString.append("    "); // 4 пробела для каждого уровня отступа
        }
        return indentString.toString();
    }

    /**
     * Удаляет документ из Elasticsearch по ID.
     *
     * @param id ID документа
     */
    public void deleteDocument(String id) {
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
