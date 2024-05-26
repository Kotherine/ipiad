package org.example;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;

public class ElasticSearchUtil {

    private static final String INDEX_NAME = "documents";

    public static void createIndex(RestHighLevelClient client) throws IOException {


        CreateIndexRequest request = new CreateIndexRequest(INDEX_NAME);
        request.settings(Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 2)
        );

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("properties");
            {
                builder.startObject("title").field("type", "text").endObject();
                builder.startObject("publicationDate").field("type", "date").endObject();
                builder.startObject("text").field("type", "text").endObject();
                builder.startObject("url").field("type", "keyword").endObject();
                builder.startObject("author").field("type", "text").endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        request.mapping(builder.toString());

        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
    }
    public static void saveDocument(RestHighLevelClient client, String id, String title, String publicationDate, String text, String url, String author) throws IOException {
        IndexRequest request = new IndexRequest(INDEX_NAME);
        request.id(id);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("title", title);
            builder.field("publicationDate", publicationDate);
            builder.field("text", text);
            builder.field("url", url);
            builder.field("author", author);
        }
        builder.endObject();
        request.source(builder);

        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
    }

    public static boolean documentExists(RestHighLevelClient client, String id) throws IOException {
        GetRequest getRequest = new GetRequest(INDEX_NAME, id);
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        return getResponse.isExists();
    }
}

