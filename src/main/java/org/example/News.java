package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

public class News {
    private String title;
    private String date;
    private String tags;
    private String URL;
    private String hash;

    // Конструктор для создания объекта News
    public News(String title, String date, String tags, String URL, String hash) {
        this.title = title;
        this.date = date;
        this.tags = tags;
        this.URL = URL;
        this.hash = hash;
    }

    // Пустой конструктор для использования при необходимости
    public News(){}

    // Геттеры и сеттеры для полей класса News
    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public String getURL() {
        return URL;
    }

    public void setURL(String URL) {
        this.URL = URL;
    }

    public String getHash() {
        return hash;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }

    // Метод для создания объекта News из строки JSON
    public void objectFromStrJson(String jsonData) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(jsonData);
        this.title = node.get("title").asText();
        this.date = node.get("date").asText();
        this.tags = node.get("tags").asText();
        this.URL = node.get("url").asText();
        this.hash = node.get("hash").asText();
    }

    // Метод для преобразования объекта News в строку JSON
    public String toJsonString() throws JsonProcessingException {
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        return ow.writeValueAsString(this);
    }

    // Переопределение метода toString() для вывода объекта News в виде строки
    @Override
    public String toString() {
        return "News{" +
                " title='" + title + '\'' +
                ", date='" + date + '\'' +
                ", tags='" + tags + '\'' +
                ", URL='" + URL + '\'' +
                ", hash='" + hash + '\'' +
                '}';
    }
}