package com.kang.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author kang
 * @version 1.0
 * @date 2020/4/17 14:48
 */
@Configuration
public class ElasticSearchConfig {

    @Bean
    public RestHighLevelClient restHighLevelClient() {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("139.186.162.76", 9200, "http"),
                        new HttpHost("139.186.162.76", 9201, "http"),
                        new HttpHost("139.186.162.76", 9202, "http"),
                        new HttpHost("139.186.148.75",9200,"http")));
        return client;
    }
}
