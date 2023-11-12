package com.example.es.utility;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.springframework.stereotype.Component;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;

@Component
public class ElasticRestClientUtility {
	
	public ElasticsearchClient buildRestClient() {
		// Create the low-level client
		RestClient httpClient = RestClient.builder(
		    new HttpHost("localhost", 9200)
		).build();

		/*
		 * Deprecated usage
		 * 
		 * RestHighLevelClient hlrc = new RestHighLevelClientBuilder(httpClient)
		 * .setApiCompatibilityMode(true) .build();
		 */

		// Create the Java API Client with the same low level client
		ElasticsearchTransport transport = new RestClientTransport(
		    httpClient,
		    new JacksonJsonpMapper()
		);

		ElasticsearchClient esClient = new ElasticsearchClient(transport);
		return esClient;
	}


}
