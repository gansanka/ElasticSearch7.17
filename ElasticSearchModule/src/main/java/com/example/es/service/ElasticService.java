package com.example.es.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.IntStream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.example.es.data.Product;
import com.example.es.utility.ElasticRestClientUtility;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.TotalHits;
import co.elastic.clients.elasticsearch.core.search.TotalHitsRelation;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.elasticsearch.indices.DeleteIndexResponse;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.transport.endpoints.BooleanResponse;

@Service
public class ElasticService {

	private static final long MILLIS_IN_A_DAY = 1000 * 60 * 60 * 24;

	@Autowired
	ElasticRestClientUtility utility;

	public boolean createIndex(String index) throws ElasticsearchException, IOException {
		boolean isExists = doesIndexExist(index);
		CreateIndexResponse response = CreateIndexResponse
				.of(c -> c.index(index).shardsAcknowledged(false).acknowledged(false));
		if (!isExists) {
			response = utility.buildRestClient().indices().create(CreateIndexRequest.of(c -> c.index(index)));
			indexSampleData(index);
		}
		return response.acknowledged();
	}

	public boolean deleteIndex(String index) throws ElasticsearchException, IOException {
		boolean isExists = doesIndexExist(index);
		DeleteIndexResponse response = DeleteIndexResponse.of(d -> d.acknowledged(false));
		if (isExists) {
			response = utility.buildRestClient().indices()
					.delete(DeleteIndexRequest.of(f -> f.index(Arrays.asList(new String[] { index }))));
		}
		return response.acknowledged();
	}

	public boolean doesIndexExist(String index) throws ElasticsearchException, IOException {
		BooleanResponse response = utility.buildRestClient().indices()
				.exists(ExistsRequest.of(e -> e.index(Arrays.asList(new String[] { index }))));
		return response.value();
	}

	public Product getProductById(String id) throws ElasticsearchException, IOException {
		Product product = null;
		GetResponse<Product> response = utility.buildRestClient().get(g -> g.index("product").id(id), Product.class);

		if (response.found()) {
			product = response.source();
			System.out.println("Product name " + product.getName());
		} else {
			System.out.println("Product not found");
		}
		return product;
	}

	public List<Product> getBySimpleSearch(String searchText) throws ElasticsearchException, IOException {
		List<Product> products = new ArrayList<>();

		SearchResponse<Product> response = utility.buildRestClient().search(
				s -> s.index("product").query(q -> q.match(t -> t.field("name").query(searchText))), Product.class);

		TotalHits total = response.hits().total();
		boolean isExactResult = total.relation() == TotalHitsRelation.Eq;

		if (isExactResult) {
			System.out.println("There are " + total.value() + " results");
		} else {
			System.out.println("There are more than " + total.value() + " results");
		}

		List<Hit<Product>> hits = response.hits().hits();
		for (Hit<Product> hit : hits) {
			Product product = hit.source();
			System.out.println("Found product " + product.getName() + ", score " + hit.score());
			products.add(product);
		}

		return products;
	}

	public List<Product> getByWildcardSearch(String searchText) throws ElasticsearchException, IOException {
		List<Product> products = new ArrayList<>();

		SearchResponse<Product> response = utility.buildRestClient().search(s -> s.index("product")
				.query(q -> q.wildcard(t -> t.field("name").value(searchText))), Product.class);

		TotalHits total = response.hits().total();
		boolean isExactResult = total.relation() == TotalHitsRelation.Eq;

		if (isExactResult) {
			System.out.println("There are " + total.value() + " results");
		} else {
			System.out.println("There are more than " + total.value() + " results");
		}

		List<Hit<Product>> hits = response.hits().hits();
		for (Hit<Product> hit : hits) {
			Product product = hit.source();
			System.out.println("Found product " + product.getName() + ", score " + hit.score());
			products.add(product);
		}

		return products;
	}
	
	public List<Product> getByRangeSearch(String from, String to) throws ElasticsearchException, IOException {
		List<Product> products = new ArrayList<>();

		SearchResponse<Product> response = utility.buildRestClient().search(s -> s.index("product")
				.query(q -> q.range(t -> t.field("id").from(from).to(to))), Product.class);

		TotalHits total = response.hits().total();
		boolean isExactResult = total.relation() == TotalHitsRelation.Eq;

		if (isExactResult) {
			System.out.println("There are " + total.value() + " results");
		} else {
			System.out.println("There are more than " + total.value() + " results");
		}

		List<Hit<Product>> hits = response.hits().hits();
		for (Hit<Product> hit : hits) {
			Product product = hit.source();
			System.out.println("Found product " + product.getName() + ", score " + hit.score());
			products.add(product);
		}

		return products;
	}

	public void indexSampleData(String index) throws ElasticsearchException, IOException {
		List<Product> products = getProducts();
		BulkRequest.Builder br = new BulkRequest.Builder();
		products.stream().forEach(product -> {
			br.operations(op -> op.index(idx -> idx.index(index).id(product.getId().toString()).document(product)));
		});

		BulkResponse result = utility.buildRestClient().bulk(br.build());

		// Log errors, if any
		if (result.errors()) {
			System.out.println("Bulk had errors");
			for (BulkResponseItem item : result.items()) {
				if (item.error() != null) {
					System.out.println(item.error().reason());
				}
			}
		}
	}

	public List<Product> getProducts() {
		List<Product> products = new ArrayList<>();
		Date today = new Date();
		// SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		IntStream.rangeClosed(1, 10).forEach(i -> {
			Product product = new Product();
			product.setId(i);
			product.setName("Product" + i);
			product.setFrom(today);
			product.setTo(new Date(today.getTime() + (i * MILLIS_IN_A_DAY)));
			products.add(product);
			System.out.println("Added Product : " + product);
		});
		return products;
	}

}
