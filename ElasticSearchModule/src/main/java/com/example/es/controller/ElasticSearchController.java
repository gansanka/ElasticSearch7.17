package com.example.es.controller;

import java.io.IOException;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.example.es.data.Product;
import com.example.es.service.ElasticService;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;

@RestController
public class ElasticSearchController {

	@Autowired
	ElasticService service;

	@PostMapping("/api/v1/index")
	public ResponseEntity<String> createIndex(@RequestParam String index) throws ElasticsearchException, IOException {
		boolean doCreate = service.createIndex(index);
		ResponseEntity<String> response = null;
		if (doCreate) {
			response = new ResponseEntity<String>("Success", HttpStatus.OK);
		} else {
			response = new ResponseEntity<String>("Index is already available", HttpStatus.CONFLICT);
		}
		return response;
	}

	@GetMapping("/api/v1/product/{id}")
	public ResponseEntity<Product> getProductById(@PathVariable(name = "id") String id)
			throws ElasticsearchException, IOException {
		ResponseEntity<Product> response = null;
		Product product = service.getProductById(id);
		if (product == null) {
			response = new ResponseEntity<Product>(HttpStatus.NOT_FOUND);
		} else {
			response = new ResponseEntity<Product>(product, HttpStatus.OK);
		}
		return response;
	}

	// http://localhost:8080/api/v1/product/simplesearch?searchText=Product2
	@GetMapping("/api/v1/product/simplesearch")
	public ResponseEntity<List<Product>> getBySimpleSearch(@RequestParam(name = "searchText") String searchText)
			throws ElasticsearchException, IOException {
		ResponseEntity<List<Product>> response = null;
		List<Product> product = service.getBySimpleSearch(searchText);
		if (product == null || product.isEmpty()) {
			response = new ResponseEntity<List<Product>>(HttpStatus.NOT_FOUND);
		} else {
			response = new ResponseEntity<List<Product>>(product, HttpStatus.OK);
		}
		return response;
	}

	// http://localhost:8080/api/v1/product/wildcardsearch?searchText=product*
	// tokens are indexed in lowercase
	@GetMapping("/api/v1/product/wildcardsearch")
	public ResponseEntity<List<Product>> getByWildcardSearch(@RequestParam(name = "searchText") String searchText)
			throws ElasticsearchException, IOException {
		ResponseEntity<List<Product>> response = null;
		List<Product> product = service.getByWildcardSearch(searchText);
		if (product == null || product.isEmpty()) {
			response = new ResponseEntity<List<Product>>(HttpStatus.NOT_FOUND);
		} else {
			response = new ResponseEntity<List<Product>>(product, HttpStatus.OK);
		}
		return response;
	}

	@GetMapping("/api/v1/product/rangesearch")
	public ResponseEntity<List<Product>> getByRangeSearch(@RequestParam(name = "from") String from,
			@RequestParam(name = "to") String to) throws ElasticsearchException, IOException {
		ResponseEntity<List<Product>> response = null;
		List<Product> product = service.getByRangeSearch(from, to);
		if (product == null || product.isEmpty()) {
			response = new ResponseEntity<List<Product>>(HttpStatus.NOT_FOUND);
		} else {
			response = new ResponseEntity<List<Product>>(product, HttpStatus.OK);
		}
		return response;
	}

	@DeleteMapping("/api/v1/index")
	public ResponseEntity<String> deleteIndex(@RequestParam String index) throws ElasticsearchException, IOException {
		boolean doDelete = service.deleteIndex(index);
		ResponseEntity<String> response = null;
		if (doDelete) {
			response = new ResponseEntity<String>("Success", HttpStatus.OK);
		} else {
			response = new ResponseEntity<String>("Index is not available", HttpStatus.CONFLICT);
		}
		return response;
	}

}
