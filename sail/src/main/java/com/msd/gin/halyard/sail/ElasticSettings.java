/**
 * Copyright (c) 2016 Eclipse RDF4J contributors.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 */
package com.msd.gin.halyard.sail;

import com.msd.gin.halyard.common.SSLSettings;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.GeneralSecurityException;

import javax.net.ssl.SSLContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;

import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;

public final class ElasticSettings {
	String protocol;
	String host;
	int port;
	String username;
	String password;
	String indexName;
	SSLSettings sslSettings;

	public String getProtocol() {
		return protocol;
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	public String getIndexName() {
		return indexName;
	}

	public static ElasticSettings from(URL esIndexUrl) {
		if (esIndexUrl == null) {
			return null;
		}

		ElasticSettings settings = new ElasticSettings();
		settings.protocol = esIndexUrl.getProtocol();
		settings.host = esIndexUrl.getHost();
		settings.port = esIndexUrl.getPort();
		settings.indexName = esIndexUrl.getPath().substring(1);
		String userInfo = esIndexUrl.getUserInfo();
		if (userInfo != null) {
			String[] creds = userInfo.split(":", 1);
			settings.username = creds[0];
			if (creds.length > 1) {
				settings.password = creds[1];
			}
		}
		return settings;
	}

	public static ElasticSettings from(Configuration conf) {
		return merge(conf, null);
	}

	public static ElasticSettings from(String esIndexUrl, Configuration conf) {
		ElasticSettings merged;
		try {
			merged = from(new URL(esIndexUrl));
		} catch (MalformedURLException e) {
			throw new IllegalArgumentException(e);
		}
		if (merged.username == null) {
			merged.username = conf.get("es.net.http.auth.user");
		}
		if (merged.password == null) {
			merged.password = conf.get("es.net.http.auth.pass");
		}
		if ("https".equals(merged.protocol)) {
			merged.sslSettings = SSLSettings.from(conf);
		}
		return merged;
	}

	public static ElasticSettings merge(Configuration conf, ElasticSettings defaults) {
		ElasticSettings merged;
		String esIndexUrl = conf.get(HBaseSail.ELASTIC_INDEX_URL);
		if (esIndexUrl != null) {
			try {
				merged = from(new URL(esIndexUrl));
			} catch (MalformedURLException e) {
				throw new IllegalArgumentException(e);
			}
		} else if (defaults != null) {
			merged = new ElasticSettings();
			merged.protocol = defaults.protocol;
			merged.host = defaults.host;
			merged.port = defaults.port;
			merged.indexName = defaults.indexName;
		} else {
			return null;
		}
		if (merged.username == null) {
			merged.username = conf.get("es.net.http.auth.user", defaults != null ? defaults.username : null);
		}
		if (merged.password == null) {
			merged.password = conf.get("es.net.http.auth.pass", defaults != null ? defaults.password : null);
		}
		if ("https".equals(merged.protocol)) {
			merged.sslSettings = SSLSettings.merge(conf, defaults != null ? defaults.sslSettings : null);
		}
		return merged;
	}

	public ElasticsearchTransport createTransport() throws IOException, GeneralSecurityException {
		RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(host, port != -1 ? port : 9200, protocol));
		CredentialsProvider esCredentialsProvider;
		if (password != null) {
			esCredentialsProvider = new BasicCredentialsProvider();
			esCredentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
		} else {
			esCredentialsProvider = null;
		}
		SSLContext sslContext;
		if (sslSettings != null) {
			sslContext = sslSettings.createSSLContext();
		} else {
			sslContext = null;
		}
		restClientBuilder.setHttpClientConfigCallback(new HttpClientConfigCallback() {
			@Override
			public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
				if (esCredentialsProvider != null) {
					httpClientBuilder.setDefaultCredentialsProvider(esCredentialsProvider);
				}
				if (sslContext != null) {
					httpClientBuilder.setSSLContext(sslContext);
				}
				return httpClientBuilder;
			}
		});
		RestClient restClient = restClientBuilder.build();
		return new RestClientTransport(restClient, new JacksonJsonpMapper());
	}
}