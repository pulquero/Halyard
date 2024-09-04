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
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.conn.DefaultSchemePortResolver;
import org.apache.http.impl.conn.SystemDefaultDnsResolver;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.conn.ManagedNHttpClientConnectionFactory;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.conn.NoopIOSessionStrategy;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.client.RestClientBuilder.RequestConfigCallback;
import org.elasticsearch.client.sniff.ElasticsearchNodesSniffer;
import org.elasticsearch.client.sniff.NodesSniffer;
import org.elasticsearch.client.sniff.Sniffer;

import co.elastic.clients.json.jackson.JacksonJsonpMapper;

public final class ElasticSettings {
	private static final String USER = "es.net.http.auth.user";
	private static final String PASS = "es.net.http.auth.pass";
	private static final String IS_WAN_ONLY = "es.nodes.wan.only";
	/**
	 * Property defining optional ElasticSearch index URL
	 */
	public static final String ELASTIC_INDEX_URL = "halyard.elastic.index.url";
	private static final String MAX_CONNECTIONS_PER_ROUTE = "halyard.elastic.connections.maxPerRoute";
	private static final String MAX_CONNECTIONS_TOTAL = "halyard.elastic.connections.maxTotal";
	private static final String CONNECTION_REQUEST_TIMEOUT_MILLIS = "halyard.elastic.connections.requestTimeoutMillis";
	private static final String IO_THREADS = "halyard.elastic.ioThreads";

	private static final int DEFAULT_CONNECTION_REQUEST_TIMEOUT = -1; // -1 system default, 0 infinite
	private static final int DEFAULT_MAX_CONNECTIONS_TOTAL = RestClientBuilder.DEFAULT_MAX_CONN_TOTAL;
	private static final int DEFAULT_MAX_CONNECTIONS_PER_ROUTE = RestClientBuilder.DEFAULT_MAX_CONN_PER_ROUTE;
	private static final int DEFAULT_MAX_IO_THREADS = IOReactorConfig.Builder.getDefaultMaxIoThreadCount();

	String protocol;
	String host;
	int port;
	String username;
	String password;
	String indexName;
	SSLSettings sslSettings;
	boolean isWanOnly;
	int connRequestTimeoutMillis = DEFAULT_CONNECTION_REQUEST_TIMEOUT;
	int maxConnTotal = DEFAULT_MAX_CONNECTIONS_TOTAL;
	int maxConnPerRoute = DEFAULT_MAX_CONNECTIONS_PER_ROUTE;
	int ioThreads = DEFAULT_MAX_IO_THREADS;

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

	public int getMaxConnectionsPerRoute() {
		return maxConnPerRoute;
	}

	public int getMaxTotalConnections() {
		return maxConnTotal;
	}

	public int getConnectionRequestTimeoutMillis() {
		return connRequestTimeoutMillis;
	}

	public int getIoThreadCount() {
		return ioThreads;
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
			merged = from(new URI(esIndexUrl).toURL());
		} catch (URISyntaxException | MalformedURLException e) {
			throw new IllegalArgumentException(e);
		}
		if (merged.username == null) {
			merged.username = conf.get(USER);
		}
		if (merged.password == null) {
			merged.password = conf.get(PASS);
		}
		if ("https".equals(merged.protocol)) {
			merged.sslSettings = SSLSettings.from(conf);
		}
		setAdditionalSettings(merged, conf, null);
		return merged;
	}

	public static ElasticSettings merge(Configuration conf, ElasticSettings defaults) {
		ElasticSettings urlSettings;
		String esIndexUrl = conf.get(ELASTIC_INDEX_URL);
		if (esIndexUrl != null) {
			try {
				urlSettings = from(new URI(esIndexUrl).toURL());
			} catch (URISyntaxException | MalformedURLException e) {
				throw new IllegalArgumentException(e);
			}
		} else if (defaults != null) {
			urlSettings = new ElasticSettings();
			urlSettings.protocol = defaults.protocol;
			urlSettings.host = defaults.host;
			urlSettings.port = defaults.port;
			urlSettings.indexName = defaults.indexName;
		} else {
			return null;
		}
		if (defaults != null) {
			if (urlSettings.username == null) {
				urlSettings.username = defaults.username;
			}
			if (urlSettings.password == null) {
				urlSettings.password = defaults.password;
			}
		}

		ElasticSettings merged = urlSettings;
		merged.username = conf.get(USER, urlSettings.username);
		merged.password = conf.get(PASS, urlSettings.password);
		if ("https".equals(merged.protocol)) {
			merged.sslSettings = SSLSettings.merge(conf, defaults != null ? defaults.sslSettings : null);
		}
		setAdditionalSettings(merged, conf, defaults);
		return merged;
	}

	private static void setAdditionalSettings(ElasticSettings settings, Configuration conf, ElasticSettings defaults) {
		settings.isWanOnly = conf.getBoolean(IS_WAN_ONLY, defaults != null ? defaults.isWanOnly : false);
		settings.ioThreads = conf.getInt(IO_THREADS, defaults != null ? defaults.ioThreads : DEFAULT_MAX_IO_THREADS);
		settings.maxConnTotal = conf.getInt(MAX_CONNECTIONS_TOTAL, defaults != null ? defaults.maxConnTotal : DEFAULT_MAX_CONNECTIONS_TOTAL);
		settings.maxConnPerRoute = conf.getInt(MAX_CONNECTIONS_PER_ROUTE, defaults != null ? defaults.maxConnPerRoute : DEFAULT_MAX_CONNECTIONS_PER_ROUTE);
		settings.connRequestTimeoutMillis = conf.getInt(CONNECTION_REQUEST_TIMEOUT_MILLIS, defaults != null ? defaults.connRequestTimeoutMillis : DEFAULT_CONNECTION_REQUEST_TIMEOUT);
	}

	public RestClientTransportWithSniffer createTransport() throws IOException, GeneralSecurityException {
		CredentialsProvider esCredentialsProvider = new BasicCredentialsProvider();
		if (password != null) {
			esCredentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
		}
		SSLContext sslContext = (sslSettings != null) ? sslSettings.createSSLContext() : SSLContext.getDefault();

		IOReactorConfig ioReactorConfig = IOReactorConfig.custom().setIoThreadCount(ioThreads).build();
		long connTimeToLive = -1;
		PoolingNHttpClientConnectionManager connManager = new PoolingNHttpClientConnectionManager(
				new DefaultConnectingIOReactor(ioReactorConfig, null),
				ManagedNHttpClientConnectionFactory.INSTANCE,
				RegistryBuilder.<SchemeIOSessionStrategy>create().register("http", NoopIOSessionStrategy.INSTANCE).register("https", new SSLIOSessionStrategy(sslContext)).build(), DefaultSchemePortResolver.INSTANCE,
				SystemDefaultDnsResolver.INSTANCE,
				connTimeToLive, TimeUnit.MILLISECONDS);
		connManager.setMaxTotal(maxConnTotal);
		connManager.setDefaultMaxPerRoute(maxConnPerRoute);

		RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(host, port != -1 ? port : 9200, protocol));
		restClientBuilder.setRequestConfigCallback(new RequestConfigCallback() {
			@Override
			public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
				requestConfigBuilder.setConnectionRequestTimeout(connRequestTimeoutMillis);
				return requestConfigBuilder;
			}
		});
		restClientBuilder.setHttpClientConfigCallback(new HttpClientConfigCallback() {
			@Override
			public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
				httpClientBuilder.setConnectionManager(connManager);
				httpClientBuilder.setDefaultCredentialsProvider(esCredentialsProvider);
				return httpClientBuilder;
			}
		});
		RestClient restClient = restClientBuilder.build();

		Sniffer sniffer;
		if (isWanOnly) {
			sniffer = null;
		} else {
			ElasticsearchNodesSniffer.Scheme snifferScheme;
			switch (protocol) {
				case "http":
					snifferScheme = ElasticsearchNodesSniffer.Scheme.HTTP;
					break;
				case "https":
					snifferScheme = ElasticsearchNodesSniffer.Scheme.HTTPS;
					break;
				default:
					throw new AssertionError("Unsupported scheme: " + protocol);
			}
			NodesSniffer nodesSniffer = new ElasticsearchNodesSniffer(restClient, ElasticsearchNodesSniffer.DEFAULT_SNIFF_REQUEST_TIMEOUT, snifferScheme);
			sniffer = Sniffer.builder(restClient).setNodesSniffer(nodesSniffer).build();
		}
		return new RestClientTransportWithSniffer(restClient, new JacksonJsonpMapper(), connManager, sniffer);
	}
}