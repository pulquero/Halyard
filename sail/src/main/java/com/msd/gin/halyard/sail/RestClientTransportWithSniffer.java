package com.msd.gin.halyard.sail;

import java.io.IOException;

import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.sniff.Sniffer;

import co.elastic.clients.json.JsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;

final class RestClientTransportWithSniffer extends RestClientTransport {
	private final PoolingNHttpClientConnectionManager connManager;
	private final Sniffer sniffer;

	RestClientTransportWithSniffer(RestClient restClient, JsonpMapper mapper, PoolingNHttpClientConnectionManager connManager, Sniffer sniffer) {
		super(restClient, mapper);
		this.connManager = connManager;
		this.sniffer = sniffer;
	}

	PoolingNHttpClientConnectionManager connectionManager() {
		return connManager;
	}

	@Override
	public void close() throws IOException {
		try {
			sniffer.close();
		} finally {
			super.close();
		}
	}
}