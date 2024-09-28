package com.msd.gin.halyard.sail.search;

import java.io.IOException;
import java.util.Arrays;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.GeoLocation;
import co.elastic.clients.elasticsearch.core.SearchResponse;

public final class SearchClient {
	public static final int DEFAULT_RESULT_SIZE = 10000;
	public static final double DEFAULT_MIN_SCORE = 0.0;
	public static final int DEFAULT_FUZZINESS = 1;
	public static final int DEFAULT_PHRASE_SLOP = 0;
	public static final int DEFAULT_K = 10;
	public static final int DEFAULT_NUM_CANDIDATES = 10000;

	private final ElasticsearchClient client;
	private final String index;

	public SearchClient(ElasticsearchClient client, String index) {
		this.client = client;
		this.index = index;
	}

	public SearchResponse<? extends SearchDocument> search(String query, int limit, double minScore, int fuzziness, int slop, boolean allFields) throws IOException {
		return client.search(
				s -> {
					s = s.index(index);
					if (!allFields) {
						s = s.source(src -> src.filter(f -> f.includes(SearchDocument.REQUIRED_FIELDS)));
					}
					return s.query(q -> q.queryString(qs -> qs.query(query).defaultField(SearchDocument.LABEL_FIELD).fuzziness(Integer.toString(fuzziness)).phraseSlop(Double.valueOf(slop)))).minScore(minScore).size(limit);
				},
				SearchDocument.class);
	}

	public SearchResponse<? extends SearchDocument> search(double lat, double lon, double dist, String units) throws IOException {
		return client.search(s -> s.index(index).source(src -> src.filter(f -> f.includes(SearchDocument.REQUIRED_FIELDS)))
				.query(q -> q.geoDistance(gd -> gd.field(SearchDocument.LABEL_POINT_FIELD).location(GeoLocation.of(gl -> gl.latlon(ll -> ll.lat(lat).lon(lon)))).distance(dist + units))),
				SearchDocument.class);
	}

	public SearchResponse<? extends SearchDocument> knn(Float[] vec, int k, int numCandidates, double minScore, boolean allFields) throws IOException {
		return client.search(
				s -> {
					s = s.index(index);
					if (!allFields) {
						s = s.source(src -> src.filter(f -> f.includes(SearchDocument.REQUIRED_FIELDS)));
					}
					return s.knn(q -> q.field(SearchDocument.VECTOR_FIELD).queryVector(Arrays.asList(vec)).k(k).numCandidates(numCandidates)).minScore(minScore);
				}, SearchDocument.class);
	}
}
