/*
 * Copyright © 2014 Merck Sharp & Dohme Corp., a subsidiary of Merck & Co., Inc.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.msd.gin.halyard.model.vocabulary;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.kohsuke.MetaInfServices;

/**
 * IRI constants used by Halyard.
 * @author Adam Sotona (MSD)
 */
@MetaInfServices(Vocabulary.class)
public final class HALYARD implements Vocabulary {

    private static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();

    public static final String PREFIX = "halyard";

    public static final String NAMESPACE = "http://merck.github.io/Halyard/ns#";

    public static final Namespace NS = new SimpleNamespace(PREFIX, NAMESPACE);

    public static final IRI STATS_ROOT_NODE = SVF.createIRI(NAMESPACE, "statsRoot");

    public static final IRI STATS_GRAPH_CONTEXT = SVF.createIRI(NAMESPACE, "statsContext");

    public static final IRI SYSTEM_GRAPH_CONTEXT = SVF.createIRI(NAMESPACE, "system");

    public static final IRI TRIPLE_GRAPH_CONTEXT = SVF.createIRI(NAMESPACE, "triples");

    public static final IRI FUNCTION_GRAPH_CONTEXT = SVF.createIRI(NAMESPACE, "functions");

    public static final IRI NAMESPACE_PREFIX_PROPERTY = SVF.createIRI(NAMESPACE, "namespacePrefix");

    public final static IRI TABLE_NAME_PROPERTY = SVF.createIRI(NAMESPACE, "tableName");

    public final static IRI SPLITBITS_PROPERTY = SVF.createIRI(NAMESPACE, "splitBits");

    public final static IRI CREATE_TABLE_PROPERTY = SVF.createIRI(NAMESPACE, "createTable");

    public final static IRI SNAPSHOT_NAME_PROPERTY = SVF.createIRI(NAMESPACE, "snapshotName");

    public final static IRI SNAPSHOT_RESTORE_PATH_PROPERTY = SVF.createIRI(NAMESPACE, "snapshotRestorePath");

    public final static IRI PUSH_STRATEGY_PROPERTY = SVF.createIRI(NAMESPACE, "pushStrategy");

    public final static IRI EVALUATION_TIMEOUT_PROPERTY = SVF.createIRI(NAMESPACE, "evaluationTimeout");
    public final static IRI TRACK_RESULT_SIZE_PROPERTY = SVF.createIRI(NAMESPACE, "trackResultSize");
    public final static IRI TRACK_RESULT_TIME_PROPERTY = SVF.createIRI(NAMESPACE, "trackResultTime");

    public final static IRI ELASTIC_INDEX_URL_PROPERTY = SVF.createIRI(NAMESPACE, "elasticIndexURL");

    public final static IRI ELASTIC_USERNAME_PROPERTY = SVF.createIRI(NAMESPACE, "elasticUsername");
    public final static IRI ELASTIC_PASSWORD_PROPERTY = SVF.createIRI(NAMESPACE, "elasticPassword");
    public final static IRI ELASTIC_KEYSTORE_LOCATION_PROPERTY = SVF.createIRI(NAMESPACE, "elasticKeystoreLocation");
    public final static IRI ELASTIC_KEYSTORE_PASSWORD_PROPERTY = SVF.createIRI(NAMESPACE, "elasticKeystorePassword");
    public final static IRI ELASTIC_TRUSTSTORE_LOCATION_PROPERTY = SVF.createIRI(NAMESPACE, "elasticTruststoreLocation");
    public final static IRI ELASTIC_TRUSTSTORE_PASSWORD_PROPERTY = SVF.createIRI(NAMESPACE, "elasticTruststorePassword");

    public final static IRI ENDPOINTS_GRAPH_CONTEXT = SVF.createIRI(NAMESPACE, "endpoints");

    public final static IRI SEARCH = SVF.createIRI(NAMESPACE, "search");

    public final static IRI DISTANCE = SVF.createIRI(NAMESPACE, "Distance");
    public final static IRI WITHIN_DISTANCE = SVF.createIRI(NAMESPACE, "withinDistance");

    public final static IRI QUERY_CLASS = SVF.createIRI(NAMESPACE, "Query");
    public final static IRI QUERY_PROPERTY = SVF.createIRI(NAMESPACE, "query");
    public final static IRI LIMIT_PROPERTY = SVF.createIRI(NAMESPACE, "limit");
    public final static IRI MIN_SCORE_PROPERTY = SVF.createIRI(NAMESPACE, "minScore");
    public final static IRI FUZZINESS_PROPERTY = SVF.createIRI(NAMESPACE, "fuzziness");
    public final static IRI PHRASE_SLOP_PROPERTY = SVF.createIRI(NAMESPACE, "phraseSlop");
    public final static IRI MATCHES_PROPERTY = SVF.createIRI(NAMESPACE, "matches");
    public final static IRI SCORE_PROPERTY = SVF.createIRI(NAMESPACE, "score");
    public final static IRI INDEX_PROPERTY = SVF.createIRI(NAMESPACE, "index");
    public final static IRI FIELD_PROPERTY = SVF.createIRI(NAMESPACE, "field");

    public final static IRI SEARCH_FIELD_FUNCTION = SVF.createIRI(NAMESPACE, "searchField");
    public final static IRI ESCAPE_TERM_FUNCTION = SVF.createIRI(NAMESPACE, "escapeTerm");
    public final static IRI GROUP_TERMS_FUNCTION = SVF.createIRI(NAMESPACE, "groupTerms");
    public final static IRI PHRASE_TERMS_FUNCTION = SVF.createIRI(NAMESPACE, "phraseTerms");

    public final static IRI WKT_POINT_FUNCTION = SVF.createIRI(NAMESPACE, "wktPoint");

    public final static IRI DATASET_IRI_FUNCTION = SVF.createIRI(NAMESPACE, "datasetIRI");
    public final static IRI PARALLEL_SPLIT_FUNCTION = SVF.createIRI(NAMESPACE, "forkAndFilterBy");

    public final static IRI DATA_URL_FUNCTION = SVF.createIRI(NAMESPACE, "dataURL");

    public final static IRI GET_FUNCTION = SVF.createIRI(NAMESPACE, "get");
    public final static IRI SLICE_FUNCTION = SVF.createIRI(NAMESPACE, "slice");
    public final static IRI FROM_TUPLE = SVF.createIRI(NAMESPACE, "fromTuple");
    public final static IRI LIKE_FUNCTION = SVF.createIRI(NAMESPACE, "like");

    public final static IRI MAX_WITH_FUNCTION = SVF.createIRI(NAMESPACE, "maxWith");
    public final static IRI MIN_WITH_FUNCTION = SVF.createIRI(NAMESPACE, "minWith");
    public final static IRI MODE_FUNCTION = SVF.createIRI(NAMESPACE, "mode");
    public final static IRI TOP_N_WITH_FUNCTION = SVF.createIRI(NAMESPACE, "topNWith");
    public final static IRI GROUP_INTO_TUPLES_FUNCTION = SVF.createIRI(NAMESPACE, "groupIntoTuples");
    public final static IRI GROUP_INTO_ARRAYS_FUNCTION = SVF.createIRI(NAMESPACE, "groupIntoArrays");
    public final static IRI H_INDEX_FUNCTION = SVF.createIRI(NAMESPACE, "hIndex");

	public final static IRI TIMESTAMP_PROPERTY = SVF.createIRI(NAMESPACE, "timestamp");

	public final static IRI IDENTIFIER_PROPERTY = SVF.createIRI(NAMESPACE, "identifier");

	public final static IRI VALUE_PROPERTY = SVF.createIRI(NAMESPACE, "value");

	public final static IRI XPATH_PROPERTY = SVF.createIRI(NAMESPACE, "xpath");

	public final static IRI NON_STRING_TYPE = SVF.createIRI(NAMESPACE, "nonString");

	public final static IRI ANY_NUMERIC_TYPE = SVF.createIRI(NAMESPACE, "anyNumeric");

	public final static IRI TUPLE_TYPE = SVF.createIRI(NAMESPACE, "tuple");
	public final static IRI ARRAY_TYPE = SVF.createIRI(NAMESPACE, "array");
	public final static IRI MAP_TYPE = SVF.createIRI(NAMESPACE, "map");

	public static final Namespace VALUE_ID_NS = new Base64Namespace("idv", "halyard:id:value:");
	
	public static final Namespace STATEMENT_ID_NS = new Base64Namespace("id3", "halyard:id:statement:");

	public static final String DATASET_NS = "halyard:dataset:";
}
