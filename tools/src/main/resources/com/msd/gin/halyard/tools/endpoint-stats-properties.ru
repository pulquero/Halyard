# property stats

PREFIX void: <http://rdfs.org/ns/void#>
PREFIX void-ext: <http://ldf.fi/void-ext#>
PREFIX halyard: <http://merck.github.io/Halyard/ns#>
PREFIX sd: <http://www.w3.org/ns/sparql-service-description#>
PREFIX dcterms: <http://purl.org/dc/terms/>

INSERT {
	GRAPH $statsContext {
		halyard:statsRoot void:propertyPartition ?partition .
		?partition a void:Dataset ;
			void:property ?p ;
			void:distinctObjects ?distinctObjs ;
			void:triples ?total .
	}
} WHERE {
	{
		SELECT ?p (count(distinct ?o) as ?distinctObjs) (count(*) as ?total) WHERE { ?s ?p ?o } GROUP BY ?p
	}
	FILTER (?total >= $threshold)
	BIND(halyard:datasetIRI(halyard:statsRoot, void:property, ?p) as ?partition)
};

INSERT {
	GRAPH $statsContext {
		halyard:statsRoot void:propertyPartition ?partition .
		?partition a void:Dataset ;
			void:property ?p ;
			void:distinctObjects ?distinctObjs ;
			void:triples ?total .
	}
} WHERE {
	{
		SELECT ?ng ?p (count(distinct ?o) as ?distinctObjs) (count(*) as ?total) WHERE { GRAPH ?ng { ?s ?p ?o } } GROUP BY ?ng ?p
	}
	FILTER (?total >= $threshold)
	BIND(halyard:datasetIRI(?ng, void:property, ?p) as ?partition)
};
