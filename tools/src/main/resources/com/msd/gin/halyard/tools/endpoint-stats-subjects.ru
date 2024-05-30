# subject stats

PREFIX void: <http://rdfs.org/ns/void#>
PREFIX void-ext: <http://ldf.fi/void-ext#>
PREFIX halyard: <http://merck.github.io/Halyard/ns#>
PREFIX sd: <http://www.w3.org/ns/sparql-service-description#>
PREFIX dcterms: <http://purl.org/dc/terms/>

INSERT {
	GRAPH $statsContext {
		halyard:statsRoot void-ext:subjectPartition ?partition .
		?partition a void:Dataset ;
			void-ext:subject ?s ;
			void:properties ?distinctPreds ;
			void:triples ?total .
	}
} WHERE {
	{
		SELECT ?s (count(distinct ?p) as ?distinctPreds) (count(*) as ?total) WHERE { ?s ?p ?o } GROUP BY ?s
	}
	FILTER (?total >= $threshold)
	BIND(halyard:datasetIRI(halyard:statsRoot, void-ext:subject, ?s) as ?partition)
};

INSERT {
	GRAPH $statsContext {
		halyard:statsRoot void-ext:subjectPartition ?partition .
		?partition a void:Dataset ;
			void-ext:subject ?s ;
			void:properties ?distinctPreds ;
			void:triples ?total .
	}
} WHERE {
	{
		SELECT ?ng ?s (count(distinct ?p) as ?distinctPreds) (count(*) as ?total) WHERE { GRAPH ?ng { ?s ?p ?o } } GROUP BY ?ng ?s
	}
	FILTER (?total >= $threshold)
	BIND(halyard:datasetIRI(?ng, void-ext:subject, ?s) as ?partition)
};
