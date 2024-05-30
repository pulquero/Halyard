# object stats

PREFIX void: <http://rdfs.org/ns/void#>
PREFIX void-ext: <http://ldf.fi/void-ext#>
PREFIX halyard: <http://merck.github.io/Halyard/ns#>
PREFIX sd: <http://www.w3.org/ns/sparql-service-description#>
PREFIX dcterms: <http://purl.org/dc/terms/>

INSERT {
	GRAPH $statsContext {
		halyard:statsRoot void-ext:objectPartition ?partition .
		?partition a void:Dataset ;
			void-ext:object ?o ;
			void:distinctSubjects ?distinctSubjs ;
			void:triples ?total .
	}
} WHERE {
	{
		SELECT ?o (count(distinct ?s) as ?distinctSubjs) (count(*) as ?total) WHERE { ?s ?p ?o } GROUP BY ?o
	}
	FILTER (?total >= $threshold)
	BIND(halyard:datasetIRI(halyard:statsRoot, void-ext:object, ?o) as ?partition)
};

INSERT {
	GRAPH $statsContext {
		halyard:statsRoot void-ext:objectPartition ?partition .
		?partition a void:Dataset ;
			void-ext:object ?o ;
			void:distinctSubjects ?distinctSubjs ;
			void:triples ?total .
	}
} WHERE {
	{
		SELECT ?ng ?o (count(distinct ?s) as ?distinctSubjs) (count(*) as ?total) WHERE { GRAPH ?ng { ?s ?p ?o } } GROUP BY ?ng ?o
	}
	FILTER (?total >= $threshold)
	BIND(halyard:datasetIRI(?ng, void-ext:object, ?o) as ?partition)
};
