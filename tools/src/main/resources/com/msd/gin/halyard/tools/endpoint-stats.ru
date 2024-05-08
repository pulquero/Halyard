PREFIX void: <http://rdfs.org/ns/void#>
PREFIX halyard: <http://merck.github.io/Halyard/ns#>
PREFIX sd: <http://www.w3.org/ns/sparql-service-description#>
PREFIX dcterms: <http://purl.org/dc/terms/>

INSERT {
	GRAPH $statsContext {
		halyard:statsRoot a void:Dataset, sd:Dataset, sd:Graph ;
			dcterms:modified $now ;
			sd:defaultGraph halyard:statsRoot ;
			void:triples ?total ;
			void:classes ?classes ;
			void:properties ?properties .
	}
} WHERE {
	{
		{
			SELECT (count(*) as ?total) (count(distinct ?p) as ?properties) WHERE { ?s ?p ?o }
		}
	} UNION {
		{
			SELECT (count(distinct ?type) as ?classes) WHERE { ?s a ?type }
		}
	}
};

INSERT {
	GRAPH $statsContext {
		halyard:statsRoot sd:namedGraph ?ng .
		?ng sd:name ?ng ;
			sd:graph ?ng ;
			a sd:NamedGraph, sd:Graph, void:Dataset ;
			dcterms:modified $now ;
			void:triples ?total ;
			void:classes ?classes ;
			void:properties ?properties .
	}
} WHERE {
	{
		{
			SELECT ?ng (count(*) as ?total) (count(distinct ?p) as ?properties) WHERE { GRAPH ?ng { ?s ?p ?o } } GROUP BY ?ng
		}
	} UNION {
		{
			SELECT ?ng (count(distinct ?type) as ?classes) WHERE { GRAPH ?ng { ?s a ?type } } GROUP BY ?ng
		}
	}
};

INSERT {
	GRAPH $statsContext {
		halyard:statsRoot void:propertyPartition ?partition .
		?partition a void:Dataset ;
			void:property ?p ;
			void:triples ?total .
	}
} WHERE {
	{
		SELECT ?p (count(*) as ?total) WHERE { ?s ?p ?o } GROUP BY ?p
	}
	FILTER (?total >= $threshold)
	BIND(halyard:datasetIRI(halyard:statsRoot, void:property, ?p) as ?partition)
};

INSERT {
	GRAPH $statsContext {
		halyard:statsRoot void:propertyPartition ?partition .
		?partition a void:Dataset ;
			void:property ?p ;
			void:triples ?total .
	}
} WHERE {
	{
		SELECT ?ng ?p (count(*) as ?total) WHERE { GRAPH ?ng { ?s ?p ?o } } GROUP BY ?ng ?p
	}
	FILTER (?total >= $threshold)
	BIND(halyard:datasetIRI(?ng, void:property, ?p) as ?partition)
};
