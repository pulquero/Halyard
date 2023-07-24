# HalyardElasticIndexer

This doesn't setup the index for you

The responsibility is with the caller to clear, set mappings and create indexes, before having HalyardExport populate the index for you

To load data Halyard takes the output of a SELECT query and creates a json object and pumps into Elasticsearch,

By writing a SELECT variable it will map to Elasticsearch field of the same name

To use this functionality you must set the queryParam `target` to the URL of an Elasticsearch index

Caveat: you must have a variable called 'value' and this must be a literal IRI (An RDF term)
This is because we would like to have a subject returned from Elasticsearch which exists in Halyard so we can query off of it

Classes of note:
* [HalyardExport](/tools/src/main/java/com/msd/gin/halyard/tools/HalyardExport.java)
* [HalyardElasticIndexer](/tools/src/main/java/com/msd/gin/halyard/tools/HalyardElasticIndexer.java)

## Elasticsearch Mapping Data

The value field will be mapped to type, label, lang, id

An example of a mapping can be seen here (Note this is not a full Elasticsearch index config)
``` json
{
  "mappings": {
    "properties": {
      "id": {
        "index": false,
        "type": "keyword"
      },
      "iri": {
        "type": "keyword",
        "index": false
      },
      "datatype": {
        "type": "keyword",
        "index": false
      },
      "label": {
        "type": "text",
        "fields": {
          "integer": {
            "type": "long",
            "ignore_malformed": true,
            "coerce": false
          },
          "number": {
            "type": "double",
            "ignore_malformed": true,
            "coerce": false
          },
          "point": {
            "type": "geo_point",
            "ignore_malformed": true
          }
        }
      },
      "lang": {
        "type": "keyword",
        "index": false
      }
    }
  }
}
```

## Writing queries

Queries must match the syntax of a [Query string query](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html)

Example
``` sparql
PREFIX halyard: <http://merck.github.io/Halyard/ns#> 
select * 
{ 
  [] a halyard:Query; 
    halyard:query 'phrase to find';
    halyard:limit 5; 
    halyard:minScore 0; 
    halyard:fuzziness 1; 
    halyard:phraseSlop 0; 
    halyard:matches [
      rdf:value ?v;
      halyard:score ?score; 
      halyard:index ?index
    ] 
}
```


A number of Halyard Utility functions are available to help in writing queries:

### EscapeTerm
[`halyard:escapeTerm`](/sail/src/main/java/com/msd/gin/halyard/sail/search/function/EscapeTerm.java)

Elasticsearch's query strings have certain [reserved characters which require escaping](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html#_reserved_characters)

### GroupTerm
[`halyard:groupTerm`](/sail/src/main/java/com/msd/gin/halyard/sail/search/function/GroupTerms.java)

###  PhraseTerm
[`halyard:phraseTerm`](/sail/src/main/java/com/msd/gin/halyard/sail/search/function/PhraseTerms.java)
