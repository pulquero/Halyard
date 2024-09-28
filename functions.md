# Functions

## MD5






## NOW






## RAND






## SHA1






## SHA256






## SHA384






## SHA512






## STRDT






## STRLANG






## STRUUID






## TZ






## UUID






## http://jena.hpl.hp.com/ARQ/function#localname






## http://merck.github.io/Halyard/ns#dataURL

Creates a data URL with the given value.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Literal
2. arg2 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2000/01/rdf-schema#Resource

## http://merck.github.io/Halyard/ns#datasetIRI






## http://merck.github.io/Halyard/ns#escapeTerm






## http://merck.github.io/Halyard/ns#forkAndFilterBy






## http://merck.github.io/Halyard/ns#get

Returns an element from a tuple.

#### Arguments

1. arg1 - http://merck.github.io/Halyard/ns#tuple
2. arg2 - http://www.w3.org/2001/XMLSchema#int

#### Returns

http://www.w3.org/2000/01/rdf-schema#Resource

## http://merck.github.io/Halyard/ns#groupTerms






## http://merck.github.io/Halyard/ns#like






## http://merck.github.io/Halyard/ns#phraseTerms






## http://merck.github.io/Halyard/ns#searchField






## http://merck.github.io/Halyard/ns#slice

Returns a sub-tuple of a tuple.

#### Arguments

1. arg1 - http://merck.github.io/Halyard/ns#tuple
2. arg2 - http://www.w3.org/2001/XMLSchema#int
3. arg3 - http://www.w3.org/2001/XMLSchema#int

#### Returns

http://merck.github.io/Halyard/ns#tuple

## http://merck.github.io/Halyard/ns#tuple

Tuple literal constructor function.


#### Returns

http://merck.github.io/Halyard/ns#tuple

## http://merck.github.io/Halyard/ns#vectorEmbedding






## http://merck.github.io/Halyard/ns#wktPoint

Creates a WKT POINT literal.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#double
2. arg2 - http://www.w3.org/2001/XMLSchema#double

#### Returns

http://www.opengis.net/ont/geosparql#wktLiteral

## http://spinrdf.org/sp#abs

Returns the absolute value of arg. An error is raised if arg is not a numeric value.

#### Arguments

1. arg1



## http://spinrdf.org/sp#add

Returns the arithmetic sum of its operands.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Resource
2. arg2 - http://www.w3.org/2000/01/rdf-schema#Resource



## http://spinrdf.org/sp#and

Return the logical AND between two (boolean) operands.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#boolean
2. arg2 - http://www.w3.org/2001/XMLSchema#boolean

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://spinrdf.org/sp#bnode

Constructs a blank node that is distinct from all blank nodes in the dataset being queried and distinct from all blank nodes created by calls to this constructor for other query solutions. If the no argument form is used, every call results in a distinct blank node. If the form with a simple literal is used, every call results in distinct blank nodes for different simple literals, and the same blank node for calls with the same simple literal within expressions for one solution mapping. This functionality is compatible with the treatment of blank nodes in SPARQL CONSTRUCT templates.

#### Arguments

1. arg1



## http://spinrdf.org/sp#bound

Returns true if ?arg1 is bound to a value. Returns false otherwise. Variables with the value NaN or INF are considered bound.

#### Arguments

1. arg1

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://spinrdf.org/sp#ceil

Returns the smallest (closest to negative infinity) number with no fractional part that is not less than the value of arg. An error is raised if ?arg1 is not a numeric value.

#### Arguments

1. arg1



## http://spinrdf.org/sp#contains

Returns an xsd:boolean indicating whether or not the value of ?arg1 contains (at the beginning, at the end, or anywhere within) at least one sequence of collation units that provides a minimal match to the collation units in the value of ?arg2, according to the collation that is used.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg2 - http://www.w3.org/2001/XMLSchema#string



## http://spinrdf.org/sp#datatype

Returns the datatype IRI of argument ?arg1; returns xsd:string if the parameter is a simple literal.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Literal

#### Returns

http://www.w3.org/2000/01/rdf-schema#Class

## http://spinrdf.org/sp#day

Extracts the day from a date/time literal.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Literal

#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://spinrdf.org/sp#divide

Returns the arithmetic quotient of its operands.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Literal
2. arg2 - http://www.w3.org/2000/01/rdf-schema#Literal

#### Returns

http://www.w3.org/2000/01/rdf-schema#Literal

## http://spinrdf.org/sp#encode_for_uri



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/sp#eq

Returns true if both arguments are equal.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#boolean
2. arg2 - http://www.w3.org/2001/XMLSchema#boolean

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://spinrdf.org/sp#floor

Returns the largest (closest to positive infinity) number with no fractional part that is not greater than the value of ?arg1. An error is raised if ?arg1 is not a numeric value.

#### Arguments

1. arg1



## http://spinrdf.org/sp#ge

Returns true if ?arg1 >= ?arg2.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Literal
2. arg2 - http://www.w3.org/2000/01/rdf-schema#Literal

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://spinrdf.org/sp#gt

Returns true if ?arg1 > arg2.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Literal
2. arg2 - http://www.w3.org/2000/01/rdf-schema#Literal

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://spinrdf.org/sp#hours

Extracts the hours from a date/time literal.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Literal

#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://spinrdf.org/sp#if

The SPARQL 1.1 built-in function IF.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#boolean
2. arg2
3. arg3



## http://spinrdf.org/sp#in

Checks whether the value on the left (?arg1) is one of the values on the right (?arg2, ?arg3 ...).

#### Arguments

1. arg1

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://spinrdf.org/sp#iri

Creates a IRI resource (node) from a given IRI string (?arg1).

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2000/01/rdf-schema#Resource

## http://spinrdf.org/sp#isBlank

Checks whether a given node is a blank node.

#### Arguments

1. arg1

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://spinrdf.org/sp#isIRI

Checks whether a given node is a IRI node.

#### Arguments

1. arg1

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://spinrdf.org/sp#isLiteral

Checks whether a given node is a literal.

#### Arguments

1. arg1

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://spinrdf.org/sp#isNumeric

Returns true if arg1 is a numeric value. Returns false otherwise. term is numeric if it has an appropriate datatype (see the section Operand Data Types) and has a valid lexical form, making it a valid argument to functions and operators taking numeric arguments.

#### Arguments

1. arg1



## http://spinrdf.org/sp#isURI

Checks whether a node is a URI.

#### Arguments

1. arg1

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://spinrdf.org/sp#lang

Returns the language tag of ?arg1, if it has one. It returns "" if the literal has no language tag. Node that the RDF data model does not include literals with an empty language tag.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Literal

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/sp#langMatches

Returns true if language-tag (first argument) matches language-range (second argument) per the basic filtering scheme defined in [RFC4647] section 3.3.1.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Literal
2. arg2 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://spinrdf.org/sp#lcase

Converts a string to lower case characters.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/sp#le

Returns true if ?arg1 <= ?arg2.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Literal
2. arg2 - http://www.w3.org/2000/01/rdf-schema#Literal

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://spinrdf.org/sp#lt

Returns true if ?arg1 < ?arg2.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Literal
2. arg2 - http://www.w3.org/2000/01/rdf-schema#Literal

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://spinrdf.org/sp#md5

Returns the MD5 checksum, as a hex digit string, calculated on the UTF-8 representation of the simple literal or lexical form of the xsd:string. Hex digits SHOULD be in lower case.

#### Arguments

1. arg1

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/sp#minutes

Extracts the minutes from a date/time literal.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Literal

#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://spinrdf.org/sp#month

Extracts the month from a date/time literal.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Literal

#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://spinrdf.org/sp#mul

Returns the arithmetic product of its operands.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Literal
2. arg2 - http://www.w3.org/2000/01/rdf-schema#Literal



## http://spinrdf.org/sp#ne

Returns true if ?arg1 != ?arg2.

#### Arguments

1. arg1
2. arg2

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://spinrdf.org/sp#not

Returns the boolean negation of the argument.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#boolean

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://spinrdf.org/sp#notIn

Checks whether the value on the left (?arg1) is none of the values on the right (?arg2, ?arg3 ...).

#### Arguments

1. arg1

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://spinrdf.org/sp#or

Returns the logical OR between two (boolean) operands.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#boolean
2. arg2 - http://www.w3.org/2001/XMLSchema#boolean

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://spinrdf.org/sp#regex

Returns true if a string (?arg1) matches the regular expression supplied as a pattern (?arg2) as influenced by the value of flags (?arg3), otherwise returns false.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg2 - http://www.w3.org/2001/XMLSchema#string
3. arg3 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://spinrdf.org/sp#replace

Replaces each non-overlapping occurrence of a regular expression pattern with a replacement string. Regular expession matching may involve modifier flags.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg2 - http://www.w3.org/2001/XMLSchema#string
3. arg3 - http://www.w3.org/2001/XMLSchema#string
4. arg4 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/sp#round

Returns the number with no fractional part that is closest to the argument. If there are two such numbers, then the one that is closest to positive infinity is returned. An error is raised if ?arg1 is not a numeric value.

#### Arguments

1. arg1



## http://spinrdf.org/sp#sameTerm

Returns TRUE if ?arg1 and ?arg2 are the same RDF term as defined in Resource Description Framework (RDF): Concepts and Abstract Syntax; returns FALSE otherwise.

#### Arguments

1. arg1
2. arg2

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://spinrdf.org/sp#seconds

Extracts the seconds from a date/time literal.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Literal

#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://spinrdf.org/sp#sha1

Returns the SHA1 checksum, as a hex digit string, calculated on the UTF-8 representation of the simple literal or lexical form of the xsd:string. Hex digits SHOULD be in lower case.

#### Arguments

1. arg1

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/sp#sha256

Returns the SHA256 checksum, as a hex digit string, calculated on the UTF-8 representation of the simple literal or lexical form of the xsd:string. Hex digits SHOULD be in lower case.

#### Arguments

1. arg1

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/sp#sha384

Returns the SHA384 checksum, as a hex digit string, calculated on the UTF-8 representation of the simple literal or lexical form of the xsd:string. Hex digits SHOULD be in lower case.

#### Arguments

1. arg1

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/sp#sha512

Returns the SHA512 checksum, as a hex digit string, calculated on the UTF-8 representation of the simple literal or lexical form of the xsd:string. Hex digits SHOULD be in lower case.

#### Arguments

1. arg1

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/sp#str

Returns the lexical form of ?arg1 (a literal); returns the codepoint representation of ?arg1 (an IRI). This is useful for examining parts of an IRI, for instance, the host-name.

#### Arguments

1. arg1

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/sp#strafter

Returns a literal of the same kind (simple literal, plain literal same language tag, xsd:string) as the first argument arg1. The lexical form of the result is the substring of the value of arg1 that proceeds in arg1 the first occurrence of the lexical form of arg2; otherwise the lexical form of the result is the empty string. If the lexical form of arg2 is the empty string, the lexical form of the result is the emprty string.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg2 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/sp#strbefore

Returns a literal of the same kind (simple literal, plain literal same language tag, xsd:string) as the first argument arg1. The lexical form of the result is the substring of the value of arg1 that precedes in arg1 the first occurrence of the lexical form of arg2; otherwise the lexical form of the result is the empty string. If the lexical form of arg2 is the empty string, the lexical form of the result is the emprty string.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg2 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/sp#strdt

Constructs a literal with lexical form and type as specified by the arguments.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg2 - http://www.w3.org/2000/01/rdf-schema#Datatype



## http://spinrdf.org/sp#strends

Returns true if the lexical form of ?arg1 ends with the lexical form of ?arg2, otherwise it returns false.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg2 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://spinrdf.org/sp#strlang

Takes a string (?arg1) and a language (?arg2) and constructs a literal with a corresponding language tag.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg2 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/1999/02/22-rdf-syntax-ns#PlainLiteral

## http://spinrdf.org/sp#strlen

Computes the length of a given input string.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://spinrdf.org/sp#strstarts

Returns true if the lexical form of ?arg1 begins with the lexical form of ?arg2, otherwise it returns false.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg2 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://spinrdf.org/sp#sub

Returns the arithmetic difference of its operands.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Literal
2. arg2 - http://www.w3.org/2000/01/rdf-schema#Literal



## http://spinrdf.org/sp#substr

Gets the sub-string of a given string. The index of the first character is 1.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg2 - http://www.w3.org/2001/XMLSchema#integer
3. arg3 - http://www.w3.org/2001/XMLSchema#integer

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/sp#timezone

Returns the timezone part of ?arg1 as an xsd:dayTimeDuration. Raises an error if there is no timezone.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#dateTime

#### Returns

http://www.w3.org/2001/XMLSchema#duration

## http://spinrdf.org/sp#ucase

Converts a string to upper case characters.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/sp#unaryMinus

Returns the operand ?arg1 with the sign reversed. If ?arg1 is positive, its negative is returned; if it is negative, its positive is returned.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Literal



## http://spinrdf.org/sp#unaryPlus

Returns the operand ?arg1 with the sign unchanged. Semantically, this operation performs no operation.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Literal



## http://spinrdf.org/sp#uri

Equivalent to IRI.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2000/01/rdf-schema#Resource

## http://spinrdf.org/sp#year

Extracts the year from a date/time literal.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Literal

#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://spinrdf.org/spif#buildString

Constructs a new string by inserting the existing variable bindings into a template. The template can mention variable names in curly braces, such as "Hello {?index}" would create "Hello 42" is ?index has the value 42. As an alternative to variable names, the function can take additional arguments after the template, the variables of which can be accessed using {?1}, {?2} etc. For example: smf:buildString("Hello-{?1}-{?2}", ?day, ?month) would insert day and month at places {?1} and {?2}.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/spif#buildStringFromRDFList

Builds a string from the members of a given rdf:List (?arg1). The function iterates over all members of the list (which must be well-formed according to the RDF syntax rules). For each member, a string template (?arg2) is applied where the expression {?member} will be substituted with the current member. Optionally, a separator (?arg3) can be inserted between the list members in the result string, e.g. to insert a comma.

#### Arguments

1. arg1 - http://www.w3.org/1999/02/22-rdf-syntax-ns#List
2. arg2 - http://www.w3.org/2001/XMLSchema#string
3. arg3 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/spif#buildURI

Constructs a new URI resource by inserting the existing variable bindings into a template. The template can mention variable names in curly braces, such as "my:Instance-{?index}" would create "my:Instance-42" is ?index has the value 42. As an alternative to variable names, the function can take additional arguments after the template, the variables of which can be accessed using {?1}, {?2} etc. For example: smf:buildURI("my:Instance-{?1}-{?2}", ?day, ?month) would insert day and month at places {?1} and {?2}.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2000/01/rdf-schema#Resource

## http://spinrdf.org/spif#buildUniqueURI

A variation of smf:buildURI that also makes sure that the created URI is unique in the current graph (that is, no triple contains the URI as either subject, predicate or object). This function is particularly useful for ontology mapping from a legacy data source into an RDF model.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2000/01/rdf-schema#Resource

## http://spinrdf.org/spif#camelCase

Converts an input string into camel case. 
For example, "semantic web" becomes "SemanticWeb".
An optional matching expression can be given to only convert the matched characters.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg2 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/spif#canInvoke

Checks whether a given SPIN function (?arg1) can be invoked with a given list of argument (?arg2, ?arg3, ...) without violating any of its declared SPIN constraints. In addition to the usual argument declarations, the SPIN function may declare ASK and CONSTRUCT queries to check additional pre-conditions.

#### Arguments

1. arg1 - http://spinrdf.org/spin#Function
2. arg2
3. arg3
4. arg4
5. arg5

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://spinrdf.org/spif#cast

Creates a new literal from an existing literal, but with a different datatype. This can, for example, be used to convert between floating point values and int values.

#### Arguments

1. datatype - http://www.w3.org/2000/01/rdf-schema#Datatype
2. arg1 - http://www.w3.org/2000/01/rdf-schema#Literal

#### Returns

http://www.w3.org/2000/01/rdf-schema#Literal

## http://spinrdf.org/spif#convertSPINRDFToString

Converts a SPARQL query encoded in SPIN RDF format to a SPARQL string in textual form. The SPIN query must be well-formed in the context graph at execution time, and the provided argument must be the root of the expression (e.g., an instance of sp:Select).

This function is available as part of the TopBraid SPIN Libraries.

#### Arguments

1. arg1 - http://spinrdf.org/sp#Query
2. arg2 - http://www.w3.org/2001/XMLSchema#boolean

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/spif#countMatches

Counts all occurrences of a triple pattern based on subject (?arg1), predicate (?arg2) and object (?arg3) input. Any of those can be unbound variables.

This function is available as part of the TopBraid SPIN Libraries.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Resource
2. arg2 - http://www.w3.org/1999/02/22-rdf-syntax-ns#Property
3. arg3

#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://spinrdf.org/spif#countTransitiveSubjects

Given a predicate and an object, this function computes the number of matches using

SELECT (COUNT(DISTINCT ?subject) AS ?result)
WHERE {
	?subject ?predicate* ?object .
}

The main purpose of this function is to optimize performance - this direction of * traversal is currently very slow in Jena. The function can be used to compute the number of subclasses of a given class.

#### Arguments

1. arg1 - http://www.w3.org/1999/02/22-rdf-syntax-ns#Property
2. arg2 - http://www.w3.org/2000/01/rdf-schema#Resource

#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://spinrdf.org/spif#currentTimeMillis






## http://spinrdf.org/spif#dateFormat

Takes a date/time literal and a pattern and renders the date according to the pattern. This is a reverse of spif:parseDate and uses the same format.

#### Arguments

1. date
2. pattern - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/spif#decimalFormat

Takes a number as its first argument and applies a given formatting string to it, for example, to convert a floating point into a number that has exactly two decimal places after the dot. For example, spif:decimalFormat(12.3456, "#.##") returns "12.35". The resulting string can then by cast back to a number, e.g. using xsd:double(?str).

#### Arguments

1. number - http://www.w3.org/2001/XMLSchema#decimal
2. pattern - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/spif#decodeURL

Decodes a URL string - this is the inverse operation of spif:encodeURL.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg2 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/spif#encodeURL

Encodes a URL string, for example so that it can be passed as an argument to REST services.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg2 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/spif#generateLabel

Constructs a human-readable label for a URI resource by taking everything after the last '/' or the last '#' as starting point.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Resource

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/spif#generateUUID






## http://spinrdf.org/spif#hasAllObjects

Checks whether a given subject/predicate combination has all values enumerated from a given rdf:List. In other words, for each member ?object of the rdf:List, the triple (?arg1, ?arg2, ?object) must be in the model to return true. If the list is empty, true will also be returned.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Resource
2. arg2 - http://www.w3.org/1999/02/22-rdf-syntax-ns#Property
3. arg3 - http://www.w3.org/1999/02/22-rdf-syntax-ns#List

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://spinrdf.org/spif#indexOf

Gets the index of the first occurrence of a certain substring in a given search string. Returns an error if the substring is not found.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg2 - http://www.w3.org/2001/XMLSchema#string
3. arg3 - http://www.w3.org/2001/XMLSchema#integer

#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://spinrdf.org/spif#invoke

Calls another SPARQL function specified by a URI resource (?arg1), with additional arguments passed into from ?arg2 onwards. This can be used to dynamically call functions, the URI of which is now known statically. The result of the function call will be passed on as result of the invoke call.

The function can also be a binary built-in SPARQL function using the SPIN function identifiers from the SPL ontology. For example, sp:gt will be executed as ?left > ?right.

#### Arguments

1. arg1 - http://spinrdf.org/spin#Function
2. arg2
3. arg3
4. arg4
5. arg5



## http://spinrdf.org/spif#isReadOnlyTriple

Checks whether a given triple is read-only, that is, cannot be deleted. Triples that are in the system ontology are generally not deletable. TopBraid also enforces that triples from a read-only file or back-end are read-only. Other platforms may have different privilege rules for this function.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Resource
2. arg2 - http://www.w3.org/1999/02/22-rdf-syntax-ns#Property
3. arg3

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://spinrdf.org/spif#isValidURI

Checks whether a given input string is a well-formed absolute URI. This can be used to validate user input before it is turned into a URI resource.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://spinrdf.org/spif#lastIndexOf

Gets the index of the last occurrence of a certain substring in a given search string. Returns an error if the substring is not found.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg2 - http://www.w3.org/2001/XMLSchema#string
3. arg3 - http://www.w3.org/2001/XMLSchema#integer

#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://spinrdf.org/spif#localName

Gets a "local name" from a URI resource. This takes everything after the last '/' or '#' character of the URI. This function is a more intuitive alternative to afn:localname, which strictly follows the W3C namespace splitting algorithm that often leads to surprising results.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Resource

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/spif#lowerCamelCase

Converts an input string into lower camel case.
For example, "semantic web" becomes "semanticWeb".
An optional matching expression can be given to only convert the matched characters.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg2 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/spif#lowerCase

Converts an input string into lower case. 
For example, "SEMANTIC Web" becomes "semantic web".
An optional matching expression can be given to only convert the matched characters.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg2 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/spif#lowerTitleCase

Converts an input string into lower title case. 
For example, "semantic web" becomes "semantic Web".
An optional matching expression can be given to only convert the matched characters.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg2 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/spif#mod

The mathematical modulo operator, aka % in Java.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#integer
2. arg2 - http://www.w3.org/2001/XMLSchema#integer

#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://spinrdf.org/spif#name

Gets a human-readable string representation from an RDF node. If it's a literal, the function will return the literal's lexical text. If it's a resource the system will use the rdfs:label (if exists) or otherwise use the qname. For an unbound input, the function will return no value.

#### Arguments

1. arg1

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/spif#parseDate

Converts a string in a semi-structured format into a xsd:date, xsd:dateTime or xsd:time literal. The input string must be in a given template format, e.g. "yyyy.MM.dd G 'at' HH:mm:ss z" for strings such as 2001.07.04 AD at 12:08:56 PDT.

#### Arguments

1. pattern - http://www.w3.org/2001/XMLSchema#string
2. arg1 - http://www.w3.org/2001/XMLSchema#string
3. arg3 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2000/01/rdf-schema#Literal

## http://spinrdf.org/spif#random






## http://spinrdf.org/spif#regex

An input string is converted into a result string by applying a match and replacement expressions.
For example, the input string "semantic web" with the match expression "([A-z]+) ([A-z]+)" and the replacement expression "The $1 life" returns the string "The semantic life".
An optional input string is returned, if no match occurs. If this string is empty and no match occurs, then the result string is unbound.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg2 - http://www.w3.org/2001/XMLSchema#string
3. arg3 - http://www.w3.org/2001/XMLSchema#string
4. arg4 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/spif#replaceAll

Does a string replacement based on the Java function String.replaceAll().

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg2 - http://www.w3.org/2001/XMLSchema#string
3. arg3 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/spif#shortestObjectsPath

Finds the shortest path from a given subject walking up a given predicate (for example, rdfs:subClassOf) and returns the path as a string of URIs separated with a space. This can be used to find the shortest path from a resource in a tree structure to the root resource.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Resource
2. arg2 - http://www.w3.org/1999/02/22-rdf-syntax-ns#Property
3. arg3 - http://www.w3.org/2000/01/rdf-schema#Resource

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/spif#shortestSubjectsPath

Finds the shortest path from a given object walking up a given predicate (for example, schema:child) and returns the path as a string of URIs separated with a space. This can be used to find the shortest path from a resource in a tree structure to the root resource.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Resource
2. arg2 - http://www.w3.org/1999/02/22-rdf-syntax-ns#Property
3. arg3 - http://www.w3.org/2000/01/rdf-schema#Resource

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/spif#timeMillis

Returns the time of a given xsd:dateTime value in milliseconds.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#dateTime

#### Returns

http://www.w3.org/2001/XMLSchema#long

## http://spinrdf.org/spif#titleCase

Converts an input string to title case.
For example, "germany" becomes "Germany".
An optional matching expression can be given to only convert the matched characters.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg2 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/spif#toJavaIdentifier

Produces a valid Java identifier based on a given input string, dropping any characters that would not be valid Java identifiers. Produces the empty string if no character can be reused from the given string. Note that this function is even stricter than the normal Java identifier algorithm, as it only allows ASCII characters or digits.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/spif#trim

Creates a new string value by trimming an input string. Leading and trailing whitespaces are deleted.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/spif#unCamelCase

Converts an input string into a reverse camel case.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/spif#upperCase

Converts an input string into upper case. 
For example, "semantic web" becomes "SEMANTIC WEB".
An optional matching expression can be given to only convert the matched characters.

#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg2 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://spinrdf.org/spif#walkObjects

Performs a depth-first tree traversal starting at a given node (?arg1) and then following the objects using a given predicate (?arg2). For each node it applies a given function (?arg3) that must take the current node as its first argument. All other arguments of the walkObjects function call will be passed into that function. The traversal stops on the first non-null result of the nested function calls.

As use case of this function is to walk up superclasses, e.g. to find the "nearest" owl:Restriction of a certain kind.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Resource
2. arg2 - http://www.w3.org/1999/02/22-rdf-syntax-ns#Property
3. arg3 - http://spinrdf.org/spin#Function



## http://spinrdf.org/spin#eval

Evaluates a given SPIN expression or SELECT or ASK query, and returns its result. The first argument must be the expression in SPIN RDF syntax. All other arguments must come in pairs: first a property name, and then a value. These name/value pairs will be pre-bound variables for the execution of the expression.

#### Arguments

1. arg1



## http://spinrdf.org/spin#violatesConstraints

Checks whether a given instance (?arg1) violates any of the constraints defined for a given class (?arg2).

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Resource
2. arg2 - http://www.w3.org/2000/01/rdf-schema#Class

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://spinrdf.org/spl#hasArgument

Checks if a given module class (?module) has at least one declared spl:Argument.

#### Arguments

1. class - http://spinrdf.org/spin#Module

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://spinrdf.org/spl#hasPrimaryKey

Checks if a given class has a declared primary key, using spl:PrimaryKeyPropertyConstraint.

#### Arguments

1. class - http://www.w3.org/2000/01/rdf-schema#Class

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://spinrdf.org/spl#hasValue

Checks whether a given resource (?arg1) has a given value (?arg3) for a given property (?arg2) or one of the sub-properties of it.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Resource
2. arg2 - http://www.w3.org/1999/02/22-rdf-syntax-ns#Property
3. arg3

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://spinrdf.org/spl#hasValueOfType

Checks whether a given subject (?arg1) has at least one value of a given type (?arg3) for a given property (?arg2) or one of its sub-properties.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Resource
2. arg2 - http://www.w3.org/1999/02/22-rdf-syntax-ns#Property
3. arg3 - http://www.w3.org/2000/01/rdf-schema#Class

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://spinrdf.org/spl#instanceOf

Checks whether a given resource (?arg1) has a given type (?arg2). In order to fulfill this condition, there must either be a triple ?arg1 rdf:type ?arg2, or ?instance rdf:type ?subClass where ?subClass is a subclass of ?arg2. If the first argument is a literal, then the second argument must be the matching XSD datatype.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Resource
2. arg2 - http://www.w3.org/2000/01/rdf-schema#Class

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://spinrdf.org/spl#isPrimaryKeyPropertyOfInstance

Checks if a given property is the primary key of a given instance.

#### Arguments

1. instance - http://www.w3.org/2000/01/rdf-schema#Resource
2. property - http://www.w3.org/1999/02/22-rdf-syntax-ns#Property

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://spinrdf.org/spl#isUntypedLiteral

Checks whether a given literal is untyped. This function was introduced because the built-in datatype operand in SPARQL casts untyped literals to xsd:string, making it impossible to check it this way. This function here uses a work-around using sameTerm instead.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Literal

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://spinrdf.org/spl#max

Takes two arguments and returns the larger one of them.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Literal
2. arg2 - http://www.w3.org/2000/01/rdf-schema#Literal

#### Returns

http://www.w3.org/2000/01/rdf-schema#Literal

## http://spinrdf.org/spl#min

Takes two arguments and returns the smaller one of them.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Literal
2. arg2 - http://www.w3.org/2000/01/rdf-schema#Literal

#### Returns

http://www.w3.org/2000/01/rdf-schema#Literal

## http://spinrdf.org/spl#object

Gets the object of a given subject (?arg1) / predicate (?arg2) combination. Note that if multiple values are present then the result might be unpredictably random.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Resource
2. arg2 - http://www.w3.org/1999/02/22-rdf-syntax-ns#Property



## http://spinrdf.org/spl#objectCount

Gets the number of values of a given property (?arg2) at a given subject (?arg1). The result is the number of matches of (?arg1, ?arg2, ?object).

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Resource
2. arg2 - http://www.w3.org/1999/02/22-rdf-syntax-ns#Property

#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://spinrdf.org/spl#objectInGraph

Gets the object of a given subject (?arg1) / predicate (?arg2) combination in a given graph ?arg3. Note that if multiple values are present then the result might be unpredictably random.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Resource
2. arg2 - http://www.w3.org/1999/02/22-rdf-syntax-ns#Property
3. arg3 - http://www.w3.org/2000/01/rdf-schema#Resource



## http://spinrdf.org/spl#objectSubProp

Gets the object of a given subject (?arg1) / predicate (?arg2) combination, also taking the sub-properties of ?arg2 into account. Note that if multiple values are present then the result might be unpredictably random.

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Resource
2. arg2 - http://www.w3.org/1999/02/22-rdf-syntax-ns#Property



## http://spinrdf.org/spl#primaryKeyProperty

Gets the primary key property declared for a given class, using spl:PrimaryKeyPropertyConstraint.

#### Arguments

1. class - http://www.w3.org/2000/01/rdf-schema#Class

#### Returns

http://www.w3.org/1999/02/22-rdf-syntax-ns#Property

## http://spinrdf.org/spl#primaryKeyURIStart

Gets the URI start declared as part of a primary key declaration for a given class, using spl:PrimaryKeyPropertyConstraint.

#### Arguments

1. class - http://www.w3.org/2000/01/rdf-schema#Class

#### Returns

http://www.w3.org/1999/02/22-rdf-syntax-ns#Property

## http://spinrdf.org/spl#subClassOf

Checks whether a given class (?arg1) is a (transitive) sub-class of another class (?arg2).

#### Arguments

1. arg1 - http://www.w3.org/2000/01/rdf-schema#Class
2. arg2 - http://www.w3.org/2000/01/rdf-schema#Class

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://spinrdf.org/spl#subPropertyOf

Checks whether a given property (?arg1) is a (transitive) sub-property of another property (?arg2).

#### Arguments

1. arg1 - http://www.w3.org/1999/02/22-rdf-syntax-ns#Property
2. arg2 - http://www.w3.org/1999/02/22-rdf-syntax-ns#Property

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://spinrdf.org/spl#subject

Gets the "first" subject of a given predicate (?arg1)/object (?arg2) combination. Note that if multiple values are present then the result might be unpredictably random.

#### Arguments

1. arg1 - http://www.w3.org/1999/02/22-rdf-syntax-ns#Property
2. arg2



## http://spinrdf.org/spl#subjectCount

Gets the number of values of a given property (?arg1) at a given object (?arg2). The result is the number of matches of (?subject, ?arg1, ?arg2).

#### Arguments

1. arg1 - http://www.w3.org/1999/02/22-rdf-syntax-ns#Property
2. arg2

#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://www.opengis.net/def/function/geosparql/boundary






## http://www.opengis.net/def/function/geosparql/buffer






## http://www.opengis.net/def/function/geosparql/convexHull






## http://www.opengis.net/def/function/geosparql/difference






## http://www.opengis.net/def/function/geosparql/distance






## http://www.opengis.net/def/function/geosparql/ehContains






## http://www.opengis.net/def/function/geosparql/ehCoveredBy






## http://www.opengis.net/def/function/geosparql/ehCovers






## http://www.opengis.net/def/function/geosparql/ehDisjoint






## http://www.opengis.net/def/function/geosparql/ehEquals






## http://www.opengis.net/def/function/geosparql/ehInside






## http://www.opengis.net/def/function/geosparql/ehMeet






## http://www.opengis.net/def/function/geosparql/ehOverlap






## http://www.opengis.net/def/function/geosparql/envelope






## http://www.opengis.net/def/function/geosparql/getSRID






## http://www.opengis.net/def/function/geosparql/intersection






## http://www.opengis.net/def/function/geosparql/rcc8dc






## http://www.opengis.net/def/function/geosparql/rcc8ec






## http://www.opengis.net/def/function/geosparql/rcc8eq






## http://www.opengis.net/def/function/geosparql/rcc8ntpp






## http://www.opengis.net/def/function/geosparql/rcc8ntppi






## http://www.opengis.net/def/function/geosparql/rcc8po






## http://www.opengis.net/def/function/geosparql/rcc8tpp






## http://www.opengis.net/def/function/geosparql/rcc8tppi






## http://www.opengis.net/def/function/geosparql/relate






## http://www.opengis.net/def/function/geosparql/sfContains






## http://www.opengis.net/def/function/geosparql/sfCrosses






## http://www.opengis.net/def/function/geosparql/sfDisjoint






## http://www.opengis.net/def/function/geosparql/sfEquals






## http://www.opengis.net/def/function/geosparql/sfIntersects






## http://www.opengis.net/def/function/geosparql/sfOverlaps






## http://www.opengis.net/def/function/geosparql/sfTouches






## http://www.opengis.net/def/function/geosparql/sfWithin






## http://www.opengis.net/def/function/geosparql/symDifference






## http://www.opengis.net/def/function/geosparql/union






## http://www.w3.org/1999/02/22-rdf-syntax-ns#Statement






## http://www.w3.org/1999/02/22-rdf-syntax-ns#isTriple






## http://www.w3.org/1999/02/22-rdf-syntax-ns#object






## http://www.w3.org/1999/02/22-rdf-syntax-ns#predicate






## http://www.w3.org/1999/02/22-rdf-syntax-ns#subject






## http://www.w3.org/2001/XMLSchema#boolean






## http://www.w3.org/2001/XMLSchema#byte






## http://www.w3.org/2001/XMLSchema#date






## http://www.w3.org/2001/XMLSchema#dateTime






## http://www.w3.org/2001/XMLSchema#decimal






## http://www.w3.org/2001/XMLSchema#double






## http://www.w3.org/2001/XMLSchema#float






## http://www.w3.org/2001/XMLSchema#int






## http://www.w3.org/2001/XMLSchema#integer






## http://www.w3.org/2001/XMLSchema#long






## http://www.w3.org/2001/XMLSchema#negativeInteger






## http://www.w3.org/2001/XMLSchema#nonNegativeInteger






## http://www.w3.org/2001/XMLSchema#nonPositiveInteger






## http://www.w3.org/2001/XMLSchema#positiveInteger






## http://www.w3.org/2001/XMLSchema#short






## http://www.w3.org/2001/XMLSchema#string






## http://www.w3.org/2001/XMLSchema#unsignedByte






## http://www.w3.org/2001/XMLSchema#unsignedInt






## http://www.w3.org/2001/XMLSchema#unsignedLong






## http://www.w3.org/2001/XMLSchema#unsignedShort






## http://www.w3.org/2005/xpath-functions#QName



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg2 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#QName

## http://www.w3.org/2005/xpath-functions#abs



#### Arguments

1. arg1



## http://www.w3.org/2005/xpath-functions#adjust-date-to-timezone



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#date
2. arg1 - http://www.w3.org/2001/XMLSchema#date
3. arg2 - http://www.w3.org/2001/XMLSchema#dayTimeDuration

#### Returns

http://www.w3.org/2001/XMLSchema#date

## http://www.w3.org/2005/xpath-functions#adjust-dateTime-to-timezone



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#dateTime
2. arg1 - http://www.w3.org/2001/XMLSchema#dateTime
3. arg2 - http://www.w3.org/2001/XMLSchema#dayTimeDuration

#### Returns

http://www.w3.org/2001/XMLSchema#dateTime

## http://www.w3.org/2005/xpath-functions#adjust-time-to-timezone



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#time
2. arg1 - http://www.w3.org/2001/XMLSchema#time
3. arg2 - http://www.w3.org/2001/XMLSchema#dayTimeDuration

#### Returns

http://www.w3.org/2001/XMLSchema#time

## http://www.w3.org/2005/xpath-functions#analyze-string



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg1 - http://www.w3.org/2001/XMLSchema#string
3. arg2 - http://www.w3.org/2001/XMLSchema#string
4. arg2 - http://www.w3.org/2001/XMLSchema#string
5. arg3 - http://www.w3.org/2001/XMLSchema#string



## http://www.w3.org/2005/xpath-functions#apply



#### Arguments

1. arg1
2. arg2



## http://www.w3.org/2005/xpath-functions#available-environment-variables




#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://www.w3.org/2005/xpath-functions#avg



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#anyAtomicType

#### Returns

http://www.w3.org/2001/XMLSchema#anyAtomicType

## http://www.w3.org/2005/xpath-functions#base-uri



#### Arguments

1. arg1

#### Returns

http://www.w3.org/2001/XMLSchema#anyURI

## http://www.w3.org/2005/xpath-functions#boolean



#### Arguments

1. arg1

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://www.w3.org/2005/xpath-functions#ceiling



#### Arguments

1. arg1



## http://www.w3.org/2005/xpath-functions#codepoint-equal



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg2 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://www.w3.org/2005/xpath-functions#codepoints-to-string



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#integer

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://www.w3.org/2005/xpath-functions#collation-key



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg1 - http://www.w3.org/2001/XMLSchema#string
3. arg2 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#base64Binary

## http://www.w3.org/2005/xpath-functions#collection



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string



## http://www.w3.org/2005/xpath-functions#compare



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg1 - http://www.w3.org/2001/XMLSchema#string
3. arg2 - http://www.w3.org/2001/XMLSchema#string
4. arg2 - http://www.w3.org/2001/XMLSchema#string
5. arg3 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://www.w3.org/2005/xpath-functions#concat



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#anyAtomicType

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://www.w3.org/2005/xpath-functions#contains



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg1 - http://www.w3.org/2001/XMLSchema#string
3. arg2 - http://www.w3.org/2001/XMLSchema#string
4. arg2 - http://www.w3.org/2001/XMLSchema#string
5. arg3 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://www.w3.org/2005/xpath-functions#contains-token



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg1 - http://www.w3.org/2001/XMLSchema#string
3. arg2 - http://www.w3.org/2001/XMLSchema#string
4. arg2 - http://www.w3.org/2001/XMLSchema#string
5. arg3 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://www.w3.org/2005/xpath-functions#copy-of



#### Arguments

1. arg1



## http://www.w3.org/2005/xpath-functions#count



#### Arguments

1. arg1

#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://www.w3.org/2005/xpath-functions#current-date




#### Returns

http://www.w3.org/2001/XMLSchema#date

## http://www.w3.org/2005/xpath-functions#current-dateTime




#### Returns

http://www.w3.org/2001/XMLSchema#dateTime

## http://www.w3.org/2005/xpath-functions#current-time




#### Returns

http://www.w3.org/2001/XMLSchema#time

## http://www.w3.org/2005/xpath-functions#data



#### Arguments

1. arg1

#### Returns

http://www.w3.org/2001/XMLSchema#anyAtomicType

## http://www.w3.org/2005/xpath-functions#dateTime



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#date
2. arg2 - http://www.w3.org/2001/XMLSchema#time

#### Returns

http://www.w3.org/2001/XMLSchema#dateTime

## http://www.w3.org/2005/xpath-functions#day-from-date



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#date

#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://www.w3.org/2005/xpath-functions#day-from-dateTime



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#dateTime

#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://www.w3.org/2005/xpath-functions#days-from-duration



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#duration

#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://www.w3.org/2005/xpath-functions#deep-equal



#### Arguments

1. arg1
2. arg1
3. arg2
4. arg2
5. arg3 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://www.w3.org/2005/xpath-functions#default-collation




#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://www.w3.org/2005/xpath-functions#default-language




#### Returns

http://www.w3.org/2001/XMLSchema#language

## http://www.w3.org/2005/xpath-functions#distinct-values



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#anyAtomicType
2. arg1 - http://www.w3.org/2001/XMLSchema#anyAtomicType
3. arg2 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#anyAtomicType

## http://www.w3.org/2005/xpath-functions#doc



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string



## http://www.w3.org/2005/xpath-functions#doc-available



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://www.w3.org/2005/xpath-functions#document-uri



#### Arguments

1. arg1

#### Returns

http://www.w3.org/2001/XMLSchema#anyURI

## http://www.w3.org/2005/xpath-functions#element-with-id



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg1 - http://www.w3.org/2001/XMLSchema#string
3. arg2



## http://www.w3.org/2005/xpath-functions#empty



#### Arguments

1. arg1

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://www.w3.org/2005/xpath-functions#encode-for-uri



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://www.w3.org/2005/xpath-functions#ends-with



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg1 - http://www.w3.org/2001/XMLSchema#string
3. arg2 - http://www.w3.org/2001/XMLSchema#string
4. arg2 - http://www.w3.org/2001/XMLSchema#string
5. arg3 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://www.w3.org/2005/xpath-functions#environment-variable



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://www.w3.org/2005/xpath-functions#error



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#QName
2. arg1 - http://www.w3.org/2001/XMLSchema#QName
3. arg1 - http://www.w3.org/2001/XMLSchema#QName
4. arg2 - http://www.w3.org/2001/XMLSchema#string
5. arg2 - http://www.w3.org/2001/XMLSchema#string
6. arg3



## http://www.w3.org/2005/xpath-functions#escape-html-uri



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://www.w3.org/2005/xpath-functions#exactly-one



#### Arguments

1. arg1



## http://www.w3.org/2005/xpath-functions#exists



#### Arguments

1. arg1

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://www.w3.org/2005/xpath-functions#false




#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://www.w3.org/2005/xpath-functions#filter



#### Arguments

1. arg1
2. arg2



## http://www.w3.org/2005/xpath-functions#floor



#### Arguments

1. arg1



## http://www.w3.org/2005/xpath-functions#fold-left



#### Arguments

1. arg1
2. arg2
3. arg3



## http://www.w3.org/2005/xpath-functions#fold-right



#### Arguments

1. arg1
2. arg2
3. arg3



## http://www.w3.org/2005/xpath-functions#for-each



#### Arguments

1. arg1
2. arg2



## http://www.w3.org/2005/xpath-functions#for-each-pair



#### Arguments

1. arg1
2. arg2
3. arg3



## http://www.w3.org/2005/xpath-functions#format-date



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#date
2. arg1 - http://www.w3.org/2001/XMLSchema#date
3. arg2 - http://www.w3.org/2001/XMLSchema#string
4. arg2 - http://www.w3.org/2001/XMLSchema#string
5. arg3 - http://www.w3.org/2001/XMLSchema#string
6. arg4 - http://www.w3.org/2001/XMLSchema#string
7. arg5 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://www.w3.org/2005/xpath-functions#format-dateTime



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#dateTime
2. arg1 - http://www.w3.org/2001/XMLSchema#dateTime
3. arg2 - http://www.w3.org/2001/XMLSchema#string
4. arg2 - http://www.w3.org/2001/XMLSchema#string
5. arg3 - http://www.w3.org/2001/XMLSchema#string
6. arg4 - http://www.w3.org/2001/XMLSchema#string
7. arg5 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://www.w3.org/2005/xpath-functions#format-integer



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#integer
2. arg1 - http://www.w3.org/2001/XMLSchema#integer
3. arg2 - http://www.w3.org/2001/XMLSchema#string
4. arg2 - http://www.w3.org/2001/XMLSchema#string
5. arg3 - http://www.w3.org/2001/XMLSchema#string



## http://www.w3.org/2005/xpath-functions#format-number



#### Arguments

1. arg1
2. arg1
3. arg2 - http://www.w3.org/2001/XMLSchema#string
4. arg2 - http://www.w3.org/2001/XMLSchema#string
5. arg3 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://www.w3.org/2005/xpath-functions#format-time



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#time
2. arg1 - http://www.w3.org/2001/XMLSchema#time
3. arg2 - http://www.w3.org/2001/XMLSchema#string
4. arg2 - http://www.w3.org/2001/XMLSchema#string
5. arg3 - http://www.w3.org/2001/XMLSchema#string
6. arg4 - http://www.w3.org/2001/XMLSchema#string
7. arg5 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://www.w3.org/2005/xpath-functions#function-arity



#### Arguments

1. arg1

#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://www.w3.org/2005/xpath-functions#function-lookup



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#QName
2. arg2 - http://www.w3.org/2001/XMLSchema#integer



## http://www.w3.org/2005/xpath-functions#function-name



#### Arguments

1. arg1

#### Returns

http://www.w3.org/2001/XMLSchema#QName

## http://www.w3.org/2005/xpath-functions#generate-id



#### Arguments

1. arg1

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://www.w3.org/2005/xpath-functions#has-children



#### Arguments

1. arg1

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://www.w3.org/2005/xpath-functions#head



#### Arguments

1. arg1



## http://www.w3.org/2005/xpath-functions#hours-from-dateTime



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#dateTime

#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://www.w3.org/2005/xpath-functions#hours-from-duration



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#duration

#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://www.w3.org/2005/xpath-functions#hours-from-time



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#time

#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://www.w3.org/2005/xpath-functions#id



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg1 - http://www.w3.org/2001/XMLSchema#string
3. arg2



## http://www.w3.org/2005/xpath-functions#idref



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg1 - http://www.w3.org/2001/XMLSchema#string
3. arg2



## http://www.w3.org/2005/xpath-functions#implicit-timezone




#### Returns

http://www.w3.org/2001/XMLSchema#dayTimeDuration

## http://www.w3.org/2005/xpath-functions#in-scope-prefixes



#### Arguments

1. arg1

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://www.w3.org/2005/xpath-functions#index-of



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#anyAtomicType
2. arg1 - http://www.w3.org/2001/XMLSchema#anyAtomicType
3. arg2 - http://www.w3.org/2001/XMLSchema#anyAtomicType
4. arg2 - http://www.w3.org/2001/XMLSchema#anyAtomicType
5. arg3 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://www.w3.org/2005/xpath-functions#innermost



#### Arguments

1. arg1



## http://www.w3.org/2005/xpath-functions#insert-before



#### Arguments

1. arg1
2. arg2 - http://www.w3.org/2001/XMLSchema#integer
3. arg3



## http://www.w3.org/2005/xpath-functions#iri-to-uri



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://www.w3.org/2005/xpath-functions#json-doc



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg1 - http://www.w3.org/2001/XMLSchema#string
3. arg2



## http://www.w3.org/2005/xpath-functions#json-to-xml



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg1 - http://www.w3.org/2001/XMLSchema#string
3. arg2



## http://www.w3.org/2005/xpath-functions#lang



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg1 - http://www.w3.org/2001/XMLSchema#string
3. arg2

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://www.w3.org/2005/xpath-functions#last




#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://www.w3.org/2005/xpath-functions#load-xquery-module



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg1 - http://www.w3.org/2001/XMLSchema#string
3. arg2



## http://www.w3.org/2005/xpath-functions#local-name



#### Arguments

1. arg1

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://www.w3.org/2005/xpath-functions#local-name-from-QName



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#QName

#### Returns

http://www.w3.org/2001/XMLSchema#NCName

## http://www.w3.org/2005/xpath-functions#lower-case



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://www.w3.org/2005/xpath-functions#matches



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg1 - http://www.w3.org/2001/XMLSchema#string
3. arg2 - http://www.w3.org/2001/XMLSchema#string
4. arg2 - http://www.w3.org/2001/XMLSchema#string
5. arg3 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://www.w3.org/2005/xpath-functions#max



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#anyAtomicType
2. arg1 - http://www.w3.org/2001/XMLSchema#anyAtomicType
3. arg2 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#anyAtomicType

## http://www.w3.org/2005/xpath-functions#min



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#anyAtomicType
2. arg1 - http://www.w3.org/2001/XMLSchema#anyAtomicType
3. arg2 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#anyAtomicType

## http://www.w3.org/2005/xpath-functions#minutes-from-dateTime



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#dateTime

#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://www.w3.org/2005/xpath-functions#minutes-from-duration



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#duration

#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://www.w3.org/2005/xpath-functions#minutes-from-time



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#time

#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://www.w3.org/2005/xpath-functions#month-from-date



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#date

#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://www.w3.org/2005/xpath-functions#month-from-dateTime



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#dateTime

#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://www.w3.org/2005/xpath-functions#months-from-duration



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#duration

#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://www.w3.org/2005/xpath-functions#name



#### Arguments

1. arg1

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://www.w3.org/2005/xpath-functions#namespace-uri



#### Arguments

1. arg1

#### Returns

http://www.w3.org/2001/XMLSchema#anyURI

## http://www.w3.org/2005/xpath-functions#namespace-uri-for-prefix



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg2

#### Returns

http://www.w3.org/2001/XMLSchema#anyURI

## http://www.w3.org/2005/xpath-functions#namespace-uri-from-QName



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#QName

#### Returns

http://www.w3.org/2001/XMLSchema#anyURI

## http://www.w3.org/2005/xpath-functions#nilled



#### Arguments

1. arg1

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://www.w3.org/2005/xpath-functions#node-name



#### Arguments

1. arg1

#### Returns

http://www.w3.org/2001/XMLSchema#QName

## http://www.w3.org/2005/xpath-functions#normalize-space



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://www.w3.org/2005/xpath-functions#normalize-unicode



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg1 - http://www.w3.org/2001/XMLSchema#string
3. arg2 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://www.w3.org/2005/xpath-functions#not



#### Arguments

1. arg1

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://www.w3.org/2005/xpath-functions#number



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#anyAtomicType

#### Returns

http://www.w3.org/2001/XMLSchema#double

## http://www.w3.org/2005/xpath-functions#numeric-abs






## http://www.w3.org/2005/xpath-functions#numeric-ceil






## http://www.w3.org/2005/xpath-functions#numeric-floor






## http://www.w3.org/2005/xpath-functions#numeric-round






## http://www.w3.org/2005/xpath-functions#one-or-more



#### Arguments

1. arg1



## http://www.w3.org/2005/xpath-functions#outermost



#### Arguments

1. arg1



## http://www.w3.org/2005/xpath-functions#parse-ietf-date



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#dateTime

## http://www.w3.org/2005/xpath-functions#parse-json



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg1 - http://www.w3.org/2001/XMLSchema#string
3. arg2



## http://www.w3.org/2005/xpath-functions#parse-xml



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string



## http://www.w3.org/2005/xpath-functions#parse-xml-fragment



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string



## http://www.w3.org/2005/xpath-functions#path



#### Arguments

1. arg1

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://www.w3.org/2005/xpath-functions#position




#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://www.w3.org/2005/xpath-functions#prefix-from-QName



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#QName

#### Returns

http://www.w3.org/2001/XMLSchema#NCName

## http://www.w3.org/2005/xpath-functions#random-number-generator



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#anyAtomicType



## http://www.w3.org/2005/xpath-functions#remove



#### Arguments

1. arg1
2. arg2 - http://www.w3.org/2001/XMLSchema#integer



## http://www.w3.org/2005/xpath-functions#replace



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg1 - http://www.w3.org/2001/XMLSchema#string
3. arg2 - http://www.w3.org/2001/XMLSchema#string
4. arg2 - http://www.w3.org/2001/XMLSchema#string
5. arg3 - http://www.w3.org/2001/XMLSchema#string
6. arg3 - http://www.w3.org/2001/XMLSchema#string
7. arg4 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://www.w3.org/2005/xpath-functions#resolve-QName



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg2

#### Returns

http://www.w3.org/2001/XMLSchema#QName

## http://www.w3.org/2005/xpath-functions#resolve-uri



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg1 - http://www.w3.org/2001/XMLSchema#string
3. arg2 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#anyURI

## http://www.w3.org/2005/xpath-functions#reverse



#### Arguments

1. arg1



## http://www.w3.org/2005/xpath-functions#root



#### Arguments

1. arg1



## http://www.w3.org/2005/xpath-functions#round



#### Arguments

1. arg1
2. arg1
3. arg2 - http://www.w3.org/2001/XMLSchema#integer



## http://www.w3.org/2005/xpath-functions#round-half-to-even



#### Arguments

1. arg1
2. arg1
3. arg2 - http://www.w3.org/2001/XMLSchema#integer



## http://www.w3.org/2005/xpath-functions#seconds-from-dateTime



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#dateTime

#### Returns

http://www.w3.org/2001/XMLSchema#decimal

## http://www.w3.org/2005/xpath-functions#seconds-from-duration



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#duration

#### Returns

http://www.w3.org/2001/XMLSchema#decimal

## http://www.w3.org/2005/xpath-functions#seconds-from-time



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#time

#### Returns

http://www.w3.org/2001/XMLSchema#decimal

## http://www.w3.org/2005/xpath-functions#serialize



#### Arguments

1. arg1
2. arg1
3. arg2

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://www.w3.org/2005/xpath-functions#snapshot



#### Arguments

1. arg1



## http://www.w3.org/2005/xpath-functions#sort



#### Arguments

1. arg1
2. arg1
3. arg1
4. arg2 - http://www.w3.org/2001/XMLSchema#string
5. arg2 - http://www.w3.org/2001/XMLSchema#string
6. arg3



## http://www.w3.org/2005/xpath-functions#starts-with



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg1 - http://www.w3.org/2001/XMLSchema#string
3. arg2 - http://www.w3.org/2001/XMLSchema#string
4. arg2 - http://www.w3.org/2001/XMLSchema#string
5. arg3 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://www.w3.org/2005/xpath-functions#static-base-uri




#### Returns

http://www.w3.org/2001/XMLSchema#anyURI

## http://www.w3.org/2005/xpath-functions#string



#### Arguments

1. arg1

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://www.w3.org/2005/xpath-functions#string-join



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#anyAtomicType
2. arg1 - http://www.w3.org/2001/XMLSchema#anyAtomicType
3. arg2 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://www.w3.org/2005/xpath-functions#string-length



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://www.w3.org/2005/xpath-functions#string-to-codepoints



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://www.w3.org/2005/xpath-functions#subsequence



#### Arguments

1. arg1
2. arg1
3. arg2
4. arg2
5. arg3



## http://www.w3.org/2005/xpath-functions#substring



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg1 - http://www.w3.org/2001/XMLSchema#string
3. arg2
4. arg2
5. arg3

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://www.w3.org/2005/xpath-functions#substring-after



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg1 - http://www.w3.org/2001/XMLSchema#string
3. arg2 - http://www.w3.org/2001/XMLSchema#string
4. arg2 - http://www.w3.org/2001/XMLSchema#string
5. arg3 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://www.w3.org/2005/xpath-functions#substring-before



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg1 - http://www.w3.org/2001/XMLSchema#string
3. arg2 - http://www.w3.org/2001/XMLSchema#string
4. arg2 - http://www.w3.org/2001/XMLSchema#string
5. arg3 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://www.w3.org/2005/xpath-functions#sum



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#anyAtomicType
2. arg1 - http://www.w3.org/2001/XMLSchema#anyAtomicType
3. arg2 - http://www.w3.org/2001/XMLSchema#anyAtomicType

#### Returns

http://www.w3.org/2001/XMLSchema#anyAtomicType

## http://www.w3.org/2005/xpath-functions#tail



#### Arguments

1. arg1



## http://www.w3.org/2005/xpath-functions#timezone-from-date



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#date

#### Returns

http://www.w3.org/2001/XMLSchema#dayTimeDuration

## http://www.w3.org/2005/xpath-functions#timezone-from-dateTime



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#dateTime

#### Returns

http://www.w3.org/2001/XMLSchema#dayTimeDuration

## http://www.w3.org/2005/xpath-functions#timezone-from-time



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#time

#### Returns

http://www.w3.org/2001/XMLSchema#dayTimeDuration

## http://www.w3.org/2005/xpath-functions#tokenize



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg1 - http://www.w3.org/2001/XMLSchema#string
3. arg1 - http://www.w3.org/2001/XMLSchema#string
4. arg2 - http://www.w3.org/2001/XMLSchema#string
5. arg2 - http://www.w3.org/2001/XMLSchema#string
6. arg3 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://www.w3.org/2005/xpath-functions#trace



#### Arguments

1. arg1
2. arg1
3. arg2 - http://www.w3.org/2001/XMLSchema#string



## http://www.w3.org/2005/xpath-functions#transform



#### Arguments

1. arg1



## http://www.w3.org/2005/xpath-functions#translate



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg2 - http://www.w3.org/2001/XMLSchema#string
3. arg3 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://www.w3.org/2005/xpath-functions#true




#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://www.w3.org/2005/xpath-functions#unordered



#### Arguments

1. arg1



## http://www.w3.org/2005/xpath-functions#unparsed-text



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg1 - http://www.w3.org/2001/XMLSchema#string
3. arg2 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://www.w3.org/2005/xpath-functions#unparsed-text-available



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg1 - http://www.w3.org/2001/XMLSchema#string
3. arg2 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#boolean

## http://www.w3.org/2005/xpath-functions#unparsed-text-lines



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string
2. arg1 - http://www.w3.org/2001/XMLSchema#string
3. arg2 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://www.w3.org/2005/xpath-functions#upper-case



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://www.w3.org/2005/xpath-functions#uri-collection



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#string

#### Returns

http://www.w3.org/2001/XMLSchema#anyURI

## http://www.w3.org/2005/xpath-functions#xml-to-json



#### Arguments

1. arg1
2. arg1
3. arg2

#### Returns

http://www.w3.org/2001/XMLSchema#string

## http://www.w3.org/2005/xpath-functions#year-from-date



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#date

#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://www.w3.org/2005/xpath-functions#year-from-dateTime



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#dateTime

#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://www.w3.org/2005/xpath-functions#years-from-duration



#### Arguments

1. arg1 - http://www.w3.org/2001/XMLSchema#duration

#### Returns

http://www.w3.org/2001/XMLSchema#integer

## http://www.w3.org/2005/xpath-functions#zero-or-one



#### Arguments

1. arg1



# Aggregate functions

## http://merck.github.io/Halyard/ns#groupIntoArrays






## http://merck.github.io/Halyard/ns#groupIntoTuples






## http://merck.github.io/Halyard/ns#hIndex






## http://merck.github.io/Halyard/ns#maxWith






## http://merck.github.io/Halyard/ns#minWith






## http://merck.github.io/Halyard/ns#mode






## http://merck.github.io/Halyard/ns#topNWith






## http://rdf4j.org/aggregate#stdev






## http://rdf4j.org/aggregate#stdev_population






## http://rdf4j.org/aggregate#variance






## http://rdf4j.org/aggregate#variance_population






