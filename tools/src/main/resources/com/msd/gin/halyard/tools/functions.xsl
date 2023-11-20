<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="3.0"
 xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
 xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#"
 xmlns:spin="http://spinrdf.org/spin#"
 xmlns:spl="http://spinrdf.org/spl#"
 xmlns:xsd="http://www.w3.org/2001/XMLSchema"
 xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

<xsl:output method="text"/>

<xsl:variable name="funcTypes" as="xsd:string*" select="('http://www.w3.org/ns/sparql-service-description#Function', 'http://spinrdf.org/spin#Function')"/>
<xsl:variable name="descs" select="/rdf:RDF/rdf:Description"/>
<xsl:variable name="funcDescs" select="$descs[rdf:type/@rdf:resource=$funcTypes]"/>
<xsl:variable name="aggFuncDescs" select="$descs[rdf:type/@rdf:resource='http://www.w3.org/ns/sparql-service-description#Aggregate']"/>
<xsl:variable name="argDescs" select="$descs[rdf:type/@rdf:resource='http://spinrdf.org/spl#Argument']"/>

<xsl:template match="/">
<xsl:text># Functions

</xsl:text>

<xsl:variable name="funcIris" as="xsd:string*"
select="distinct-values($funcDescs/@rdf:about)"/>

<xsl:for-each select="$funcIris">
<xsl:sort/>
<xsl:call-template name="function">
<xsl:with-param name="iri" select="."/>
</xsl:call-template>
</xsl:for-each>

<xsl:text># Aggregate functions

</xsl:text>

<xsl:variable name="aggFuncIris" as="xsd:string*"
select="distinct-values($aggFuncDescs/@rdf:about)"/>

<xsl:for-each select="$aggFuncIris">
<xsl:sort/>
<xsl:call-template name="function">
<xsl:with-param name="iri" select="."/>
</xsl:call-template>
</xsl:for-each>
</xsl:template>

<xsl:template name="function">
<xsl:param name="iri" required="yes" as="xsd:string"/>
<xsl:text>## </xsl:text><xsl:value-of select="if (starts-with($iri, 'builtin:')) then substring-after($iri, 'builtin:') else $iri"/><xsl:text>

</xsl:text>
<xsl:variable name="desc" select="$descs[@rdf:about=$iri]"/>
<xsl:value-of select="$desc/rdfs:comment"/><xsl:text>

</xsl:text>
<xsl:variable name="args" select="$argDescs[@rdf:nodeID=$desc/spin:constraint/@rdf:nodeID]"/>
<xsl:if test="count($args) > 0">
<xsl:text>#### Arguments

</xsl:text>
<xsl:for-each select="$args">
<xsl:sort select="spl:predicate/@rdf:resource"/>
<xsl:value-of select="position()"/><xsl:text>. </xsl:text>
<xsl:value-of select="substring-after(spl:predicate/@rdf:resource, '#')"/>
<xsl:if test="spl:valueType">
<xsl:text> - </xsl:text><xsl:value-of select="spl:valueType/@rdf:resource"/>
</xsl:if>
<xsl:text>
</xsl:text>
</xsl:for-each>
</xsl:if>
<xsl:text>
</xsl:text>
<xsl:if test="$desc/spin:returnType/@rdf:resource">
<xsl:text>#### Returns

</xsl:text>
<xsl:value-of select="$desc/spin:returnType/@rdf:resource"/>
</xsl:if>
<xsl:text>

</xsl:text>
</xsl:template>

</xsl:stylesheet>