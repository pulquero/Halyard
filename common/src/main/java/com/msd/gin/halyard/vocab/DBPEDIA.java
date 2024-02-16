package com.msd.gin.halyard.vocab;

import com.msd.gin.halyard.model.Vocabulary;

import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Vocabulary.class)
public final class DBPEDIA implements Vocabulary {
	public static final String ONTOLOGY_NAMESPACE = "https://dbpedia.org/ontology/";
	public static final String PROPERTY_NAMESPACE = "https://dbpedia.org/property/";
	public static final String RESOURCE_NAMESPACE = "https://dbpedia.org/resource/";
	public static final String RESOURCE_TEMPLATE_NAMESPACE = "https://dbpedia.org/resource/Template:";
	public static final String YAGO_NAMESPACE = "https://dbpedia.org/class/yago/";
	public static final Namespace ONTOLOGY_NS = new SimpleNamespace("dbo", ONTOLOGY_NAMESPACE);
	public static final Namespace PROPERTY_NS = new SimpleNamespace("dbp", PROPERTY_NAMESPACE);
	public static final Namespace RESOURCE_NS = new SimpleNamespace("dbr", RESOURCE_NAMESPACE);
	public static final Namespace RESOURCE_TEMPLATE_NS = new SimpleNamespace("dbt", RESOURCE_TEMPLATE_NAMESPACE);
	public static final Namespace YAGO_NS = new SimpleNamespace("yago", YAGO_NAMESPACE);
}
