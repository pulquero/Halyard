package com.msd.gin.halyard.model.vocabulary;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.kohsuke.MetaInfServices;

/**
 * http://jena.apache.org/ARQ/property#.
 */
@MetaInfServices(Vocabulary.class)
public final class APF implements Vocabulary {
	private static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();

	/**
	 * http://jena.apache.org/ARQ/property#
	 */
	public static final String NAMESPACE = "http://jena.apache.org/ARQ/property#";

	public static final String PREFIX = "apf";

	public static final Namespace NS = new SimpleNamespace(PREFIX, NAMESPACE);
	public static final IRI STR_SPLIT = SVF.createIRI(NAMESPACE, "strSplit");
	public static final IRI CONCAT = SVF.createIRI(NAMESPACE, "concat");
	public static final IRI SPLIT_IRI = SVF.createIRI(NAMESPACE, "splitIRI");
}
