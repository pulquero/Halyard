package com.msd.gin.halyard.model.vocabulary;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Vocabulary.class)
public final class SPAR implements Vocabulary {
	private static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();

	public static final String BIDO_NAMESPACE = "http://purl.org/spar/bido/";
	public static final Namespace BIDO_NS = new SimpleNamespace("bido", BIDO_NAMESPACE);

	public static final String CITO_NAMESPACE = "http://purl.org/spar/cito/";
	public static final Namespace CITO_NS = new SimpleNamespace("cito", CITO_NAMESPACE);

	public static final IRI IS_DISCUSSED_BY = SVF.createIRI(CITO_NAMESPACE, "isDiscussedBy");

	public static final String FABIO_NAMESPACE = "http://purl.org/spar/fabio/";
	public static final Namespace FABIO_NS = new SimpleNamespace("fabio", FABIO_NAMESPACE);
}
