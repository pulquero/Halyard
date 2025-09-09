package com.msd.gin.halyard.model.vocabulary;

import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Vocabulary.class)
public final class PRISM_30 implements Vocabulary {
	public static final String NAMESPACE = "http://prismstandard.org/namespaces/basic/3.0/";
	public static final Namespace NS = new SimpleNamespace("prism_30", NAMESPACE);
}
