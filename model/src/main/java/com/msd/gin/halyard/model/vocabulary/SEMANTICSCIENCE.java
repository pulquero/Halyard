package com.msd.gin.halyard.model.vocabulary;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Vocabulary.class)
public final class SEMANTICSCIENCE implements Vocabulary {
    private static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();

    public static final String PREFIX = "sio";

    public static final String NAMESPACE = "http://semanticscience.org/resource/";

    public static final Namespace NS = new SimpleNamespace(PREFIX, NAMESPACE);

    public static final IRI HAS_ATTRIBUTE = SVF.createIRI(NAMESPACE, "SIO_000008");
    public static final IRI IS_ATTRIBUTE_OF = SVF.createIRI(NAMESPACE, "SIO_000011");
    public static final IRI HAS_UNIT = SVF.createIRI(NAMESPACE, "SIO_000221");
    public static final IRI HAS_VALUE = SVF.createIRI(NAMESPACE, "SIO_000300");

    public static final IRI IS_ISOTOPOLOGUE_OF = SVF.createIRI(NAMESPACE, "CHEMINF_000455");
    public static final IRI HAS_PUBCHEM_NORMALIZED_COUNTERPART = SVF.createIRI(NAMESPACE, "CHEMINF_000477");
}
