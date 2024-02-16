package com.msd.gin.halyard.vocab;

import com.msd.gin.halyard.model.Vocabulary;

import org.eclipse.rdf4j.model.Namespace;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Vocabulary.class)
public final class UUID implements Vocabulary {
    public static final String PREFIX = "uuid";

    public static final String NAMESPACE = "urn:uuid:";

    public static final Namespace NS = new UUIDNamespace(PREFIX, NAMESPACE);
}
