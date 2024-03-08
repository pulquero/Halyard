package com.msd.gin.halyard.model.vocabulary;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Vocabulary.class)
public final class GRID implements Vocabulary {
    private static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();

    public static final String RESOURCE_PREFIX = "grid";

    public static final String RESOURCE_NAMESPACE = "http://www.grid.ac/institutes/";

    public static final Namespace RESOURCE_NS = new SimpleNamespace(RESOURCE_PREFIX, RESOURCE_NAMESPACE);

    public static final String ROR_PREFIX = "ror";

    public static final String ROR_NAMESPACE = "https://ror.org/";

    public static final Namespace ROR_NS = new SimpleNamespace(ROR_PREFIX, ROR_NAMESPACE);

    public static final String PREFIX = "grid_ont";

    public static final String NAMESPACE = "http://www.grid.ac/ontology/";

    public static final Namespace NS = new SimpleNamespace(PREFIX, NAMESPACE);

    public static final IRI ADDRESS = SVF.createIRI(NAMESPACE, "Address");
    public static final IRI ARCHIVE = SVF.createIRI(NAMESPACE, "Archive");
    public static final IRI COMPANY = SVF.createIRI(NAMESPACE, "Company");
    public static final IRI EDUCATION = SVF.createIRI(NAMESPACE, "Education");
    public static final IRI FACILITY = SVF.createIRI(NAMESPACE, "Facility");
    public static final IRI GOVERNMENT = SVF.createIRI(NAMESPACE, "Government");
    public static final IRI HEALTHCARE = SVF.createIRI(NAMESPACE, "Healthcare");
    public static final IRI NON_PROFIT = SVF.createIRI(NAMESPACE, "Nonprofit");
    public static final IRI ORGANISATION = SVF.createIRI(NAMESPACE, "Organisation");
    public static final IRI OTHER = SVF.createIRI(NAMESPACE, "Other");

    public static final IRI HAS_ADDDRESS = SVF.createIRI(NAMESPACE, "hasAddress");
    public static final IRI HAS_CHILD = SVF.createIRI(NAMESPACE, "hasChild");
    public static final IRI HAS_GEONAMES_CITY = SVF.createIRI(NAMESPACE, "hasGeonamesCity");
    public static final IRI HAS_PARENT = SVF.createIRI(NAMESPACE, "hasParent");
    public static final IRI HAS_RELATED = SVF.createIRI(NAMESPACE, "hasRelated");
    public static final IRI HAS_WIKIDATA_ID = SVF.createIRI(NAMESPACE, "hasWikidataID");
    public static final IRI CITY_NAME = SVF.createIRI(NAMESPACE, "cityName");
    public static final IRI COUNTRY_CODE = SVF.createIRI(NAMESPACE, "countryCode");
    public static final IRI COUNTRY_NAME = SVF.createIRI(NAMESPACE, "countryName");
    public static final IRI ESTABLISHED_YEAR = SVF.createIRI(NAMESPACE, "establishedYear");
    public static final IRI HAS_CROSSREF_FUNDER_ID = SVF.createIRI(NAMESPACE, "hasCrossrefFunderID");
    public static final IRI HAS_ISNI = SVF.createIRI(NAMESPACE, "hasISNI");
    public static final IRI HAS_UKPRN = SVF.createIRI(NAMESPACE, "hasUKPRN");
    public static final IRI ID = SVF.createIRI(NAMESPACE, "id");
    public static final IRI WIKIPEDIA_PAGE = SVF.createIRI(NAMESPACE, "wikipediaPage");
}
