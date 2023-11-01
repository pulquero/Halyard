package com.msd.gin.halyard.vocab;

import com.msd.gin.halyard.common.Vocabulary;

import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Vocabulary.class)
public final class SEMOPENALEX implements Vocabulary {
	public static final String CLASS_NAMESPACE = "https://semopenalex.org/class/";
	public static final String PROPERTY_NAMESPACE = "https://semopenalex.org/property/";
	public static final String CONCEPT_NAMESPACE = "https://semopenalex.org/concept/";
	public static final String WORK_NAMESPACE = "https://semopenalex.org/work/";
	public static final String AUTHOR_NAMESPACE = "https://semopenalex.org/author/";
	public static final String INSTITUTION_NAMESPACE = "https://semopenalex.org/institution/";
	public static final String AUTHOR_POSITION_NAMESPACE = "https://semopenalex.org/authorposition/";
	public static final String CONCEPT_SCORE_NAMESPACE = "https://semopenalex.org/conceptscore/";
	public static final String COUNTS_BY_YEAR_NAMESPACE = "https://semopenalex.org/countsbyyear/";
	public static final String LOCATION_NAMESPACE = "https://semopenalex.org/location/";
	public static final Namespace CLASS_NS = new SimpleNamespace("soa_class", CLASS_NAMESPACE);
	public static final Namespace PROPERTY_NS = new SimpleNamespace("soa_prop", PROPERTY_NAMESPACE);
	public static final Namespace CONCEPT_NS = new PrefixedIntegerNamespace("soa_concept", CONCEPT_NAMESPACE, "C");
	public static final Namespace WORK_NS = new PrefixedIntegerNamespace("soa_work", WORK_NAMESPACE, "W");
	public static final Namespace AUTHOR_NS = new PrefixedIntegerNamespace("soa_auth", AUTHOR_NAMESPACE, "A");
	public static final Namespace INSTITUTION_NS = new PrefixedIntegerNamespace("soa_inst", INSTITUTION_NAMESPACE, "I");
	public static final Namespace AUTHOR_POSITION_NS = new IntegerPairNamespace("soa_ap", AUTHOR_POSITION_NAMESPACE, "W", "A");
	public static final Namespace CONCEPT_SCORE_NS = new IntegerPairNamespace("soa_cs", CONCEPT_SCORE_NAMESPACE, "W", "C");
	public static final Namespace COUNTS_BY_YEAR_NS = new PrefixedIntegerNamespace("soa_cby", COUNTS_BY_YEAR_NAMESPACE, "P");
	public static final Namespace LOCATION_NS = new PrefixedIntegerNamespace("soa_loc", LOCATION_NAMESPACE, "W");
}
