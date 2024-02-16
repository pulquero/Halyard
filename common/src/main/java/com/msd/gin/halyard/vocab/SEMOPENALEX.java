package com.msd.gin.halyard.vocab;

import com.msd.gin.halyard.common.ValueIO;
import com.msd.gin.halyard.model.Vocabulary;

import java.nio.ByteBuffer;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Vocabulary.class)
public final class SEMOPENALEX implements Vocabulary {
    private static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();

    public static final String NAMESPACE = "https://semopenalex.org/ontology/";
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
	public static final Namespace NS = new SimpleNamespace("soa", NAMESPACE);
	public static final Namespace CLASS_NS = new SimpleNamespace("soa_class", CLASS_NAMESPACE);
	public static final Namespace PROPERTY_NS = new SimpleNamespace("soa_prop", PROPERTY_NAMESPACE);
	public static final Namespace CONCEPT_NS = new PrefixedIntegerNamespace("soa_concept", CONCEPT_NAMESPACE, "C");
	public static final Namespace WORK_NS = new PrefixedIntegerNamespace("soa_work", WORK_NAMESPACE, "W");
	public static final Namespace AUTHOR_NS = new PrefixedIntegerNamespace("soa_auth", AUTHOR_NAMESPACE, "A");
	public static final Namespace INSTITUTION_NS = new PrefixedIntegerNamespace("soa_inst", INSTITUTION_NAMESPACE, "I");
	public static final Namespace AUTHOR_POSITION_NS = new IntegerPairNamespace("soa_ap", AUTHOR_POSITION_NAMESPACE, "W", "A");
	public static final Namespace CONCEPT_SCORE_NS = new IntegerPairNamespace("soa_cs", CONCEPT_SCORE_NAMESPACE, "W", "C");
	public static final Namespace COUNTS_BY_YEAR_NS = new AlphaIntegerNamespace("soa_cby", COUNTS_BY_YEAR_NAMESPACE);
	public static final Namespace LOCATION_NS = new PrefixedIntegerNamespace("soa_loc", LOCATION_NAMESPACE, "W");

	public static final IRI AUTHOR_CLASS = SVF.createIRI(NAMESPACE, "Author");
	public static final IRI CONCEPT_SCORE_CLASS = SVF.createIRI(NAMESPACE, "ConceptScore");
	public static final IRI COUNTS_BY_YEAR_CLASS = SVF.createIRI(NAMESPACE, "CountsByYear");
	public static final IRI INSTITUTION_CLASS = SVF.createIRI(NAMESPACE, "Institution");
	public static final IRI WORK_CLASS = SVF.createIRI(NAMESPACE, "Work");
	public static final IRI COUNTS_BY_YEAR = SVF.createIRI(NAMESPACE, "countsByYear");
	public static final IRI HAS_AUTHORSHIP = SVF.createIRI(NAMESPACE, "hasAuthorship");
	public static final IRI HAS_CONCEPT = SVF.createIRI(NAMESPACE, "hasConcept");
	public static final IRI HAS_CONCEPT_SCORE = SVF.createIRI(NAMESPACE, "hasConceptScore");
	public static final IRI HAS_RELATED_WORK = SVF.createIRI(NAMESPACE, "hasRelatedWork");
	public static final IRI SCORE = SVF.createIRI(NAMESPACE, "score");


	static final class AlphaIntegerNamespace extends IntegerNamespace {
		private static final long serialVersionUID = -1687678822966929087L;

		public AlphaIntegerNamespace(String prefix, String ns) {
			super(prefix, ns);
		}

		@Override
		public ByteBuffer writeBytes(String localName, ByteBuffer b) {
			char alpha = localName.charAt(0);
			b = ValueIO.ensureCapacity(b, 1);
			b.put((byte)alpha);
			return super.writeBytes(localName.substring(1), b);
		}

		@Override
		public String readBytes(ByteBuffer b) {
			char alpha = (char) b.get();
			return alpha + super.readBytes(b);
		}
	}
}

