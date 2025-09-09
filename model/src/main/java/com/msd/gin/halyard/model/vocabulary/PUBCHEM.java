package com.msd.gin.halyard.model.vocabulary;

import java.nio.ByteBuffer;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Vocabulary.class)
public final class PUBCHEM implements Vocabulary {
    private static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();

    public static final String PREFIX = "vocab";

    public static final String NAMESPACE = "http://rdf.ncbi.nlm.nih.gov/pubchem/vocabulary#";

    public static final Namespace NS = new SimpleNamespace(PREFIX, NAMESPACE);
    public static final Namespace ANATOMYID_NS = new PrefixedIntegerNamespace("pubchem_anatomy", "http://rdf.ncbi.nlm.nih.gov/pubchem/anatomy/", "ANATOMYID");
    public static final Namespace AUTHOR_NS = new MD5Namespace("pubchem_author", "http://rdf.ncbi.nlm.nih.gov/pubchem/author/");
    public static final Namespace REFERENCE_NS = new IntegerNamespace("pubchem_ref", "http://rdf.ncbi.nlm.nih.gov/pubchem/reference/");
    public static final Namespace ORGANIZATION_NS = new MD5Namespace("pubchem_org", "http://rdf.ncbi.nlm.nih.gov/pubchem/organization/");
    public static final Namespace COMPOUND_NS = new PrefixedIntegerNamespace("pubchem_compound", "http://rdf.ncbi.nlm.nih.gov/pubchem/compound/", "CID");
    public static final Namespace DESCRIPTOR_NS = new SimpleNamespace("pubchem_desc", "http://rdf.ncbi.nlm.nih.gov/pubchem/descriptor/");
    public static final Namespace DISEASE_NS = new PrefixedIntegerNamespace("pubchem_disease", "http://rdf.ncbi.nlm.nih.gov/pubchem/disease/", "DZID");
    public static final Namespace SOURCE_NS = new PrefixedIntegerNamespace("pubchem_src", "http://rdf.ncbi.nlm.nih.gov/pubchem/source/", "ID");
    public static final Namespace SUBSTANCE_NS = new PrefixedIntegerNamespace("pubchem_substance", "http://rdf.ncbi.nlm.nih.gov/pubchem/substance/", "SID");
    public static final Namespace SYNONYM_NS = new MD5Namespace("pubchem_synonym", "http://rdf.ncbi.nlm.nih.gov/pubchem/synonym/");
    public static final Namespace COOCCURRENCE_NS = new SimpleNamespace("pubchem_coo", "http://rdf.ncbi.nlm.nih.gov/pubchem/cooccurrence/");
    public static final Namespace PATENT_NS = new SimpleNamespace("pubchem_patent", "http://rdf.ncbi.nlm.nih.gov/pubchem/patent/");
    public static final Namespace PROTEIN_NS = new SimpleNamespace("pubchem_protein", "http://rdf.ncbi.nlm.nih.gov/pubchem/protein/");
    public static final Namespace TAXONOMY_NS = new PrefixedIntegerNamespace("pubchem_tax", "http://rdf.ncbi.nlm.nih.gov/pubchem/taxonomy/", "TAXID");

    public static final IRI ANATOMY_CLASS = SVF.createIRI(NAMESPACE, "Anatomy");
    public static final IRI AUTHOR_CLASS = SVF.createIRI(NAMESPACE, "Author");
    public static final IRI COMPOUND_CLASS = SVF.createIRI(NAMESPACE, "Compound");
    public static final IRI COOCCURRENCE_CLASS = SVF.createIRI(NAMESPACE, "Cooccurrence");
    public static final IRI DESCRIPTOR_CLASS = SVF.createIRI(NAMESPACE, "Descriptor");
    public static final IRI ORGANIZATION_CLASS = SVF.createIRI(NAMESPACE, "Organization");
    public static final IRI PATENT_CLASS = SVF.createIRI(NAMESPACE, "Patent");
    public static final IRI PROTEIN_CLASS = SVF.createIRI(NAMESPACE, "Protein");
    public static final IRI REFERENCE_CLASS = SVF.createIRI(NAMESPACE, "Reference");
    public static final IRI SOURCE_CLASS = SVF.createIRI(NAMESPACE, "Source");
    public static final IRI SUBSTANCE_CLASS = SVF.createIRI(NAMESPACE, "Substance");
    public static final IRI SYNONYM_CLASS = SVF.createIRI(NAMESPACE, "Synonym");
    public static final IRI TAXONOMY_CLASS = SVF.createIRI(NAMESPACE, "Taxonomy");

    public static final IRI DISCUSSES_AS_DERIVED_BY_TEXT_MINING = SVF.createIRI(NAMESPACE, "discussesAsDerivedByTextMining");


    static final class MD5Namespace extends AbstractIRIEncodingNamespace {
    	private static final long serialVersionUID = -4991612855175764488L;

    	private static final String MD5_PREFIX = "MD5_";
    	private static final String ORCID_PREFIX = "ORCID_";
    	private final HashNamespace hash;
    	private final OrcidNamespace orcid;

		public MD5Namespace(String prefix, String name) {
			super(prefix, name);
			hash = new HashNamespace(prefix, name);
			orcid = new OrcidNamespace(prefix, name);
		}

		@Override
		public ByteBuffer writeBytes(String localName, ByteBuffer b) {
			if (localName.startsWith(MD5_PREFIX)) {
				b.put((byte)0);
				hash.writeBytes(localName.substring(MD5_PREFIX.length()), b);
			} else if (localName.startsWith(ORCID_PREFIX)) {
				b.put((byte)1);
				orcid.writeBytes(localName.substring(ORCID_PREFIX.length()), b);
			} else {
				throw new IllegalArgumentException(String.format("Unsupported local name: %s", localName));
			}
			return b;
		}

		@Override
		public String readBytes(ByteBuffer b) {
			int type = b.get();
			switch (type) {
				case 0:
					return MD5_PREFIX + hash.readBytes(b);
				case 1:
					return ORCID_PREFIX + orcid.readBytes(b);
				default:
					throw new AssertionError(String.format("Unexpected type: %d", type));
			}
		}
    }
}
