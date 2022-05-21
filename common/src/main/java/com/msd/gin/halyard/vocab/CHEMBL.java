package com.msd.gin.halyard.vocab;

import com.msd.gin.halyard.common.Vocabulary;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Vocabulary.class)
public final class CHEMBL implements Vocabulary {
    private static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();

    public static final String PREFIX = "cco";

    public static final String NAMESPACE = "http://rdf.ebi.ac.uk/terms/chembl#";

    public static final Namespace NS = new SimpleNamespace(PREFIX, NAMESPACE);
    public static final Namespace ACTIVITY_NS = new SimpleNamespace("ch_act", "http://rdf.ebi.ac.uk/resource/chembl/activity/");
    public static final Namespace ASSAY_NS = new SimpleNamespace("ch_assay", "http://rdf.ebi.ac.uk/resource/chembl/assay/");
    public static final Namespace DOCUMENT_NS = new SimpleNamespace("ch_doc", "http://rdf.ebi.ac.uk/resource/chembl/document/");
    public static final Namespace MOLECULE_NS = new SimpleNamespace("ch_mole", "http://rdf.ebi.ac.uk/resource/chembl/molecule/");
    public static final Namespace BAO_NS = new SimpleNamespace("bao", "http://www.bioassayontology.org/bao#");
    public static final Namespace BIBO_NS = new SimpleNamespace("bibo", "http://purl.org/ontology/bibo/");
    public static final Namespace ID_NS = new SimpleNamespace("id", "http://identifiers.org/");
    public static final Namespace QUDT_NS = new SimpleNamespace("qudt", "http://qudt.org/vocab/unit#");
    public static final Namespace OPS_NS = new SimpleNamespace("ops", "http://www.openphacts.org/units/");
    public static final Namespace CLO_NS = new SimpleNamespace("clo", "http://purl.obolibrary.org/obo/");
    public static final Namespace EFO_NS = new SimpleNamespace("efo", "http://www.ebi.ac.uk/efo/");
    public static final Namespace PUBCHEM_NS = new SimpleNamespace("pubchem_c", "http://pubchem.ncbi.nlm.nih.gov/compound/");
    public static final Namespace SEMSCI_NS = new SimpleNamespace("semsci", "http://semanticscience.org/resource/");

    public static final IRI HAS_DOCUMENT = SVF.createIRI(NAMESPACE, "hasDocument");
    public static final IRI HAS_ACTIVITY = SVF.createIRI(NAMESPACE, "hasActivity");
}
