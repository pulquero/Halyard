package com.msd.gin.halyard.model.vocabulary;

import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Vocabulary.class)
public final class IDENTIFIERS_ORG implements Vocabulary {
    public static final String BASE_NAMESPACE = "http://identifiers.org/";

    // URIs (https://docs.identifiers.org/pages/resolving_mechanisms.html)
    public static final Namespace INTACT_NS = new SimpleNamespace("id_intact", BASE_NAMESPACE+"intact/");
    public static final Namespace MESH_NS = new SimpleNamespace("id_mesh", BASE_NAMESPACE+"mesh/");
    public static final Namespace OBO_NS = new SimpleNamespace("id_obo", BASE_NAMESPACE+"obo.go/");
    public static final Namespace PDB_NS = new SimpleNamespace("id_pdb", BASE_NAMESPACE+"pdb/");
    public static final Namespace PUBMED_NS = new IntegerNamespace("id_pm", BASE_NAMESPACE+"pubmed/");
    public static final Namespace REACTOME_NS = new SimpleNamespace("id_react", BASE_NAMESPACE+"reactome/");
    public static final Namespace TAXONOMY_NS = new IntegerNamespace("id_tax", BASE_NAMESPACE+"taxonomy/");

    // URLs (https://docs.identifiers.org/pages/identification_scheme.html)
    public static final Namespace _COL_NS = new SimpleNamespace("id_col_", BASE_NAMESPACE+"col:");
    public static final Namespace _MESH_NS = new SimpleNamespace("id_mesh_", BASE_NAMESPACE+"mesh:");
    public static final Namespace _NCIT_NS = new SimpleNamespace("id_ncit_", BASE_NAMESPACE+"ncit:");
    public static final Namespace _TAXONOMY_NS = new SimpleNamespace("id_tax_", BASE_NAMESPACE+"taxonomy:");
}
