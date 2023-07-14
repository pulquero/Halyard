package com.msd.gin.halyard.common;

import com.msd.gin.halyard.vocab.HALYARD;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.base.AbstractValueFactory;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.vocabulary.GEO;
import org.eclipse.rdf4j.model.vocabulary.RDF;

public final class AdvancedValueFactory extends AbstractValueFactory {
	private Literal createAdvancedLiteral(String label, IRI datatype, CoreDatatype coreDatatype) {
		try {
			if (coreDatatype == CoreDatatype.GEO.WKT_LITERAL || GEO.WKT_LITERAL.equals(datatype)) {
				return new WKTLiteral(label);
			} else if (coreDatatype == CoreDatatype.RDF.XMLLITERAL || RDF.XMLLITERAL.equals(datatype)) {
				return new XMLLiteral(label);
			} else if (HALYARD.TUPLE_TYPE.equals(datatype)) {
				return new TupleLiteral(label);
			} else if (HALYARD.ARRAY_TYPE.equals(datatype)) {
				return new ArrayLiteral(label);
			} else if (HALYARD.MAP_TYPE.equals(datatype)) {
				return new MapLiteral(label);
			}
		} catch (IllegalArgumentException e) {
			// catch any illegal values and fallback
		}
		return null;
	}

	@Override
	public Literal createLiteral(String label, IRI datatype) {
		Literal l = createAdvancedLiteral(label, datatype, null);
		return (l != null) ? l : super.createLiteral(label, datatype);
	}

	@Override
	public Literal createLiteral(String label, CoreDatatype coreDatatype) {
		Literal l = createAdvancedLiteral(label, null, coreDatatype);
		return (l != null) ? l : super.createLiteral(label, coreDatatype);
	}

	@Override
	public Literal createLiteral(String label, IRI datatype, CoreDatatype coreDatatype) {
		Literal l = createAdvancedLiteral(label, datatype, coreDatatype);
		return (l != null) ? l : super.createLiteral(label, datatype, coreDatatype);
	}
}