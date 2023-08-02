package com.msd.gin.halyard.common;

import java.util.Set;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MutableBindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;

public final class ValueFactories {

    public static MutableBindingSet convertValues(BindingSet srcBs, ValueFactory tgtVf) {
		Set<String> bindingNames = srcBs.getBindingNames();
		QueryBindingSet tgtBs = new QueryBindingSet(bindingNames.size());
		for (String bn : bindingNames) {
			tgtBs.setBinding(bn, convertValue(srcBs.getValue(bn), tgtVf));
		}
		return tgtBs;
    }

    public static <T extends Value> T convertValue(T v, ValueFactory vf) {
    	if (v == null) {
    		return null;
    	}
    	if (v instanceof AbstractDataLiteral) {
    		return v;  // these are ValueFactory independent and also expensive
    	} else if ((v instanceof IdentifiableValue) && (vf instanceof IdValueFactory)) {
    		return (T) ((IdentifiableValue)v).clone();
    	}
    	if  (v.isIRI()) {
    		return (T) vf.createIRI(v.stringValue());
    	} else if (v.isLiteral()) {
    		Literal l = (Literal) v;
    		CoreDatatype cdt = l.getCoreDatatype();
    		if (cdt == CoreDatatype.XSD.STRING) {
    			return (T) vf.createLiteral(l.getLabel());
    		} else if (cdt == CoreDatatype.RDF.LANGSTRING) {
				return (T) vf.createLiteral(l.getLabel(), l.getLanguage().get());
			} else if (cdt == CoreDatatype.NONE) {
				return (T) vf.createLiteral(l.getLabel(), l.getDatatype());
			} else {
				return (T) vf.createLiteral(l.getLabel(), l.getCoreDatatype());
			}
    	} else if (v.isBNode()) {
    		return (T) vf.createBNode(v.stringValue());
    	} else if (v.isTriple()) {
    		Triple t = (Triple) v;
    		return (T) vf.createTriple(convertValue(t.getSubject(), vf), convertValue(t.getPredicate(), vf), convertValue(t.getObject(), vf));
    	} else {
    		throw new AssertionError();
    	}
    }
}
