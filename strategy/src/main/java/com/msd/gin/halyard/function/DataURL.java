package com.msd.gin.halyard.function;

import com.google.common.net.UrlEscapers;
import com.msd.gin.halyard.vocab.HALYARD;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.helpers.NTriplesUtil;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Function.class)
public final class DataURL implements Function {

	@Override
	public String getURI() {
		return HALYARD.DATA_URL_FUNCTION.stringValue();
	}

	@Override
	public Value evaluate(ValueFactory valueFactory, Value... args) throws ValueExprEvaluationException {
		if (args.length < 1 || !args[0].isLiteral()) {
			throw new ValueExprEvaluationException(String.format("%s requires a literal and optional MIME type", getURI()));
		}
		Literal l = (Literal) args[0];
		String mimeType;
		if (args.length > 1 && args[1].isLiteral()) {
			Literal mtl = (Literal) args[1];
			if (mtl.getCoreDatatype() != CoreDatatype.XSD.STRING) {
				throw new ValueExprEvaluationException("MIME type must be a string");
			}
			mimeType = ((Literal)args[1]).getLabel();
		} else {
			mimeType = "";
		}
		String data;
		if (RDFFormat.NTRIPLES.getDefaultMIMEType().equals(mimeType)) {
			data = UrlEscapers.urlPathSegmentEscaper().escape(NTriplesUtil.toNTriplesString(l, true));
		} else {
			data = UrlEscapers.urlPathSegmentEscaper().escape(l.getLabel());
		}
		return valueFactory.createIRI("data:" + mimeType + "," + data);
	}

}
