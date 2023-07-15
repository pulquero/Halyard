package com.msd.gin.halyard.function;

import com.msd.gin.halyard.common.WKTLiteral;
import com.msd.gin.halyard.vocab.HALYARD;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.kohsuke.MetaInfServices;
import org.locationtech.jts.geom.Coordinate;

@MetaInfServices(Function.class)
public final class WktPoint implements Function {

	@Override
	public String getURI() {
		return HALYARD.WKT_POINT_FUNCTION.stringValue();
	}

	@Override
	public Value evaluate(ValueFactory valueFactory, Value... args) throws ValueExprEvaluationException {
		if (args.length != 2 || !args[0].isLiteral() || !args[1].isLiteral()) {
			throw new ValueExprEvaluationException(String.format("%s requires longitude and latitude", getURI()));
		}
		Literal lon = (Literal) args[0];
		Literal lat = (Literal) args[1];
		return new WKTLiteral(WKTLiteral.getGeometryFactory().createPoint(new Coordinate(lon.doubleValue(), lat.doubleValue())));
	}
}
