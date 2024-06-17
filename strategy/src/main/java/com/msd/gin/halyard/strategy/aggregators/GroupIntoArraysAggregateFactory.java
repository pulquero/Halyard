package com.msd.gin.halyard.strategy.aggregators;

import com.msd.gin.halyard.model.ArrayLiteral;
import com.msd.gin.halyard.model.MapLiteral;
import com.msd.gin.halyard.model.vocabulary.HALYARD;

import java.util.function.Function;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.base.CoreDatatype.XSD;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateCollector;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateFunction;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateFunctionFactory;
import org.kohsuke.MetaInfServices;

@MetaInfServices(AggregateFunctionFactory.class)
public final class GroupIntoArraysAggregateFactory implements AggregateFunctionFactory {
	@Override
	public String getIri() {
		return HALYARD.GROUP_INTO_ARRAYS_FUNCTION.stringValue();
	}

	@Override
	public AggregateFunction buildFunction(Function<BindingSet, Value> evaluationStep) {
		return new ValuesAggregateFunction();
	}

	@Override
	public AggregateCollector getCollector() {
		return new ValuesCollector(values -> {
			// NB: values.size() is expensive
			Value[] varr = values.toArray(new Value[0]);
			Object[] oarr = new Object[varr.length];
			for (int i=0; i<varr.length; i++) {
				Value v = varr[i];
				if (!v.isLiteral()) {
					throw new ValueExprEvaluationException("not a literal: " + v);
				}
				Object o;
				Literal l = (Literal) v;
				XSD xsd = l.getCoreDatatype().asXSDDatatype().orElse(null);
				if (xsd != null && xsd.isIntegerDatatype()) {
					// use exact integer representation if available
					// NB: floating-point values aren't guaranteed to have an exact representation so coerce them from string instead
					try {
						if (xsd == XSD.INT) {
							o = l.intValue();
						} else {
							o = l.longValue();
						}
					} catch (NumberFormatException nfe) {
						o = l.getLabel();
					}
				} else if (HALYARD.ARRAY_TYPE.equals(l.getDatatype())) {
					o = ArrayLiteral.objectArray(l);
				} else if (HALYARD.MAP_TYPE.equals(l.getDatatype())) {
					o = MapLiteral.objectMap(l);
				} else {
					o = l.getLabel();
				}
				oarr[i] = o;
			}
			return (oarr.length > 0) ? new ArrayLiteral(oarr) : null;
		});
	}
}
