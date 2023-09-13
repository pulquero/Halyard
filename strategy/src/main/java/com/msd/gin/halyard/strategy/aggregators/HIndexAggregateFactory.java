package com.msd.gin.halyard.strategy.aggregators;

import com.msd.gin.halyard.common.IntLiteral;
import com.msd.gin.halyard.vocab.HALYARD;

import java.util.Arrays;
import java.util.function.Function;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.util.Literals;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.util.ValueComparator;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateCollector;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateFunction;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateFunctionFactory;
import org.kohsuke.MetaInfServices;

@MetaInfServices(AggregateFunctionFactory.class)
public class HIndexAggregateFactory implements AggregateFunctionFactory {
	@Override
	public String getIri() {
		return HALYARD.H_INDEX_FUNCTION.stringValue();
	}

	@Override
	public AggregateFunction buildFunction(Function<BindingSet, Value> evaluationStep) {
		return new ValuesAggregateFunction(evaluationStep);
	}

	@Override
	public AggregateCollector getCollector() {
		return new ValuesCollector(values -> {
			// NB: values.size() is expensive
			Value[] arr = values.toArray(new Value[0]);
			Arrays.sort(arr, new ValueComparator());
			for (int i=arr.length-1; i>=0; i--) {
				Value cites = arr[i];
				if (!cites.isLiteral()) {
					throw new ValueExprEvaluationException("not a number: " + cites);
				}
				Literal lit = (Literal) cites;
				CoreDatatype coreDatatype = lit.getCoreDatatype();
				if (!coreDatatype.isXSDDatatype() || !((CoreDatatype.XSD) coreDatatype).isNumericDatatype()) {
					throw new ValueExprEvaluationException("not a number: " + cites);
				}
				int pos = arr.length - i;
				if (Literals.getIntValue(lit, 0) < pos) {
					return new IntLiteral(pos - 1, CoreDatatype.XSD.INTEGER);
				}
			}
			return NumberCollector.ZERO;
		});
	}
}
