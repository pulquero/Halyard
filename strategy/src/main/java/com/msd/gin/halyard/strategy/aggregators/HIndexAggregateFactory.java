package com.msd.gin.halyard.strategy.aggregators;

import com.msd.gin.halyard.model.IntLiteral;
import com.msd.gin.halyard.model.vocabulary.HALYARD;

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
		return new ValuesAggregateFunction();
	}

	@Override
	public AggregateCollector getCollector() {
		return new ValuesCollector(values -> {
			// NB: values.size() is expensive
			Value[] arr = values.toArray(new Value[0]);
			Arrays.sort(arr, new ValueComparator());
			int hindex = 0;
			for (int i=arr.length-1; i>=0; i--) {
				Value cites = arr[i];
				if (!cites.isLiteral()) {
					throw new ValueExprEvaluationException("not a number: " + cites);
				}
				Literal citesLit = (Literal) cites;
				CoreDatatype coreDatatype = citesLit.getCoreDatatype();
				if (!coreDatatype.isXSDDatatype() || !((CoreDatatype.XSD) coreDatatype).isNumericDatatype()) {
					throw new ValueExprEvaluationException("not a number: " + cites);
				}
				int c = Literals.getIntValue(citesLit, 0);
				int pos = arr.length - i;
				if (c >= pos) {
					hindex++;
				} else {
					break;
				}
			}
			return (hindex > 0) ? new IntLiteral(hindex, CoreDatatype.XSD.INTEGER) : NumberCollector.ZERO;
		});
	}
}
