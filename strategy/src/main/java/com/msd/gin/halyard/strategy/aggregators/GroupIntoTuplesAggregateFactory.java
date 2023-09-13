package com.msd.gin.halyard.strategy.aggregators;

import com.msd.gin.halyard.common.TupleLiteral;
import com.msd.gin.halyard.vocab.HALYARD;

import java.util.function.Function;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateCollector;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateFunction;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateFunctionFactory;
import org.kohsuke.MetaInfServices;

@MetaInfServices(AggregateFunctionFactory.class)
public final class GroupIntoTuplesAggregateFactory implements AggregateFunctionFactory {
	@Override
	public String getIri() {
		return HALYARD.GROUP_INTO_TUPLES_FUNCTION.stringValue();
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
			return (arr.length > 0) ? new TupleLiteral(arr) : null;
		});
	}
}
