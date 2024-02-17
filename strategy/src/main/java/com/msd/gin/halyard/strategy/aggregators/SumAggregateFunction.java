package com.msd.gin.halyard.strategy.aggregators;

import com.msd.gin.halyard.strategy.QueryValueStepEvaluator;

import java.util.function.Predicate;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;

public final class SumAggregateFunction extends ThreadSafeAggregateFunction<NumberCollector,Value> {

	@Override
	public void processAggregate(BindingSet bs, Predicate<Value> distinctPredicate, NumberCollector col, QueryValueStepEvaluator evaluationStep) {
		if (col.hasError()) {
			// Prevent calculating the aggregate further if a type error has
			// occured.
			return;
		}

		Value v = evaluationStep.apply(bs);
		if (v != null) {
			if (v.isLiteral()) {
				if (distinctPredicate.test(v)) {
					Literal nextLiteral = (Literal) v;
					// check if the literal is numeric.
					CoreDatatype coreDatatype = nextLiteral.getCoreDatatype();
					if (coreDatatype.isXSDDatatype() && ((CoreDatatype.XSD) coreDatatype).isNumericDatatype()) {
						col.add(nextLiteral);
					} else {
						col.setError(new ValueExprEvaluationException("not a number: " + v));
					}
				}
			} else {
				col.setError(new ValueExprEvaluationException("not a number: " + v));
			}
		}
	}
}
