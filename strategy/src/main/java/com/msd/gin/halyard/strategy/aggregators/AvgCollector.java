package com.msd.gin.halyard.strategy.aggregators;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.MathExpr.MathOp;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.util.MathUtil;

public final class AvgCollector implements ExtendedAggregateCollector {
	private final AtomicLong count = new AtomicLong();
	private final AtomicReference<Literal> sumRef = new AtomicReference<>(NumberCollector.ZERO);
	private volatile ValueExprEvaluationException typeError;

	public void addValue(Literal l) {
		sumRef.accumulateAndGet(l, (total,next) -> MathUtil.compute(total, next, MathOp.PLUS));
	}

	public void incrementCount() {
		count.incrementAndGet();
	}

	public boolean hasError() {
		return typeError != null;
	}

	public void setError(ValueExprEvaluationException err) {
		typeError = err;
	}

	@Override
	public Value getFinalValue(TripleSource ts) {
		if (typeError != null) {
			// a type error occurred while processing the aggregate, throw it
			// now.
			throw typeError;
		}

		if (count.get() == 0) {
			return NumberCollector.ZERO;
		} else {
			Literal sizeLit = ts.getValueFactory().createLiteral(count.get());
			return MathUtil.compute(sumRef.get(), sizeLit, MathOp.DIVIDE);
		}
	}
}
