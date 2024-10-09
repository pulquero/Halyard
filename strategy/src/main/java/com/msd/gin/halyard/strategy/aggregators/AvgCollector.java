package com.msd.gin.halyard.strategy.aggregators;

import com.msd.gin.halyard.strategy.MathOpEvaluator;

import java.util.concurrent.atomic.AtomicLong;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.MathExpr.MathOp;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;

public final class AvgCollector extends SumCollector {
	private final AtomicLong count = new AtomicLong();

	public AvgCollector(MathOpEvaluator mathOpEval) {
		super(mathOpEval);
	}

	void incrementCount() {
		count.incrementAndGet();
	}

	@Override
	public Value getFinalValue(TripleSource ts) {
		validate();
		if (count.get() == 0) {
			return SumCollector.ZERO;
		} else {
			Literal sum = getTotal();
			Literal sizeLit = ts.getValueFactory().createLiteral(count.get());
			return mathOpEval.evaluate(sum, sizeLit, MathOp.DIVIDE, ts.getValueFactory());
		}
	}
}
