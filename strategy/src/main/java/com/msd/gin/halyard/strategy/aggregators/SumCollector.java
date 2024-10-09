package com.msd.gin.halyard.strategy.aggregators;

import com.msd.gin.halyard.model.vocabulary.HALYARD;
import com.msd.gin.halyard.strategy.MathOpEvaluator;

import java.math.BigInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.algebra.MathExpr.MathOp;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;

public class SumCollector implements ExtendedAggregateCollector {
	static final Literal ZERO = SimpleValueFactory.getInstance().createLiteral(BigInteger.ZERO);

	private final AtomicReference<Literal> sumRef = new AtomicReference<>();
	protected final MathOpEvaluator mathOpEval;
	private volatile ValueExprEvaluationException typeError;

	public SumCollector(MathOpEvaluator mathOpEval) {
		this.mathOpEval = mathOpEval;
	}

	public void addValue(Literal l, ValueFactory vf) {
		sumRef.accumulateAndGet(l, (total,next) -> {
			if (total != null) {
				return mathOpEval.evaluate(total, next, MathOp.PLUS, vf);
			} else {
				IRI dt = l.getDatatype();
				if (HALYARD.ARRAY_TYPE.equals(dt)) {
					return next;
				} else {
					// as per SPARQL spec: https://www.w3.org/TR/sparql11-query/#defn_aggSum
					return mathOpEval.evaluate(next, ZERO, MathOp.PLUS, vf);
				}
			}
		});
	}

	Literal getTotal() {
		return sumRef.get();
	}

	boolean hasError() {
		return typeError != null;
	}

	void setError(ValueExprEvaluationException err) {
		typeError = err;
	}

	void validate() throws ValueExprEvaluationException {
		if (typeError != null) {
			// a type error occurred while processing the aggregate, throw it
			// now.
			throw typeError;
		}
	}

	@Override
	public Value getFinalValue(TripleSource ts) {
		validate();
		return getTotal();
	}
}
