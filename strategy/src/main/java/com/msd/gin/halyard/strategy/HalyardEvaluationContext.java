package com.msd.gin.halyard.strategy;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Date;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryEvaluationContext;

public final class HalyardEvaluationContext implements QueryEvaluationContext {
	private static final VarHandle NOW;

	static {
		try {
			NOW = MethodHandles
					.privateLookupIn(HalyardEvaluationContext.class, MethodHandles.lookup())
					.findVarHandle(HalyardEvaluationContext.class, "now", Literal.class);

		} catch (NoSuchFieldException | IllegalAccessException e) {
			throw new AssertionError("The 'now' field is missing");
		}
	}

	@SuppressWarnings("unused")
	private volatile Literal now;
	private final Dataset dataset;
	private final ValueFactory vf;

	public HalyardEvaluationContext(Dataset dataset, ValueFactory vf) {
		this.dataset = dataset;
		this.vf = vf;
	}

	@Override
	public Literal getNow() {
		Literal now = (Literal) NOW.get(this);

		// creating a new date is expensive because it uses the XMLGregorianCalendar implementation which is very
		// complex.
		if (now == null) {
			now = vf.createLiteral(new Date());
			boolean success = NOW.compareAndSet(this, null, now);
			if (!success) {
				now = (Literal) NOW.getAcquire(this);
			}
		}

		return now;
	}

	@Override
	public Dataset getDataset() {
		return dataset;
	}
}
