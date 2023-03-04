package com.msd.gin.halyard.strategy;

import java.util.Date;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryContext;

public final class HalyardExecutionContext {
	public static final String QUERY_CONTEXT_SOURCE_STRING_ATTRIBUTE = "SourceString";

    private final QueryContext queryContext;

    /**
     * Ensures 'now' is the same across all parts of the query evaluation chain.
     */
    private Literal sharedValueOfNow;

    HalyardExecutionContext(QueryContext queryContext) {
    	this.queryContext = queryContext;
    }

	String getSourceString() {
		return queryContext.getAttribute(QUERY_CONTEXT_SOURCE_STRING_ATTRIBUTE);
	}

	Literal getNow(ValueFactory vf) {
		if (sharedValueOfNow == null) {
			sharedValueOfNow = vf.createLiteral(new Date());
		}
		return sharedValueOfNow;
	}
}
