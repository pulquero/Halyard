package com.msd.gin.halyard.tools;

import java.io.Closeable;
import java.util.List;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.BooleanQueryResultHandler;
import org.eclipse.rdf4j.query.QueryResultHandler;
import org.eclipse.rdf4j.query.QueryResultHandlerException;
import org.eclipse.rdf4j.query.TupleQueryResultHandler;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;

class MergingQueryResultHandler implements TupleQueryResultHandler, BooleanQueryResultHandler, Closeable {
	private final QueryResultHandler delegate;
	private boolean started;

	MergingQueryResultHandler(QueryResultHandler delegate) {
		this.delegate = delegate;
	}

	@Override
	public void startQueryResult(List<String> bindingNames) throws TupleQueryResultHandlerException {
		if (!started) {
			delegate.startQueryResult(bindingNames);
			started = true;
		}
	}

	@Override
	public void handleLinks(List<String> linkUrls) throws QueryResultHandlerException {
		delegate.handleLinks(linkUrls);
	}

	@Override
	public void handleBoolean(boolean value) throws QueryResultHandlerException {
		delegate.handleBoolean(value);
	}

	@Override
	public void handleSolution(BindingSet bindingSet) throws TupleQueryResultHandlerException {
		delegate.handleSolution(bindingSet);
	}

	@Override
	public void endQueryResult() throws TupleQueryResultHandlerException {
	}

	@Override
	public void close() {
		delegate.endQueryResult();
	}
}
