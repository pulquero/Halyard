package com.msd.gin.halyard.tools;

import java.io.Closeable;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;

class MergingRDFHandler implements RDFHandler, Closeable {
	private final RDFHandler delegate;
	private boolean started;

	MergingRDFHandler(RDFHandler delegate) {
		this.delegate = delegate;
	}

	@Override
	public void startRDF() throws RDFHandlerException {
		if (!started) {
			delegate.startRDF();
			started = true;
		}
	}

	@Override
	public void handleNamespace(String prefix, String uri) throws RDFHandlerException {
		delegate.handleNamespace(prefix, uri);
	}

	@Override
	public void handleStatement(Statement st) throws RDFHandlerException {
		delegate.handleStatement(st);
	}

	@Override
	public void handleComment(String comment) throws RDFHandlerException {
		delegate.handleComment(comment);
	}

	@Override
	public void endRDF() throws RDFHandlerException {
	}

	@Override
	public void close() {
		delegate.endRDF();
	}
}
