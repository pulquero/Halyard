package com.msd.gin.halyard.model;

import org.eclipse.rdf4j.model.BNode;

public abstract class BNodeWrapper implements BNode, Wrapper<BNode> {
	private static final long serialVersionUID = -3342094906407476268L;
	protected final BNode bnode;

	protected BNodeWrapper(BNode iri) {
		this.bnode = iri;
	}

	@Override
	public final BNode unwrap() {
		return bnode;
	}

	@Override
	public final String getID() {
		return bnode.getID();
	}

	@Override
	public final String stringValue() {
		return bnode.stringValue();
	}

	@Override
	public final String toString() {
		return bnode.toString();
	}

	@Override
	public final int hashCode() {
		return bnode.hashCode();
	}

	@Override
	public final boolean equals(Object o) {
		return bnode.equals(o);
	}
}
