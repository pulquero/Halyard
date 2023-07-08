package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.BNode;

public final class IdentifiableBNode extends IdentifiableValue implements BNode {
	private final String id;

	IdentifiableBNode(String id) {
		this.id = id;
	}

	@Override
	public final String getID() {
		return id;
	}

	@Override
	public String stringValue() {
		return id;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		ValueIdentifier thatId = getCompatibleId(o);
		if (thatId != null) {
			return getId(null).equals(thatId);
		}
		return (o instanceof BNode)
			&& this.id.equals(((BNode)o).getID());
	}

	@Override
	public int hashCode() {
		return id.hashCode();
	}

	@Override
	public String toString() {
		return "_:" + id;
	}
}
