package com.msd.gin.halyard.common;

import java.util.Objects;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.Value;

public final class IdentifiableTriple extends IdentifiableValue implements Triple {
	private final Resource subject;
	private final IRI predicate;
	private final Value object;

	IdentifiableTriple(Resource subject, IRI predicate, Value object) {
		this.subject = Objects.requireNonNull(subject);
		this.predicate = Objects.requireNonNull(predicate);
		this.object = Objects.requireNonNull(object);
	}

	@Override
	public Resource getSubject() {
		return subject;
	}

	@Override
	public IRI getPredicate() {
		return predicate;
	}

	@Override
	public Value getObject() {
		return object;
	}

	@Override
	public String stringValue() {
		return "<<" +subject + " " + predicate + " " + object + ">>";
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
		if (o instanceof Triple) {
			Triple that = (Triple) o;
			return this.object.equals(that.getObject())
					&& this.subject.equals(that.getSubject())
					&& this.predicate.equals(that.getPredicate());
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return Objects.hash(getSubject(), getPredicate(), getObject());
	}

	@Override
	public final String toString() {
		return stringValue();
	}
}
