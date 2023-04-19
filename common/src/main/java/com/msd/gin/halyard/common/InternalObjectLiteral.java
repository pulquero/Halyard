package com.msd.gin.halyard.common;

import com.msd.gin.halyard.vocab.HALYARD;

import java.util.Objects;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.base.CoreDatatype;

public class InternalObjectLiteral<T> extends AbstractDataLiteral implements ObjectLiteral<T> {
	private static final long serialVersionUID = -124780683447095687L;

	private final T obj;

	public static <T> InternalObjectLiteral<T> of(T o) {
		return new InternalObjectLiteral<>(o);
	}

	public InternalObjectLiteral(T o) {
		this.obj = o;
	}

	public T objectValue() {
		return obj;
	}

	@Override
	public String getLabel() {
		throw new IllegalArgumentException("Object content");
	}

	@Override
	public IRI getDatatype() {
		return HALYARD.JAVA_TYPE;
	}

	@Override
	public CoreDatatype getCoreDatatype() {
		return CoreDatatype.NONE;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o instanceof InternalObjectLiteral) {
			InternalObjectLiteral<?> other = (InternalObjectLiteral<?>) o;
			return Objects.equals(obj, other.obj);
		}
		return false;
	}
}
