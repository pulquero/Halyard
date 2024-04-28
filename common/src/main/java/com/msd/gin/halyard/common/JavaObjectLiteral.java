package com.msd.gin.halyard.common;

import com.msd.gin.halyard.model.AbstractDataLiteral;
import com.msd.gin.halyard.model.ObjectLiteral;

import java.util.Objects;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

public class JavaObjectLiteral<T> extends AbstractDataLiteral implements ObjectLiteral<T> {
	private static final long serialVersionUID = -124780683447095687L;

	private final T obj;
	private final IRI datatype;

	public static <T> JavaObjectLiteral<T> of(T o) {
		return new JavaObjectLiteral<>(o);
	}

	public JavaObjectLiteral(T o) {
		this.obj = o;
		Class<?> cls = (obj != null) ? obj.getClass() : Void.class;
		this.datatype = SimpleValueFactory.getInstance().createIRI("java:", cls.getName());
	}

	@Override
	public T objectValue() {
		return obj;
	}

	@Override
	public String getLabel() {
		return String.valueOf(obj);
	}

	@Override
	public IRI getDatatype() {
		return datatype;
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

		if (o instanceof JavaObjectLiteral) {
			JavaObjectLiteral<?> other = (JavaObjectLiteral<?>) o;
			return Objects.equals(obj, other.obj);
		} else {
			return super.equals(o);
		}
	}
}
