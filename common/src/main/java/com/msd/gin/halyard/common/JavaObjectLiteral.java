package com.msd.gin.halyard.common;

import com.msd.gin.halyard.vocab.HALYARD;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Objects;
import java.util.zip.GZIPOutputStream;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.base.CoreDatatype;

public class JavaObjectLiteral<T> extends AbstractDataLiteral implements ObjectLiteral<T> {
	private static final long serialVersionUID = -124780683447095687L;

	private final T obj;

	public static <T> JavaObjectLiteral<T> of(T o) {
		return new JavaObjectLiteral<>(o);
	}

	public JavaObjectLiteral(T o) {
		this.obj = o;
	}

	@Override
	public T objectValue() {
		return obj;
	}

	@Override
	public String getLabel() {
		ByteArrayOutputStream out = new ByteArrayOutputStream(512);
		try (ObjectOutputStream objOut = new ObjectOutputStream(new GZIPOutputStream(out))) {
			objOut.writeObject(obj);
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
		return Hashes.encode(out.toByteArray());
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

		if (o instanceof JavaObjectLiteral) {
			JavaObjectLiteral<?> other = (JavaObjectLiteral<?>) o;
			return Objects.equals(obj, other.obj);
		} else {
			return super.equals(o);
		}
	}
}
