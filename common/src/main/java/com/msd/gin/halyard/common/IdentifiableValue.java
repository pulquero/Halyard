package com.msd.gin.halyard.common;

import java.io.ObjectStreamException;
import java.nio.ByteBuffer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;

public abstract class IdentifiableValue implements Value, Cloneable {
	protected static final ValueFactory MATERIALIZED_VALUE_FACTORY = new AdvancedValueFactory();

	private IdSer cachedIV;
	private Value materializedValue;

	protected IdentifiableValue(Value v) {
		cachedIV = IdSer.NONE;
		materializedValue = v;
	}

	protected IdentifiableValue(ByteArray ser, RDFFactory rdfFactory) {
		cachedIV = new IdSer(null, ser, rdfFactory);
	}

	@Override
	public final String stringValue() {
		return getValue().stringValue();
	}

	@Override
	public final boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o instanceof IdentifiableValue) {
			IdentifiableValue that = (IdentifiableValue) o;
			IdSer thisCurrent = this.cachedIV;
			IdSer thatCurrent = that.cachedIV;
			RDFFactory commonFactory;
			if (thatCurrent.rdfFactory == null || thisCurrent.rdfFactory == thatCurrent.rdfFactory) {
				commonFactory = thisCurrent.rdfFactory;
			} else {
				commonFactory = null;
			}
			if (commonFactory != null) {
				return this.getId(commonFactory).equals(that.getId(commonFactory));
			}
		}
		return getValue().equals(o);
	}

	@Override
	public final int hashCode() {
		return getValue().hashCode();
	}

	@Override
	public final IdentifiableValue clone() {
		try {
			return (IdentifiableValue) super.clone();
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public final String toString() {
		return getValue().toString();
	}

	public final ValueIdentifier getId(@Nonnull RDFFactory rdfFactory) {
		IdSer current = cachedIV;
		if (rdfFactory != null && current.rdfFactory != rdfFactory) {
			materialize(current);
			current = createIdSer(null, true, null, rdfFactory);
			cachedIV = current;
		} else if (current.id == null) {
			current = createIdSer(null, true, current.ser, rdfFactory);
			cachedIV = current;
		}
		return current.id;
	}

	public final ByteArray getSerializedForm(@Nonnull RDFFactory rdfFactory) {
		IdSer current = cachedIV;
		if (current.rdfFactory != rdfFactory) {
			materialize(current);
			current = createIdSer(null, false, null, rdfFactory);
			cachedIV = current;
		} else if (current.ser == null) {
			current = createIdSer(current.id, false, null, rdfFactory);
			cachedIV = current;
		}
		return current.ser;
	}

	public final void setId(@Nonnull ValueIdentifier id, @Nonnull RDFFactory rdfFactory) {
		IdSer current = cachedIV;
		if (current.rdfFactory != rdfFactory) {
			cachedIV = new IdSer(id, null, rdfFactory);
		} else if (current.id == null) {
			cachedIV = new IdSer(id, current.ser, rdfFactory);
		}
	}

	private Value materialize(IdSer current) {
		Value mv = materializedValue;
		if (mv == null) {
			mv = current.rdfFactory.valueReader.readValue(ByteBuffer.wrap(current.ser.copyBytes()), MATERIALIZED_VALUE_FACTORY);
			materializedValue = mv;
		}
		return mv;
	}

	protected final Value getValue() {
		return materialize(cachedIV);
	}

	private IdSer createIdSer(ValueIdentifier id, boolean makeId, ByteArray ser, @Nonnull RDFFactory rdfFactory) {
		byte[] serBytes;
		if (ser == null) {
			serBytes = rdfFactory.valueWriter.toBytes(this);
			ser = new ByteArray(serBytes);
		} else {
			serBytes = null;
		}
		if (id == null && makeId) {
			if (serBytes == null) {
				serBytes = ser.copyBytes();
			}
			id = rdfFactory.id(this, serBytes);
		}
		return new IdSer(id, ser, rdfFactory);
	}

	protected final Object writeReplace() throws ObjectStreamException {
		byte[] b = ValueIO.getDefaultWriter().toBytes(this);
		return new SerializedValue(b);
	}


	private static final class IdSer {
		static final IdSer NONE = new IdSer();

		final ValueIdentifier id;
		final ByteArray ser;
		final RDFFactory rdfFactory;

		private IdSer() {
			this.id = null;
			this.ser = null;
			this.rdfFactory = null;
		}

		IdSer(@Nullable ValueIdentifier id, @Nullable ByteArray ser, @Nonnull RDFFactory rdfFactory) {
			this.id = id;
			this.ser = ser;
			this.rdfFactory = rdfFactory;
		}
	}
}
