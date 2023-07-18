package com.msd.gin.halyard.common;

import java.io.ObjectStreamException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.eclipse.rdf4j.model.Value;

public abstract class IdentifiableValue implements Value {
	private IdSer cachedIV = IdSer.NONE;

	protected final ValueIdentifier getCompatibleId(Object o) {
		if (o instanceof IdentifiableValue) {
			IdentifiableValue that = (IdentifiableValue) o;
			IdSer current = this.cachedIV;
			IdSer thatCurrent = that.cachedIV;
			if (thatCurrent.rdfFactory != null && current.rdfFactory == thatCurrent.rdfFactory) {
				return thatCurrent.id;
			}
		}
		return null;
	}

	public final ValueIdentifier getId(RDFFactory rdfFactory) {
		IdSer current = cachedIV;
		if (rdfFactory != null && current.rdfFactory != rdfFactory) {
			current = makeIdSer(null, rdfFactory);
			cachedIV = current;
		}
		return current.id;
	}

	public final ByteArray getSerializedForm(@Nonnull RDFFactory rdfFactory) {
		IdSer current = cachedIV;
		if (current.rdfFactory != rdfFactory) {
			current = makeIdSer(null, rdfFactory);
			cachedIV = current;
		} else if (current.ser == null) {
			current = makeIdSer(current.id, rdfFactory);
			cachedIV = current;
		}
		return current.ser;
	}

	private IdSer makeIdSer(ValueIdentifier id, RDFFactory rdfFactory) {
		byte[] ser = rdfFactory.valueWriter.toBytes(this);
		if (id == null) {
			id = rdfFactory.id(this, ser);
		}
		return new IdSer(id, new ByteArray(ser), rdfFactory);
	}

	public final void setId(@Nonnull ValueIdentifier id, @Nonnull RDFFactory rdfFactory) {
		IdSer current = cachedIV;
		if (current.rdfFactory != rdfFactory) {
			cachedIV = new IdSer(id, null, rdfFactory);
		}
	}

	protected final Object writeReplace() throws ObjectStreamException {
		byte[] b = ValueIO.getDefaultWriter().toBytes(this);
		return new SerializedValue(b);
	}

	static final class IdSer {
		static final IdSer NONE = new IdSer();

		final ValueIdentifier id;
		final ByteArray ser;
		final RDFFactory rdfFactory;

		private IdSer() {
			this.id = null;
			this.ser = null;
			this.rdfFactory = null;
		}

		/**
		 * Identifier must always be present.
		 */
		IdSer(@Nonnull ValueIdentifier id, @Nullable ByteArray ser, @Nonnull RDFFactory rdfFactory) {
			this.id = id;
			this.ser = ser;
			this.rdfFactory = rdfFactory;
		}
	}
}
