package com.msd.gin.halyard.common;

import java.io.ObjectStreamException;

import javax.annotation.Nonnull;

import org.eclipse.rdf4j.model.Value;

public abstract class IdentifiableValue implements Value {
	private transient IdSer cachedIV = IdSer.NONE;

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
		ValueIdentifier id = current.id;
		if (rdfFactory != null && current.rdfFactory != rdfFactory) {
			ByteArray ser = rdfFactory.getSerializedForm(this);
			id = rdfFactory.id(this, ser.copyBytes());
			cachedIV = new IdSer(id, ser, rdfFactory);
		}
		return id;
	}

	public final ByteArray getSerializedForm(@Nonnull RDFFactory rdfFactory) {
		IdSer current = cachedIV;
		ByteArray ser = current.ser;
		if (current.rdfFactory != rdfFactory) {
			byte[] b = rdfFactory.valueWriter.toBytes(this);
			ValueIdentifier id = rdfFactory.id(this, b);
			ser = new ByteArray(b);
			cachedIV = new IdSer(id, ser, rdfFactory);
		} else if (ser == null) {
			ser = rdfFactory.getSerializedForm(this);
			cachedIV = new IdSer(current.id, ser, rdfFactory);
		}
		return ser;
	}

	public final void setId(@Nonnull RDFFactory rdfFactory, @Nonnull ValueIdentifier id) {
		IdSer current = cachedIV;
		if (current.rdfFactory != rdfFactory) {
			cachedIV = new IdSer(id, null, rdfFactory);
		}
	}

	protected final Object writeReplace() throws ObjectStreamException {
		byte[] b = ValueIO.getDefaultWriter().toBytes(this);
		return new SerializedValue(b);
	}
}
