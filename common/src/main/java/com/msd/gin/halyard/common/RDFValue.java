package com.msd.gin.halyard.common;

import java.nio.ByteBuffer;
import java.util.Objects;

import org.eclipse.rdf4j.model.Value;

public abstract class RDFValue<V extends Value, T extends SPOC<V>> extends RDFIdentifier {
	final V val;
	private final RDFFactory rdfFactory;
	private ByteBuffer ser;

	public static <V extends Value, T extends SPOC<V>> boolean matches(V value, RDFValue<V, T> pattern) {
		return pattern == null || pattern.val.equals(value);
	}


	protected RDFValue(RDFRole.Name role, V val, RDFFactory valueIO) {
		super(role);
		this.val = Objects.requireNonNull(val);
		this.rdfFactory = Objects.requireNonNull(valueIO);
	}

	boolean isWellKnownIRI() {
		return rdfFactory.isWellKnownIRI(val);
	}

	public final ByteBuffer getSerializedForm() {
		if (ser == null) {
			if (val instanceof SerializableValue) {
				ser = ((SerializableValue) val).getSerializedForm(rdfFactory);
			} else {
				ser = rdfFactory.getSerializedForm(val);
			}
		}
		return ser.duplicate();
	}

	@Override
	protected final ValueIdentifier calculateId() {
		return rdfFactory.id(val, getSerializedForm());
	}

	@Override
	public String toString() {
		return val+" "+super.toString();
	}
}
