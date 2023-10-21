package com.msd.gin.halyard.common;

import java.util.Objects;

import org.eclipse.rdf4j.model.Value;

public abstract class RDFValue<V extends Value, T extends SPOC<V>> extends RDFIdentifier<T> {
	final V val;
	private final RDFFactory rdfFactory;
	private ByteArray ser;
	private Boolean isWellKnown;

	public static <V extends Value, T extends SPOC<V>> boolean matches(V value, RDFValue<V, T> pattern) {
		return pattern == null || pattern.val.equals(value);
	}


	protected RDFValue(RDFRole.Name role, V val, RDFFactory valueIO) {
		super(role);
		this.val = Objects.requireNonNull(val);
		this.rdfFactory = Objects.requireNonNull(valueIO);
	}

	boolean isWellKnownIRI() {
		if (isWellKnown == null) {
			isWellKnown = rdfFactory.isWellKnownIRI(val);
		}
		return isWellKnown;
	}

	public final ByteArray getSerializedForm() {
		if (ser == null) {
			if (val instanceof IdentifiableValue) {
				ser = ((IdentifiableValue) val).getSerializedForm(rdfFactory);
			} else {
				ser = rdfFactory.getSerializedForm(val);
			}
		}
		return ser;
	}

	@Override
	protected final ValueIdentifier calculateId() {
		if (val instanceof IdentifiableValue) {
			return ((IdentifiableValue) val).getId(rdfFactory);
		} else {
			return rdfFactory.id(val, getSerializedForm().copyBytes());
		}
	}

	@Override
	public String toString() {
		return val+" "+super.toString();
	}
}
