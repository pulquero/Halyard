package com.msd.gin.halyard.common;

import com.msd.gin.halyard.model.TermRole;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.eclipse.rdf4j.model.Value;

public abstract class RDFValue<V extends Value, T extends SPOC<V>> extends RDFIdentifier<T> {
	final V val;
	private final RDFFactory rdfFactory;
	private final boolean isWellKnownIri;
	private ByteArray ser;

	public static <V extends Value, T extends SPOC<V>> boolean matches(V value, RDFValue<V, T> pattern) {
		return pattern == null || pattern.val.equals(value);
	}

	protected RDFValue(TermRole role, V val, RDFFactory rdfFactory) {
		super(role);
		this.val = Objects.requireNonNull(val);
		this.rdfFactory = Objects.requireNonNull(rdfFactory);
		this.isWellKnownIri = false;
	}

	protected RDFValue(TermRole role, V val, RDFFactory rdfFactory, @Nonnull ValueIdentifier wellKnownIriId) {
		super(role, wellKnownIriId);
		this.val = Objects.requireNonNull(val);
		this.rdfFactory = Objects.requireNonNull(rdfFactory);
		this.isWellKnownIri = true;
	}

	boolean isWellKnownIRI() {
		return isWellKnownIri;
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
