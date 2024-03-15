package com.msd.gin.halyard.common;

import com.msd.gin.halyard.model.TermRole;

import java.util.Objects;

public class RDFIdentifier<T extends SPOC<?>> {
	private final TermRole roleName;
	private ValueIdentifier id;

	RDFIdentifier(TermRole role, ValueIdentifier id) {
		this(role);
		this.id = Objects.requireNonNull(id);
	}

	protected RDFIdentifier(TermRole role) {
		this.roleName = role;
	}

	public final TermRole getRoleName() {
		return roleName;
	}

	protected ValueIdentifier calculateId() {
		throw new AssertionError("ID must be provided");
	}

	public ValueIdentifier getId() {
		if (id == null) {
			id = calculateId();
		}
		return id;
	}

	@Override
	public String toString() {
		return "["+getId()+", "+roleName+"]";
	}
}
