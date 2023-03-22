package com.msd.gin.halyard.common;

import java.util.Objects;

public class RDFIdentifier<T extends SPOC<?>> {
	private final RDFRole.Name roleName;
	private ValueIdentifier id;

	RDFIdentifier(RDFRole.Name role, ValueIdentifier id) {
		this(role);
		this.id = Objects.requireNonNull(id);
	}

	protected RDFIdentifier(RDFRole.Name role) {
		this.roleName = role;
	}

	public final RDFRole.Name getRoleName() {
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
