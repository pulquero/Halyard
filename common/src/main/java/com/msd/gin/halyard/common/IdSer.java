package com.msd.gin.halyard.common;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

final class IdSer {
	static final IdSer NONE = new IdSer();

	final ValueIdentifier id;
	final ByteArray ser;
	final RDFFactory rdfFactory;

	private IdSer() {
		this(null, null, null);
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
