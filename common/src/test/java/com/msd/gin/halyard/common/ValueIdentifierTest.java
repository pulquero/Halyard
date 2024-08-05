package com.msd.gin.halyard.common;

import com.msd.gin.halyard.common.ValueIdentifier.TypeNibble;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ValueIdentifierTest {
	@Test
	public void testIDTooShort() {
		assertThrows(IllegalArgumentException.class, () ->
			new ValueIdentifier.Format("Murmur3-128", 5, 1, TypeNibble.BIG_NIBBLE, true)
		);
	}

	@Test
	public void testHashCode_shortID() {
		ValueFactory vf = SimpleValueFactory.getInstance();
		ValueIdentifier.Format f = new ValueIdentifier.Format("Murmur3-128", 3, 1, TypeNibble.BIG_NIBBLE, false);
		Literal l = vf.createLiteral("foobar");
		ValueIdentifier id = f.id(l, ValueIO.getDefaultWriter().toBytes(l));
		assertThrows(IllegalArgumentException.class, () ->
			id.valueHashCode(f)
		);
	}

	@Test
	public void testHashCode_longID() {
		ValueFactory vf = SimpleValueFactory.getInstance();
		ValueIdentifier.Format f = new ValueIdentifier.Format("Murmur3-128", 6, 1, TypeNibble.BIG_NIBBLE, true);
		Literal l = vf.createLiteral("foobar");
		ValueIdentifier id = f.id(l, ValueIO.getDefaultWriter().toBytes(l));
		assertEquals(l.hashCode(), id.valueHashCode(f));
	}
}
