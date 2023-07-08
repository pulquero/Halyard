package com.msd.gin.halyard.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ByteSequenceTest {
	@Test
	public void testEquals() {
		ByteSequence seq1 = new ByteFiller((byte)4, 3);
		ByteSequence seq2 = new ByteArray(new byte[] {4, 4, 4});
		assertEquals(seq1, seq2);
		assertEquals(seq2, seq1);
	}
}
