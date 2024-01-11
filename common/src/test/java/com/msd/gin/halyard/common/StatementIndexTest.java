package com.msd.gin.halyard.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class StatementIndexTest {
	@Test
	public void testPrefixWithPartition_subByte() {
		ByteSequence bseq = new ByteFiller((byte)0xFF, 6);
		byte[] actual = StatementIndex.prefixWithPartition(5, 3, bseq);
		byte[] expected = new byte[] {(byte)0xBF, (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF};
		assertArrayEquals(expected, actual);
	}

	@Test
	public void testPrefixWithPartition() {
		ByteSequence bseq = new ByteFiller((byte)0xFF, 6);
		byte[] actual = StatementIndex.prefixWithPartition(581, 13, bseq);
		byte[] expected = new byte[] {(byte)0x12, (byte)0x2F, (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF};
		assertArrayEquals(expected, actual);
	}

	@Test
	public void testPrefixWithPartition_byteAligned() {
		ByteSequence bseq = new ByteFiller((byte)0xFF, 6);
		byte[] actual = StatementIndex.prefixWithPartition(5, 16, bseq);
		byte[] expected = new byte[] {(byte)0x00, (byte)0x05, (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF};
		assertArrayEquals(expected, actual);
	}

	@Test
	public void testPartitionSizeTooBig() {
		ByteSequence bseq = new ByteFiller((byte)0xFF, 1);
		assertThrows(IllegalArgumentException.class, () ->
			StatementIndex.prefixWithPartition(500, 10, bseq)
		);
	}
}
