package com.msd.gin.halyard.common;

import com.msd.gin.halyard.model.ValueConstraint;
import com.msd.gin.halyard.model.ValueType;
import com.msd.gin.halyard.model.vocabulary.SCHEMA_ORG;

import org.apache.hadoop.hbase.client.Scan;
import org.eclipse.rdf4j.model.vocabulary.RDF;
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

	@Test
	public void testScanRange() {
		StatementIndices indices = StatementIndices.create();
		StatementIndex<SPOC.P,SPOC.O,SPOC.S,SPOC.C> pos = indices.getPOSIndex();
		RDFFactory rdfFactory = indices.getRDFFactory();
		int emptyScanCount = 0;
		int numPartitions = 64;
		int nbits = StatementIndices.powerOf2BitCount(numPartitions);
		for (int i=0; i<numPartitions; i++) {
			Scan scan = pos.scanWithConstraint(rdfFactory.createPredicate(RDF.TYPE), rdfFactory.createObject(SCHEMA_ORG.PERSON), i, nbits, new ValueConstraint(ValueType.IRI), null);
			if (scan == null) {
				emptyScanCount++;
			}
		}
		assertEquals(48, emptyScanCount);
	}
}
