/*
 * Copyright 2016 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
 * Inc., Kenilworth, NJ, USA.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.msd.gin.halyard.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.Test;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HalyardTableUtilsTest {

    @Test
    public void testIdIsUnique() {
        ValueFactory vf = SimpleValueFactory.getInstance();
        assertNotEquals(
        	HalyardTableUtils.id(vf.createLiteral("1", vf.createIRI("local:type1"))),
        	HalyardTableUtils.id(vf.createLiteral("1", vf.createIRI("local:type2"))));
    }

    @Test
    public void testNewInstance() {
        new HalyardTableUtils();
    }

    @Test(expected = RuntimeException.class)
    public void testGetInvalidMessageDigest() {
        HalyardTableUtils.getMessageDigest("invalid");
    }

    @Test
    public void testNoResult() {
        assertEquals(0, HalyardTableUtils.parseStatements(null, null, null, null, Result.EMPTY_RESULT, SimpleValueFactory.getInstance()).size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeSplitBits() {
        HalyardTableUtils.calculateSplits(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTooBigSplitBits() {
        HalyardTableUtils.calculateSplits(21);
    }

    @Test
    public void testToKeyValuesDelete() throws Exception {
        IRI res = SimpleValueFactory.getInstance().createIRI("http://testiri");
        KeyValue kvs[] = HalyardTableUtils.toKeyValues(res, res, res, res, true, 0);
        assertEquals(6, kvs.length);
        for (KeyValue kv : kvs) {
            assertEquals(KeyValue.Type.DeleteColumn, KeyValue.Type.codeToType(kv.getTypeByte()));
        }
    }

    @Test
    public void testEncode() {
        assertEquals("AQIDBAU", HalyardTableUtils.encode(new byte[]{1, 2, 3, 4, 5}));
    }

    @Test
    public void testSalt() {
    	for(int salt=0; salt<HalyardTableUtils.NUM_SALTS; salt++) {
    		for(int prefix=0; prefix<=HalyardTableUtils.COSP_PREFIX; prefix++) {
    			assertEquals(prefix, HalyardTableUtils.unsalt(HalyardTableUtils.addSalt((byte)prefix, salt)));
    		}
    	}
    }

    @Test
    public void testScanSalt() throws IOException {
    	byte prefix = 4;
		Scan scan = HalyardTableUtils.scan(new byte[] { prefix }, new byte[] { prefix });
    	List<Scan> saltedScans = HalyardTableUtils.addSalt(scan);
    	for(int i=0; i<HalyardTableUtils.NUM_SALTS; i++) {
    		byte[] startRow = saltedScans.get(i).getStartRow();
    		assertEquals(prefix, HalyardTableUtils.unsalt(startRow[0]));
    		assertEquals(i, HalyardTableUtils.getSalt(startRow[0]));
    		byte[] stopRow = saltedScans.get(i).getStopRow();
    		assertEquals(prefix, HalyardTableUtils.unsalt(stopRow[0]));
    		assertEquals(i, HalyardTableUtils.getSalt(stopRow[0]));
    	}
    }

    @Test
    public void testEmptyScanSalt() throws IOException {
    	Scan scan = HalyardTableUtils.scan(null, null);
    	List<Scan> saltedScans = HalyardTableUtils.addSalt(scan);
    	for(int i=0; i<HalyardTableUtils.NUM_SALTS; i++) {
    		byte[] startRow = saltedScans.get(i).getStartRow();
			assertEquals(0, HalyardTableUtils.unsalt(startRow[0]));
			assertEquals(i, HalyardTableUtils.getSalt(startRow[0]));
    		byte[] stopRow = saltedScans.get(i).getStopRow();
    		if(i == HalyardTableUtils.NUM_SALTS-1) {
    			assertEquals(0, stopRow.length);
    		} else {
	    		assertEquals(0, HalyardTableUtils.unsalt(stopRow[0]));
				assertEquals(i + 1, HalyardTableUtils.getSalt(stopRow[0]));
    		}
    	}
    }

	@Test
	public void testGetScanSalt() throws IOException {
		Scan scan = HalyardTableUtils.scan(new byte[] { 0x01, 0x2e }, new byte[] { 0x01, 0x2e });
		List<Scan> saltedScans = HalyardTableUtils.addSalt(scan);
		assertEquals(1, saltedScans.size());
		byte[] startRow = saltedScans.get(0).getStartRow();
		assertEquals(2, HalyardTableUtils.getSalt(startRow[0]));
		byte[] stopRow = saltedScans.get(0).getStartRow();
		assertEquals(2, HalyardTableUtils.getSalt(stopRow[0]));
	}
}
