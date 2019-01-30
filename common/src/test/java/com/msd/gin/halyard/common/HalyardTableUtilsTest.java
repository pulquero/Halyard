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

import java.util.List;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.BeforeClass;
import static org.junit.Assert.*;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HalyardTableUtilsTest {

    private static HTable table;

    @BeforeClass
    public static void setup() throws Exception {
        table = HalyardTableUtils.getTable(HBaseServerTestInstance.getInstanceConfig(), "testUtils", true, -1);
    }

    @AfterClass
    public static void teardown() throws Exception {
        table.close();
    }

    @Test
    public void testGetTheSameTableAgain() throws Exception {
        table.close();
        table = HalyardTableUtils.getTable(HBaseServerTestInstance.getInstanceConfig(), "testUtils", true, 1);
    }

    @Test
    public void testBigLiteral() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        Resource subj = vf.createIRI("http://testBigLiteral/subject/");
        IRI pred = vf.createIRI("http://testBigLiteral/pred/");
        Value obj = vf.createLiteral(RandomStringUtils.random(100000));
        for (KeyValue kv : HalyardTableUtils.toKeyValues(subj, pred, obj, null, false, System.currentTimeMillis())) {
                table.put(new Put(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), kv.getTimestamp()).add(kv));
        }
        table.flushCommits();

        try (ResultScanner rs = table.getScanner(HalyardTableUtils.scan(subj, pred, obj, null))) {
            assertEquals(obj, HalyardTableUtils.parseStatement(rs.next(), subj, pred, obj, null, vf).getObject());
        }
        try (ResultScanner rs = table.getScanner(HalyardTableUtils.scan(subj, pred, null, null))) {
            assertEquals(obj, HalyardTableUtils.parseStatement(rs.next(), subj, pred, null, null, vf).getObject());
        }
    }

    @Test
    public void testTruncateTable() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        Resource subj = vf.createIRI("http://whatever/subj/");
        IRI pred = vf.createIRI("http://whatever/pred/");
        Value expl = vf.createLiteral("explicit");
        for (KeyValue kv : HalyardTableUtils.toKeyValues(subj, pred, expl, null, false, System.currentTimeMillis())) {
                table.put(new Put(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), kv.getTimestamp()).add(kv));
        }
        table.flushCommits();
        try (ResultScanner rs = table.getScanner(HalyardTableUtils.scan(subj, pred, expl, null))) {
            assertNotNull(rs.next());
        }
        table = HalyardTableUtils.truncateTable(table);
        try (ResultScanner rs = table.getScanner(HalyardTableUtils.scan(subj, pred, expl, null))) {
            assertNull(rs.next());
        }
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
        assertNull(HalyardTableUtils.parseStatement(Result.EMPTY_RESULT, null, null, null, null, SimpleValueFactory.getInstance()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeSplitBits() {
        HalyardTableUtils.calculateSplits(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTooBigSplitBits() {
        HalyardTableUtils.calculateSplits(17);
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
}
