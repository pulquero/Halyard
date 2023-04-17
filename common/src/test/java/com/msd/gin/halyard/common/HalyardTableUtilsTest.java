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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 * @author Adam Sotona (MSD)
 */
@RunsLocalHBase
public class HalyardTableUtilsTest {
	private static final int ID_SIZE = 8;
	private static final int OBJECT_KEY_SIZE = 5;
	private static Connection conn;
	private static Table table;
	private static KeyspaceConnection keyspaceConn;
	private static RDFFactory rdfFactory;
	private static StatementIndices stmtIndices;

    @BeforeAll
    public static void setup() throws Exception {
		Configuration conf = HBaseServerTestInstance.getInstanceConfig();
		conf.setInt(TableConfig.ID_SIZE, ID_SIZE);
		conf.setInt(TableConfig.KEY_SIZE_OBJECT, OBJECT_KEY_SIZE);
		conn = HalyardTableUtils.getConnection(conf);
		table = HalyardTableUtils.getTable(conn, "testUtils", true, -1);
		keyspaceConn = new TableKeyspace.TableKeyspaceConnection(table);
		rdfFactory = RDFFactory.create(keyspaceConn);
		stmtIndices = new StatementIndices(conf, rdfFactory);
    }

    @AfterAll
    public static void teardown() throws Exception {
        table.close();
		conn.close();
    }

    @Test
    public void testConfig() {
    	assertEquals(ID_SIZE, rdfFactory.getIdSize());
    	assertEquals(OBJECT_KEY_SIZE, rdfFactory.getObjectRole(StatementIndex.Name.POS).keyHashSize());
    }

    @Test
    public void testGetTheSameTableAgain() throws Exception {
        table.close();
        table = HalyardTableUtils.getTable(conn, "testUtils", false, -1);
    }

    @Test
    public void testBigLiteral() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        ValueIO.Reader reader = rdfFactory.valueReader;

        Resource subj = vf.createIRI("http://testBigLiteral/subject/");
        IRI pred = vf.createIRI("http://testBigLiteral/pred/");
        Value obj = vf.createLiteral(RandomStringUtils.random(100000));
		List<Put> puts = new ArrayList<>();
        for (Cell kv : stmtIndices.insertKeyValues(subj, pred, obj, null, System.currentTimeMillis())) {
			puts.add(new Put(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), kv.getTimestamp()).add(kv));
        }
		table.put(puts);

        RDFSubject s = rdfFactory.createSubject(subj);
        RDFPredicate p = rdfFactory.createPredicate(pred);
        RDFObject o = rdfFactory.createObject(obj);
        try (ResultScanner rs = table.getScanner(stmtIndices.scan(s, p, o, null))) {
            assertEquals(obj, stmtIndices.parseStatements(s, p, o, null, rs.next(), reader, vf)[0].getObject());
        }
        try (ResultScanner rs = table.getScanner(stmtIndices.scan(s, p, null, null))) {
            assertEquals(obj, stmtIndices.parseStatements(s, p, null, null, rs.next(), reader, vf)[0].getObject());
        }
    }

    @Test
    public void testNestedTriples() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        IRI res = vf.createIRI("http://testiri");
        Triple t1 = vf.createTriple(res, res, res);
        Triple t2 = vf.createTriple(t1, res, t1);
        List<? extends Cell> kvs = stmtIndices.insertKeyValues(t2, res, t2, res, 0);
        for (Cell kv : kvs) {
			table.put(new Put(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), kv.getTimestamp()).add(kv));
        }
        assertTrue(stmtIndices.isTripleReferenced(keyspaceConn, t1));
        assertTrue(stmtIndices.isTripleReferenced(keyspaceConn, t2));
    }

    @Test
    public void testConflictingHash() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        ValueIO.Reader reader = rdfFactory.valueReader;

        Resource subj = vf.createIRI("http://testConflictingHash/subject/");
        IRI pred1 = vf.createIRI("http://testConflictingHash/pred1/");
        IRI pred2 = vf.createIRI("http://testConflictingHash/pred2/");
        Value obj1 = vf.createLiteral("literal1");
        Value obj2 = vf.createLiteral("literal2");
        long timestamp = System.currentTimeMillis();
        List<? extends Cell> kv1 = stmtIndices.insertKeyValues(subj, pred1, obj1, null, timestamp);
        List<? extends Cell> kv2 = stmtIndices.insertKeyValues(subj, pred2, obj2, null, timestamp);
		List<Put> puts = new ArrayList<>();
        for (int i=0; i<3; i++) {
        	Cell cell1 = kv1.get(i);
        	Cell cell2 = kv2.get(i);
			puts.add(new Put(cell1.getRowArray(), cell1.getRowOffset(), cell1.getRowLength(), cell1.getTimestamp())
					.add(cell1));
            Cell conflicting = new KeyValue(cell1.getRowArray(), cell1.getRowOffset(), cell1.getRowLength(),
                    cell1.getFamilyArray(), cell1.getFamilyOffset(), cell1.getFamilyLength(),
                    cell2.getQualifierArray(), cell2.getQualifierOffset(), cell2.getQualifierLength(),
                    cell1.getTimestamp(), KeyValue.Type.Put, cell2.getValueArray(), cell2.getValueOffset(), cell2.getValueLength());
			puts.add(new Put(conflicting.getRowArray(), conflicting.getRowOffset(), conflicting.getRowLength(),
					conflicting.getTimestamp()).add(conflicting));
        }
		table.put(puts);

        RDFSubject s = rdfFactory.createSubject(subj);
        RDFPredicate p1 = rdfFactory.createPredicate(pred1);
        RDFObject o1 = rdfFactory.createObject(obj1);
        try (ResultScanner rs = table.getScanner(stmtIndices.scan(s, p1, o1, null))) {
            Statement[] res = stmtIndices.parseStatements(s, p1, o1, null, rs.next(), reader, vf);
            assertEquals(1, res.length);
            assertEquals(vf.createStatement(subj, pred1, obj1), res[0]);
        }
    }

    @Test
    public void testClearTriples() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        Resource subj = vf.createIRI("http://whatever/subj/");
        IRI pred = vf.createIRI("http://whatever/pred/");
        Value expl = vf.createLiteral("explicit");
		List<Put> puts = new ArrayList<>();
        for (Cell kv : stmtIndices.insertKeyValues(subj, pred, expl, null, System.currentTimeMillis())) {
			puts.add(new Put(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), kv.getTimestamp()).add(kv));
        }
		table.put(puts);
        RDFSubject s = rdfFactory.createSubject(subj);
        RDFPredicate p = rdfFactory.createPredicate(pred);
        RDFObject o = rdfFactory.createObject(expl);
        try (ResultScanner rs = table.getScanner(stmtIndices.scan(s, p, o, null))) {
            assertNotNull(rs.next());
        }
		HalyardTableUtils.clearStatements(conn, table.getName());
        try (ResultScanner rs = table.getScanner(stmtIndices.scan(s, p, o, null))) {
            assertNull(rs.next());
        }
    }

    @Test
    public void testNoResult() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        ValueIO.Reader reader = rdfFactory.valueReader;

        assertEquals(0, stmtIndices.parseStatements(null, null, null, null, Result.EMPTY_RESULT, reader, vf).length);
    }

    @Test
    public void testNegativeSplitBits() {
    	assertThrows(IllegalArgumentException.class, () ->
    		HalyardTableUtils.calculateSplits(-1, true, stmtIndices)
		);
    }

    @Test
    public void testTooBigSplitBits() {
    	assertThrows(IllegalArgumentException.class, () ->
    		HalyardTableUtils.calculateSplits(17, true, stmtIndices)
		);
    }

    @Test
    public void testToKeyValues() throws Exception {
        IRI res = SimpleValueFactory.getInstance().createIRI("http://testiri");
        List<? extends Cell> kvs = stmtIndices.toKeyValues(res, res, res, res, true, 0, true);
        assertEquals(6, kvs.size());
        for (Cell kv : kvs) {
            assertEquals(Cell.Type.DeleteColumn, kv.getType());
        }
    }

    @Test
    public void testToKeyValuesTriple() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        IRI res = vf.createIRI("http://testiri");
        Triple t = vf.createTriple(res, res, res);
        List<? extends Cell> kvs = stmtIndices.toKeyValues(t, res, t, res, true, 0, true);
        // 6 for the statement, 3 for the subject triple, 3 for the object triple - no dedupping
        assertEquals(12, kvs.size());
        for (Cell kv : kvs) {
            assertEquals(Cell.Type.DeleteColumn, kv.getType());
        }
    }
}
