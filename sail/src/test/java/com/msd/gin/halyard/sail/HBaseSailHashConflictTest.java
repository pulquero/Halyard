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
package com.msd.gin.halyard.sail;

import com.msd.gin.halyard.common.HBaseServerTestInstance;
import com.msd.gin.halyard.common.HalyardTableUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import static org.junit.Assert.*;

/**
 *
 * @author Adam Sotona (MSD)
 */
@RunWith(Parameterized.class)
public class HBaseSailHashConflictTest {

	private static final Resource SUBJ = SimpleValueFactory.getInstance().createIRI("http://testConflictingHash/subject1/");
    private static final IRI PRED = SimpleValueFactory.getInstance().createIRI("http://testConflictingHash/pred1/");
    private static final Value OBJ = SimpleValueFactory.getInstance().createLiteral("literal1");
    private static final IRI CONF = SimpleValueFactory.getInstance().createIRI("http://testConflictingHash/conflict/");

    private static HBaseSail sail;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                 {null, null, null, 8},
                 {SUBJ, null, null, 4},
                 {null, PRED, null, 4},
                 {null, null,  OBJ, 4},
                 {SUBJ, PRED, null, 2},
                 {null, PRED,  OBJ, 2},
                 {SUBJ, null,  OBJ, 2},
                 {SUBJ, PRED,  OBJ, 1},
                 {CONF, null, null, 0},
                 {null, CONF, null, 0},
                 {null, null, CONF, 0},
        });
    }

    @BeforeClass
    public static void setup() throws Exception {
		try (Table table = HalyardTableUtils.getTable(HBaseServerTestInstance.getInstanceConfig(), "testConflictingHash", true, 0)) {
            long timestamp = System.currentTimeMillis();
			List<? extends Cell> triple = HalyardTableUtils.toKeyValues(SUBJ, PRED, OBJ, null, false, timestamp);
			List<? extends Cell> conflicts[] = new List[] {
                HalyardTableUtils.toKeyValues(SUBJ, PRED, CONF, null, false, timestamp),
                HalyardTableUtils.toKeyValues(SUBJ, CONF,  OBJ, null, false, timestamp),
                HalyardTableUtils.toKeyValues(SUBJ, CONF, CONF, null, false, timestamp),
                HalyardTableUtils.toKeyValues(CONF, PRED,  OBJ, null, false, timestamp),
                HalyardTableUtils.toKeyValues(CONF, PRED, CONF, null, false, timestamp),
                HalyardTableUtils.toKeyValues(CONF, CONF,  OBJ, null, false, timestamp),
                HalyardTableUtils.toKeyValues(CONF, CONF, CONF, null, false, timestamp),
            };
			List<Put> puts = new ArrayList<>();
			for (int i = 0; i < triple.size(); i++) {
				Cell kv = triple.get(i);
				puts.add(new Put(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), kv.getTimestamp()).add(kv));
                for (int j=0; j<conflicts.length; j++) {
					Cell conflictCell = conflicts[j].get(i);
					Cell xkv = new KeyValue(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(),
                            kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength(),
							conflictCell.getQualifierArray(), conflictCell.getQualifierOffset(), conflictCell.getQualifierLength(),
                            kv.getTimestamp(), KeyValue.Type.Put,
							conflictCell.getValueArray(), conflictCell.getValueOffset(), conflictCell.getValueLength());
					puts.add(new Put(xkv.getRowArray(), xkv.getRowOffset(), xkv.getRowLength(), xkv.getTimestamp()).add(xkv));
                }
            }
			table.put(puts);
        }
        sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "testConflictingHash", false, 0, true, 0, null, null);
        sail.initialize();
    }

    @AfterClass
    public static void teardown() throws Exception {
        sail.shutDown();
    }

    private final Resource subj;
    private final IRI pred;
    private final Value obj;
    private final int results;

    public HBaseSailHashConflictTest(Resource subj, IRI pred, Value obj, int results) {
        this.subj = subj;
        this.pred = pred;
        this.obj = obj;
        this.results = results;
    }

    @Test
    public void testConflictingHash() throws Exception {
		try (SailConnection conn = sail.getConnection()) {
			try (CloseableIteration<? extends Statement, SailException> iter = conn.getStatements(subj, pred, obj, true)) {
				HashSet<Statement> res = new HashSet<>();
				while (iter.hasNext()) {
					res.add(iter.next());
				}
				assertEquals(results, res.size());
			}
		}
    }
}
