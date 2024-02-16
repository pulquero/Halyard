/*
 * Copyright 2018 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
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
package com.msd.gin.halyard.query.algebra.evaluation.function;

import static com.msd.gin.halyard.vocab.HALYARD.PARALLEL_SPLIT_FUNCTION;

import com.msd.gin.halyard.common.StatementIndex.Name;
import com.msd.gin.halyard.query.algebra.evaluation.EmptyTripleSource;
import com.msd.gin.halyard.query.algebra.evaluation.PartitionableTripleSource;
import com.msd.gin.halyard.query.algebra.evaluation.function.ParallelSplitFunction;
import com.msd.gin.halyard.common.ValueConstraint;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class ParallelSplitFunctionTest {

    @Test
    public void testGetURI() {
        assertEquals(PARALLEL_SPLIT_FUNCTION.stringValue(), new ParallelSplitFunction().getURI());
    }

    @Test(expected = ValueExprEvaluationException.class)
    public void testEvaluateNoArgs() {
		TripleSource ts = new MockPartitionedTripleSource(1, 2);
        new ParallelSplitFunction().evaluate(ts);
    }

    @Test(expected = ValueExprEvaluationException.class)
    public void testEvaluateNullArgs() {
		TripleSource ts = new MockPartitionedTripleSource(1, 2);
		ValueFactory vf = ts.getValueFactory();
		new ParallelSplitFunction().evaluate(ts, vf.createLiteral(10), null);
    }

    @Test(expected = ValueExprEvaluationException.class)
    public void testEvaluateSingleArg() {
		TripleSource ts = new MockPartitionedTripleSource(1, 2);
		ValueFactory vf = ts.getValueFactory();
        new ParallelSplitFunction().evaluate(ts, vf.createLiteral(10));
    }

    @Test(expected = ValueExprEvaluationException.class)
    public void testEvaluateNegativeArg() {
		TripleSource ts = new MockPartitionedTripleSource(1, 2);
		ValueFactory vf = ts.getValueFactory();
        new ParallelSplitFunction().evaluate(ts, vf.createLiteral(-1), vf.createLiteral("hello"));
    }

    @Test(expected = ValueExprEvaluationException.class)
    public void testEvaluateNANArg() {
		TripleSource ts = new MockPartitionedTripleSource(1, 2);
		ValueFactory vf = ts.getValueFactory();
        new ParallelSplitFunction().evaluate(ts, vf.createLiteral("not a number"), vf.createLiteral("hello"));
    }

    @Test
    public void testEvaluatePartition0() {
		TripleSource ts = new MockPartitionedTripleSource(0, 2);
		ValueFactory vf = ts.getValueFactory();
        assertTrue(((Literal)new ParallelSplitFunction().evaluate(ts, vf.createLiteral(3), vf.createLiteral("hello"))).booleanValue());
    }

    @Test
    public void testEvaluatePartition1() {
		TripleSource ts = new MockPartitionedTripleSource(1, 2);
		ValueFactory vf = ts.getValueFactory();
        assertFalse(((Literal)new ParallelSplitFunction().evaluate(ts, vf.createLiteral(3), vf.createLiteral("hello"))).booleanValue());
    }

    @Test
    public void testGetNumberOfForksFromFunction() {
        assertEquals(8, ParallelSplitFunction.getNumberOfPartitionsFromFunctionArgument("select * where {?s ?p ?o. filter <" + PARALLEL_SPLIT_FUNCTION.stringValue() + ">(8, ?s)}", -1, EmptyBindingSet.getInstance()));
    }

    @Test
    public void testGetNumberOfForksFromSelectWithoutFunction() {
        assertEquals(1, ParallelSplitFunction.getNumberOfPartitionsFromFunctionArgument("select * where {?s ?p ?o.}", -1, EmptyBindingSet.getInstance()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetNumberOfForksNoArgs() {
        ParallelSplitFunction.getNumberOfPartitionsFromFunctionArgument("select * where {?s ?p ?o. filter <" + PARALLEL_SPLIT_FUNCTION.stringValue() + ">()}", -1, EmptyBindingSet.getInstance());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetNumberOfForksNANArg() {
        ParallelSplitFunction.getNumberOfPartitionsFromFunctionArgument("select * where {?s ?p ?o. filter <" + PARALLEL_SPLIT_FUNCTION.stringValue() + ">(\"not a number\", ?s)}", -1, EmptyBindingSet.getInstance());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetNumberOfForksUnboundVarArg() {
        ParallelSplitFunction.getNumberOfPartitionsFromFunctionArgument("select * where {?s ?p ?o. filter <" + PARALLEL_SPLIT_FUNCTION.stringValue() + ">(?p, ?s)}", -1, EmptyBindingSet.getInstance());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetNumberOfForksNegativeArg() {
        ParallelSplitFunction.getNumberOfPartitionsFromFunctionArgument("select * where {?s ?p ?o. filter <" + PARALLEL_SPLIT_FUNCTION.stringValue() + ">(-1, ?s)}", -1, EmptyBindingSet.getInstance());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetNumberOfForksDoubleFunction() {
        ParallelSplitFunction.getNumberOfPartitionsFromFunctionArgument("select * where {?s ?p ?o. filter <" + PARALLEL_SPLIT_FUNCTION.stringValue() + ">(8, ?s) filter <" + PARALLEL_SPLIT_FUNCTION.stringValue() + ">(3, ?s)}", -1, EmptyBindingSet.getInstance());
    }

    @Test
    public void testGetNumberOfForksDoubleMatchingFunction() {
        assertEquals(8, ParallelSplitFunction.getNumberOfPartitionsFromFunctionArgument("select * where {?s ?p ?o. filter <" + PARALLEL_SPLIT_FUNCTION.stringValue() + ">(8, ?s) filter <" + PARALLEL_SPLIT_FUNCTION.stringValue() + ">(8, ?s) filter <http://whatever/function>()}", -1, EmptyBindingSet.getInstance()));
    }

    @Test
    public void testGetNumberOfForksFromUpdate() {
        String query = "insert {?s ?p ?o} where {?s ?p ?o. filter <" + PARALLEL_SPLIT_FUNCTION.stringValue() + ">(16, ?s)};"
            + "clear all;"
            + "delete {?s ?p ?o} where {?s ?p ?o. filter <" + PARALLEL_SPLIT_FUNCTION.stringValue() + ">(8, ?s)}";
        BindingSet bs = EmptyBindingSet.getInstance();
        assertEquals(16, ParallelSplitFunction.getNumberOfPartitionsFromFunctionArgument(query, 0, bs));
        assertEquals(1, ParallelSplitFunction.getNumberOfPartitionsFromFunctionArgument(query, 1, bs));
        assertEquals(8, ParallelSplitFunction.getNumberOfPartitionsFromFunctionArgument(query, 2, bs));
        assertEquals(0, ParallelSplitFunction.getNumberOfPartitionsFromFunctionArgument(query, 3, bs));
    }


    @Test
    public void testGetNumberOfForksFromBindings() {
    	QueryBindingSet bs = new QueryBindingSet();
    	bs.setBinding("forks", SimpleValueFactory.getInstance().createLiteral(8));
        assertEquals(8, ParallelSplitFunction.getNumberOfPartitionsFromFunctionArgument("select * where {?s ?p ?o. filter <" + PARALLEL_SPLIT_FUNCTION.stringValue() + ">(?forks, ?s)}", -1, bs));
    }


    static class MockPartitionedTripleSource extends EmptyTripleSource implements PartitionableTripleSource {
    	private final int forkIndex;
    	private final int forkCount;

    	MockPartitionedTripleSource(int forkIndex, int forkCount) {
    		this.forkIndex = forkIndex;
    		this.forkCount = forkCount;
    	}

    	@Override
		public int getPartitionIndex() {
    		return forkIndex;
		}

    	@Override
		public int getPartitionCount() {
    		return forkCount;
		}

		@Override
		public TripleSource partition(Name indexToPartition, com.msd.gin.halyard.common.RDFRole.Name role, ValueConstraint constraint) {
			return this;
		}
    }
}
