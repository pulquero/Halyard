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
package com.msd.gin.halyard.function;

import static com.msd.gin.halyard.vocab.HALYARD.PARALLEL_SPLIT_FUNCTION;

import com.msd.gin.halyard.algebra.evaluation.EmptyTripleSource;
import com.msd.gin.halyard.strategy.HalyardEvaluationContext;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.impl.SimpleDataset;
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
		TripleSource ts = new EmptyTripleSource();
		ValueFactory vf = ts.getValueFactory();
		HalyardEvaluationContext evalContext = new HalyardEvaluationContext(new SimpleDataset(), vf, 1);
        new ParallelSplitFunction().evaluate(ts, evalContext);
    }

    @Test(expected = ValueExprEvaluationException.class)
    public void testEvaluateNullArgs() {
		TripleSource ts = new EmptyTripleSource();
		ValueFactory vf = ts.getValueFactory();
		HalyardEvaluationContext evalContext = new HalyardEvaluationContext(new SimpleDataset(), vf, 1);
		new ParallelSplitFunction().evaluate(ts, evalContext, vf.createLiteral(10), null);
    }

    @Test(expected = ValueExprEvaluationException.class)
    public void testEvaluateSingleArg() {
		TripleSource ts = new EmptyTripleSource();
		ValueFactory vf = ts.getValueFactory();
		HalyardEvaluationContext evalContext = new HalyardEvaluationContext(new SimpleDataset(), vf, 1);
        new ParallelSplitFunction().evaluate(ts, evalContext, vf.createLiteral(10));
    }

    @Test(expected = ValueExprEvaluationException.class)
    public void testEvaluateNegativeArg() {
		TripleSource ts = new EmptyTripleSource();
		ValueFactory vf = ts.getValueFactory();
		HalyardEvaluationContext evalContext = new HalyardEvaluationContext(new SimpleDataset(), vf, 1);
        new ParallelSplitFunction().evaluate(ts, evalContext, vf.createLiteral(-1), vf.createLiteral("hello"));
    }

    @Test(expected = ValueExprEvaluationException.class)
    public void testEvaluateNANArg() {
		TripleSource ts = new EmptyTripleSource();
		ValueFactory vf = ts.getValueFactory();
		HalyardEvaluationContext evalContext = new HalyardEvaluationContext(new SimpleDataset(), vf, 1);
        new ParallelSplitFunction().evaluate(ts, evalContext, vf.createLiteral("not a number"), vf.createLiteral("hello"));
    }

    @Test
    public void testEvaluatePartition0() {
		TripleSource ts = new EmptyTripleSource();
		ValueFactory vf = ts.getValueFactory();
		HalyardEvaluationContext evalContext = new HalyardEvaluationContext(new SimpleDataset(), vf, 0);
        assertTrue(((Literal)new ParallelSplitFunction().evaluate(ts, evalContext, vf.createLiteral(3), vf.createLiteral("hello"))).booleanValue());
    }

    @Test
    public void testEvaluatePartition1() {
		TripleSource ts = new EmptyTripleSource();
		ValueFactory vf = ts.getValueFactory();
		HalyardEvaluationContext evalContext = new HalyardEvaluationContext(new SimpleDataset(), vf, 1);
        assertFalse(((Literal)new ParallelSplitFunction().evaluate(ts, evalContext, vf.createLiteral(3), vf.createLiteral("hello"))).booleanValue());
    }

    @Test
    public void testGetNumberOfForksFromFunction() {
        assertEquals(8, ParallelSplitFunction.getNumberOfForksFromFunctionArgument("select * where {?s ?p ?o. filter <" + PARALLEL_SPLIT_FUNCTION.stringValue() + ">(8, ?s)}", -1));
    }

    @Test
    public void testGetNumberOfForksFromSelectWithoutFunction() {
        assertEquals(0, ParallelSplitFunction.getNumberOfForksFromFunctionArgument("select * where {?s ?p ?o.}", -1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetNumberOfForksNoArgs() {
        ParallelSplitFunction.getNumberOfForksFromFunctionArgument("select * where {?s ?p ?o. filter <" + PARALLEL_SPLIT_FUNCTION.stringValue() + ">()}", -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetNumberOfForksNANArg() {
        ParallelSplitFunction.getNumberOfForksFromFunctionArgument("select * where {?s ?p ?o. filter <" + PARALLEL_SPLIT_FUNCTION.stringValue() + ">(\"not a number\", ?s)}", -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetNumberOfForksVarArg() {
        ParallelSplitFunction.getNumberOfForksFromFunctionArgument("select * where {?s ?p ?o. filter <" + PARALLEL_SPLIT_FUNCTION.stringValue() + ">(?p, ?s)}", -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetNumberOfForksNegativeArg() {
        ParallelSplitFunction.getNumberOfForksFromFunctionArgument("select * where {?s ?p ?o. filter <" + PARALLEL_SPLIT_FUNCTION.stringValue() + ">(-1, ?s)}", -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetNumberOfForksDoubleFunction() {
        ParallelSplitFunction.getNumberOfForksFromFunctionArgument("select * where {?s ?p ?o. filter <" + PARALLEL_SPLIT_FUNCTION.stringValue() + ">(8, ?s) filter <" + PARALLEL_SPLIT_FUNCTION.stringValue() + ">(3, ?s)}", -1);
    }

    @Test
    public void testGetNumberOfForksDoubleMatchingFunction() {
        assertEquals(8, ParallelSplitFunction.getNumberOfForksFromFunctionArgument("select * where {?s ?p ?o. filter <" + PARALLEL_SPLIT_FUNCTION.stringValue() + ">(8, ?s) filter <" + PARALLEL_SPLIT_FUNCTION.stringValue() + ">(8, ?s) filter <http://whatever/function>()}", -1));
    }

    @Test
    public void testGetNumberOfForksFromUpdate() {
        String query = "insert {?s ?p ?o} where {?s ?p ?o. filter <" + PARALLEL_SPLIT_FUNCTION.stringValue() + ">(16, ?s)};"
            + "clear all;"
            + "delete {?s ?p ?o} where {?s ?p ?o. filter <" + PARALLEL_SPLIT_FUNCTION.stringValue() + ">(8, ?s)}";
        assertEquals(16, ParallelSplitFunction.getNumberOfForksFromFunctionArgument(query, 0));
        assertEquals(0, ParallelSplitFunction.getNumberOfForksFromFunctionArgument(query, 1));
        assertEquals(8, ParallelSplitFunction.getNumberOfForksFromFunctionArgument(query, 2));
        assertEquals(0, ParallelSplitFunction.getNumberOfForksFromFunctionArgument(query, 3));
    }

}
