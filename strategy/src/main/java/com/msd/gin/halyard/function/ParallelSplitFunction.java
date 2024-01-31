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

import com.msd.gin.halyard.algebra.Algebra;
import com.msd.gin.halyard.algebra.SkipVarsQueryModelVisitor;
import com.msd.gin.halyard.strategy.HalyardEvaluationContext;

import java.util.Arrays;
import java.util.List;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.algebra.FunctionCall;
import org.eclipse.rdf4j.query.algebra.UpdateExpr;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryEvaluationContext;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.kohsuke.MetaInfServices;

/**
 *
 * @author Adam Sotona (MSD)
 */
@MetaInfServices(Function.class)
public final class ParallelSplitFunction implements ExtendedFunction {
	public static final int NO_FORKING = -1;

    @Override
    public String getURI() {
        return PARALLEL_SPLIT_FUNCTION.stringValue();
    }

    @Override
    public Value evaluate(TripleSource ts, QueryEvaluationContext qec, Value... args) throws ValueExprEvaluationException {
        if (args == null || args.length < 2) {
            throw new ValueExprEvaluationException(PARALLEL_SPLIT_FUNCTION.getLocalName() + " function has at least two mandatory arguments: <constant number of parallel forks> and <binding variable(s) to filter by>");
        }
        for (Value v : args) {
            if (v == null) {
                throw new ValueExprEvaluationException(PARALLEL_SPLIT_FUNCTION.getLocalName() + " function does not allow null values");
            }
        }
        try {
            int forks = Integer.parseInt(args[0].stringValue());
            if (forks < 1) {
                throw new ValueExprEvaluationException(PARALLEL_SPLIT_FUNCTION.getLocalName() + " function first argument must be > 0");
            }
            int forkIndex = getForkIndex(qec);
            int actualForkCount = toActualForkCount(forks);
            if (forkIndex >= actualForkCount) {
            	throw new ValueExprEvaluationException(String.format("Fork index %d must be less than fork count %d", forkIndex, actualForkCount));
            }
            return ts.getValueFactory().createLiteral(forkIndex == NO_FORKING || (Math.floorMod(Arrays.hashCode(args), actualForkCount) == forkIndex));
        } catch (NumberFormatException e) {
            throw new ValueExprEvaluationException(PARALLEL_SPLIT_FUNCTION.getLocalName() + " function first argument must be numeric constant");
        }
    }

    public static int getForkIndex(QueryEvaluationContext qec) {
    	return (qec instanceof HalyardEvaluationContext) ? ((HalyardEvaluationContext)qec).getForkIndex() : NO_FORKING;
    }

    public static int getNumberOfForksFromFunctionArgument(String query, int stage, BindingSet bindings) throws IllegalArgumentException{
        ParallelSplitFunctionVisitor psfv = new ParallelSplitFunctionVisitor(bindings);
        if (stage >= 0) {
            List<UpdateExpr> exprs = QueryParserUtil.parseUpdate(QueryLanguage.SPARQL, query, null).getUpdateExprs();
            if (stage < exprs.size()) {
                exprs.get(stage).visit(psfv);
            }
        } else {
            QueryParserUtil.parseQuery(QueryLanguage.SPARQL, query, null).getTupleExpr().visit(psfv);
        }
        return toActualForkCount(psfv.forks);
    }

    public static int toActualForkCount(int forks) {
    	return floorToPowerOf2(forks);
    }

    private static int floorToPowerOf2(int i) {
    	return (i > 0) ? 1<<powerOf2BitCount(i) : 0;
    }

    public static int powerOf2BitCount(int i) {
    	return (i > 0) ? Integer.SIZE - 1 - Integer.numberOfLeadingZeros(i) : 0;
    }


    private static final class ParallelSplitFunctionVisitor extends SkipVarsQueryModelVisitor<IllegalArgumentException> {
    	final BindingSet bindings;
		int forks = 0;

		ParallelSplitFunctionVisitor(BindingSet bindings) {
			this.bindings = bindings;
		}

		@Override
        public void meet(FunctionCall node) throws IllegalArgumentException {
            if (PARALLEL_SPLIT_FUNCTION.stringValue().equals(node.getURI())) {
                List<ValueExpr> args = node.getArgs();
                if (args.size() < 2) {
                	throw new IllegalArgumentException(PARALLEL_SPLIT_FUNCTION.getLocalName() + " function has at least two mandatory arguments: <constant number of parallel forks> and <binding variable(s) to filter by>");
                }
                int num;
                try {
                	Value v = Algebra.evaluateConstant(args.get(0), bindings);
                	if (v == null) {
                    	throw new IllegalArgumentException();
                	}
                    num = Integer.parseInt(v.stringValue());
                    if (num < 1) {
                    	throw new IllegalArgumentException();
                    }
                } catch (ClassCastException | IllegalArgumentException ex) {
                    throw new IllegalArgumentException(PARALLEL_SPLIT_FUNCTION.getLocalName() + " function first argument (number of forks) must be integer constant > 0");
                }
                if (forks == 0) {
                    forks = num;
                } else if (forks != num) {
                    throw new IllegalArgumentException(PARALLEL_SPLIT_FUNCTION.getLocalName() + " function is used twice with different first argument (number of forks)");
                }
            }
        }
    }
}
