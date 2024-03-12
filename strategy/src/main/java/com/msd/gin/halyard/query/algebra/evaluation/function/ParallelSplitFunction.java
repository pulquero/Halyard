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

import static com.msd.gin.halyard.model.vocabulary.HALYARD.PARALLEL_SPLIT_FUNCTION;

import com.msd.gin.halyard.common.StatementIndices;
import com.msd.gin.halyard.query.algebra.Algebra;
import com.msd.gin.halyard.query.algebra.SkipVarsQueryModelVisitor;
import com.msd.gin.halyard.query.algebra.evaluation.PartitionableTripleSource;

import java.util.Arrays;
import java.util.List;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.algebra.FunctionCall;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.UpdateExpr;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.kohsuke.MetaInfServices;

/**
 *
 * @author Adam Sotona (MSD)
 */
@MetaInfServices(Function.class)
public final class ParallelSplitFunction implements Function {
	@Override
    public String getURI() {
        return PARALLEL_SPLIT_FUNCTION.stringValue();
    }

    @Override
    public Value evaluate(TripleSource ts, Value... args) throws ValueExprEvaluationException {
        if (args == null || args.length < 2) {
            throw new ValueExprEvaluationException(PARALLEL_SPLIT_FUNCTION.getLocalName() + " function has at least two mandatory arguments: <constant number of parallel forks> and <binding variable(s) to filter by>");
        }
        for (Value v : args) {
            if (v == null) {
                throw new ValueExprEvaluationException(PARALLEL_SPLIT_FUNCTION.getLocalName() + " function does not allow null values");
            }
        }

        final int forksArg;
        try {
	        forksArg = Integer.parseInt(args[0].stringValue());
	    } catch (NumberFormatException e) {
	        throw new ValueExprEvaluationException(PARALLEL_SPLIT_FUNCTION.getLocalName() + " function first argument must be numeric constant");
	    }
        if (forksArg < 1) {
            throw new ValueExprEvaluationException(PARALLEL_SPLIT_FUNCTION.getLocalName() + " function first argument must be > 0");
        }
        final int forkCount = toActualForkCount(forksArg);

        int forkIndex;
        if (ts instanceof PartitionableTripleSource) {
        	PartitionableTripleSource pts = (PartitionableTripleSource) ts;
        	forkIndex = pts.getPartitionIndex();
            if (forkIndex >= forkCount) {
            	throw new ValueExprEvaluationException(String.format("Fork index %d must be less than fork count %d", forkIndex, forkCount));
            }
        } else {
        	forkIndex = StatementIndices.NO_PARTITIONING;
        }
        return ts.getValueFactory().createLiteral(forkIndex == StatementIndices.NO_PARTITIONING || forkCount == 1 || (Math.floorMod(Arrays.hashCode(args), forkCount) == forkIndex));
    }

	@Override
	public Value evaluate(ValueFactory valueFactory, Value... args) throws ValueExprEvaluationException {
		return valueFactory.createLiteral(true);
	}

    public static int getNumberOfPartitionsFromFunctionArgument(String query, int stage, BindingSet bindings) throws ValueExprEvaluationException{
        if (stage >= 0) {
            List<UpdateExpr> exprs = QueryParserUtil.parseUpdate(QueryLanguage.SPARQL, query, null).getUpdateExprs();
            if (stage < exprs.size()) {
                return getNumberOfPartitionsFromFunctionArgument(exprs.get(stage), bindings);
            } else {
            	// no query - no partitions
            	return 0;
            }
        } else {
            return getNumberOfPartitionsFromFunctionArgument(QueryParserUtil.parseQuery(QueryLanguage.SPARQL, query, null).getTupleExpr(), bindings);
        }
    }

    public static int getNumberOfPartitionsFromFunctionArgument(QueryModelNode node, BindingSet bindings) throws ValueExprEvaluationException{
        ParallelSplitFunctionVisitor psfv = new ParallelSplitFunctionVisitor(bindings);
        node.visit(psfv);
        // there is at least one partition - everything
        return Math.max(1, toActualForkCount(psfv.forks));
    }

    public static int toActualForkCount(int forks) {
    	return floorToPowerOf2(forks);
    }

    private static int floorToPowerOf2(int i) {
    	return (i > 0) ? 1<<powerOf2BitCount(i) : 0;
    }

    public static int powerOf2BitCount(int i) {
    	if (i < 0) {
    		throw new IllegalArgumentException(String.format("Must be a non-negative integer: %d", i));
    	}
    	return (i > 0) ? Integer.SIZE - 1 - Integer.numberOfLeadingZeros(i) : 0;
    }


    private static final class ParallelSplitFunctionVisitor extends SkipVarsQueryModelVisitor<ValueExprEvaluationException> {
    	final BindingSet bindings;
		int forks = 0;

		ParallelSplitFunctionVisitor(BindingSet bindings) {
			this.bindings = bindings;
		}

		@Override
        public void meet(FunctionCall node) throws ValueExprEvaluationException {
            if (PARALLEL_SPLIT_FUNCTION.stringValue().equals(node.getURI())) {
                List<ValueExpr> args = node.getArgs();
                if (args.size() < 2) {
                	throw new ValueExprEvaluationException(PARALLEL_SPLIT_FUNCTION.getLocalName() + " function has at least two mandatory arguments: <constant number of parallel forks> and <binding variable(s) to filter by>");
                }
                int num;
                try {
                	Value v = Algebra.evaluateConstant(args.get(0), bindings);
                	if (v == null) {
                        throw new ValueExprEvaluationException(PARALLEL_SPLIT_FUNCTION.getLocalName() + " function first argument (number of forks) must be an integer constant > 0");
                	}
                    num = Integer.parseInt(v.stringValue());
                    if (num < 1) {
                        throw new ValueExprEvaluationException(PARALLEL_SPLIT_FUNCTION.getLocalName() + " function first argument (number of forks) must be an integer constant > 0");
                    }
                } catch (NumberFormatException ex) {
                    throw new ValueExprEvaluationException(PARALLEL_SPLIT_FUNCTION.getLocalName() + " function first argument (number of forks) must be an integer constant > 0", ex);
                }
                if (forks == 0) {
                    forks = num;
                } else if (forks != num) {
                    throw new ValueExprEvaluationException(PARALLEL_SPLIT_FUNCTION.getLocalName() + " function is used twice with different first argument (number of forks)");
                }
            }
        }
    }
}
