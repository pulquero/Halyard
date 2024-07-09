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
package com.msd.gin.halyard.strategy;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.msd.gin.halyard.common.JavaObjectLiteral;
import com.msd.gin.halyard.query.BindingSetPipe;
import com.msd.gin.halyard.query.BindingSetPipeQueryEvaluationStep;
import com.msd.gin.halyard.query.ValuePipe;
import com.msd.gin.halyard.query.ValuePipeQueryValueEvaluationStep;
import com.msd.gin.halyard.query.algebra.evaluation.ExtendedTripleSource;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.rdf4j.common.net.ParsedIRI;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.datatypes.XMLDatatypeUtil;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryInterruptedException;
import org.eclipse.rdf4j.query.algebra.And;
import org.eclipse.rdf4j.query.algebra.BNodeGenerator;
import org.eclipse.rdf4j.query.algebra.BinaryValueOperator;
import org.eclipse.rdf4j.query.algebra.Bound;
import org.eclipse.rdf4j.query.algebra.Coalesce;
import org.eclipse.rdf4j.query.algebra.Compare;
import org.eclipse.rdf4j.query.algebra.CompareAll;
import org.eclipse.rdf4j.query.algebra.CompareAny;
import org.eclipse.rdf4j.query.algebra.Datatype;
import org.eclipse.rdf4j.query.algebra.Exists;
import org.eclipse.rdf4j.query.algebra.FunctionCall;
import org.eclipse.rdf4j.query.algebra.IRIFunction;
import org.eclipse.rdf4j.query.algebra.If;
import org.eclipse.rdf4j.query.algebra.In;
import org.eclipse.rdf4j.query.algebra.IsBNode;
import org.eclipse.rdf4j.query.algebra.IsLiteral;
import org.eclipse.rdf4j.query.algebra.IsNumeric;
import org.eclipse.rdf4j.query.algebra.IsResource;
import org.eclipse.rdf4j.query.algebra.IsURI;
import org.eclipse.rdf4j.query.algebra.Label;
import org.eclipse.rdf4j.query.algebra.Lang;
import org.eclipse.rdf4j.query.algebra.LangMatches;
import org.eclipse.rdf4j.query.algebra.ListMemberOperator;
import org.eclipse.rdf4j.query.algebra.LocalName;
import org.eclipse.rdf4j.query.algebra.MathExpr;
import org.eclipse.rdf4j.query.algebra.NAryValueOperator;
import org.eclipse.rdf4j.query.algebra.Namespace;
import org.eclipse.rdf4j.query.algebra.Not;
import org.eclipse.rdf4j.query.algebra.Or;
import org.eclipse.rdf4j.query.algebra.Regex;
import org.eclipse.rdf4j.query.algebra.SameTerm;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Str;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UnaryValueOperator;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.ValueExprTripleRef;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.eclipse.rdf4j.query.algebra.evaluation.function.datetime.Now;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryEvaluationContext;
import org.eclipse.rdf4j.query.algebra.evaluation.util.QueryEvaluationUtility;
import org.eclipse.rdf4j.query.algebra.evaluation.util.XMLDatatypeMathUtil;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;

/**
 * Evaluates "value" expressions (low level language functions and operators, instances of {@link ValueExpr}) from SPARQL such as 'Regex', 'IsURI', math expressions etc.
 *
 * @author Adam Sotona (MSD)
 */
class HalyardValueExprEvaluation {

	private static final class RegexCacheLoader implements CacheLoader<Pair<String,String>,JavaObjectLiteral<Pattern>> {
		@Override
		public JavaObjectLiteral<Pattern> load(Pair<String, String> key) throws ValueExprEvaluationException {
			String ptn = key.getLeft();
			String flags = key.getRight();
			int f = 0;
            for (char c : flags.toCharArray()) {
                switch (c) {
                    case 's':
                        f |= Pattern.DOTALL;
                        break;
                    case 'm':
                        f |= Pattern.MULTILINE;
                        break;
                    case 'i':
                        f |= Pattern.CASE_INSENSITIVE;
                        f |= Pattern.UNICODE_CASE;
                        break;
                    case 'x':
                        f |= Pattern.COMMENTS;
                        break;
                    case 'd':
                        f |= Pattern.UNIX_LINES;
                        break;
                    case 'u':
                        f |= Pattern.UNICODE_CASE;
                        break;
                    default:
                        throw new ValueExprEvaluationException(flags);
                }
            }
            return JavaObjectLiteral.of(Pattern.compile(ptn, f), Pattern.class);
		}
	}
	private static final LoadingCache<Pair<String,String>,JavaObjectLiteral<Pattern>> REGEX_CACHE = Caffeine.newBuilder().maximumSize(100).expireAfterAccess(1L, TimeUnit.DAYS).build(new RegexCacheLoader());

    private final HalyardEvaluationStrategy parentStrategy;
    private final TripleSource tripleSource;
    private final ValueFactory valueFactory;
    private final Literal TRUE;
    private final Literal FALSE;
    private final Literal EMPTY_STRING;
    private final ValueOrError OK_TRUE;
    private final ValueOrError OK_FALSE;
    private LoadingCache<String,Literal> langTagCache;
	private int pollTimeoutMillis;

	HalyardValueExprEvaluation(HalyardEvaluationStrategy parentStrategy,
			TripleSource tripleSource, int pollTimeoutMillis) {
        this.parentStrategy = parentStrategy;
        this.tripleSource = tripleSource;
        this.valueFactory = tripleSource.getValueFactory();
        this.TRUE = valueFactory.createLiteral(true);
        this.FALSE = valueFactory.createLiteral(false);
        this.EMPTY_STRING = valueFactory.createLiteral("");
        this.OK_TRUE = ValueOrError.ok(TRUE);
        this.OK_FALSE = ValueOrError.ok(FALSE);
        this.pollTimeoutMillis = pollTimeoutMillis;
    }

    /**
     * Precompiles the given {@link ValueExpr}
     */
	ValuePipeQueryValueEvaluationStep precompile(ValueExpr expr, QueryEvaluationContext evalContext) throws ValueExprEvaluationException, QueryEvaluationException {
		ValuePipeEvaluationStep step = precompileValueExpr(expr, evalContext);
		return new ValuePipeQueryValueEvaluationStep() {
			@Override
			public void evaluate(ValuePipe parent, BindingSet bindings) {
				step.evaluate(parent, bindings);
			}
			@Override
			public Value evaluate(BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
				ValueOrError result = get(step, bindings);
				if (result.isOk()) {
					return result.getValue();
				} else {
					throw new ValueExprEvaluationException(result.getMessage());
				}
			}
		};
	}

	private ValuePipeEvaluationStep precompileValueExpr(ValueExpr expr, QueryEvaluationContext evalContext) throws ValueExprEvaluationException, QueryEvaluationException {
        if (expr instanceof Var) {
            return precompileVar((Var) expr);
        } else if (expr instanceof ValueConstant) {
            return precompileValueConstant((ValueConstant) expr);
        } else if (expr instanceof BNodeGenerator) {
            return precompileBNodeGenerator((BNodeGenerator) expr, evalContext);
        } else if (expr instanceof Bound) {
            return precompileBound((Bound) expr);
        } else if (expr instanceof Str) {
            return precompileStr((Str) expr, evalContext);
        } else if (expr instanceof Label) {
            return precompileLabel((Label) expr, evalContext);
        } else if (expr instanceof Lang) {
            return precompileLang((Lang) expr, evalContext);
        } else if (expr instanceof LangMatches) {
            return precompileLangMatches((LangMatches) expr, evalContext);
        } else if (expr instanceof Datatype) {
            return precompileDatatype((Datatype) expr, evalContext);
        } else if (expr instanceof Namespace) {
            return precompileNamespace((Namespace) expr, evalContext);
        } else if (expr instanceof LocalName) {
            return precompileLocalName((LocalName) expr, evalContext);
        } else if (expr instanceof IsResource) {
            return precompileIsResource((IsResource) expr, evalContext);
        } else if (expr instanceof IsURI) {
            return precompileIsURI((IsURI) expr, evalContext);
        } else if (expr instanceof IsBNode) {
            return precompileIsBNode((IsBNode) expr, evalContext);
        } else if (expr instanceof IsLiteral) {
            return precompileIsLiteral((IsLiteral) expr, evalContext);
        } else if (expr instanceof IsNumeric) {
            return precompileIsNumeric((IsNumeric) expr, evalContext);
        } else if (expr instanceof IRIFunction) {
            return precompileIRIFunction((IRIFunction) expr, evalContext);
        } else if (expr instanceof Regex) {
            return precompileRegex((Regex) expr, evalContext);
        } else if (expr instanceof Coalesce) {
            return precompileCoalesce((Coalesce) expr, evalContext);
        } else if (expr instanceof FunctionCall) {
            return precompileFunctionCall((FunctionCall) expr, evalContext);
        } else if (expr instanceof And) {
            return precompileAnd((And) expr, evalContext);
        } else if (expr instanceof Or) {
            return precompileOr((Or) expr, evalContext);
        } else if (expr instanceof Not) {
            return precompileNot((Not) expr, evalContext);
        } else if (expr instanceof SameTerm) {
            return precompileSameTerm((SameTerm) expr, evalContext);
        } else if (expr instanceof Compare) {
            return precompileCompare((Compare) expr, evalContext);
        } else if (expr instanceof MathExpr) {
            return precompileMathExpr((MathExpr) expr, evalContext);
        } else if (expr instanceof In) {
            return precompileIn((In) expr, evalContext);
        } else if (expr instanceof CompareAny) {
            return precompileCompareAny((CompareAny) expr, evalContext);
        } else if (expr instanceof CompareAll) {
            return precompileCompareAll((CompareAll) expr, evalContext);
        } else if (expr instanceof Exists) {
            return precompileExists((Exists) expr, evalContext);
        } else if (expr instanceof If) {
            return precompileIf((If) expr, evalContext);
        } else if (expr instanceof ListMemberOperator) {
            return precompileListMemberOperator((ListMemberOperator) expr, evalContext);
		} else if (expr instanceof ValueExprTripleRef) {
			return precompileValueExprTripleRef((ValueExprTripleRef) expr );
        } else if (expr == null) {
            throw new IllegalArgumentException("expr must not be null");
        } else {
            throw new QueryEvaluationException("Unsupported value expr type: " + expr.getClass());
        }
    }

	private ValuePipeEvaluationStep precompileUnaryValueExpr(ValuePipeEvaluationStep argStep, java.util.function.Function<ValuePipeEvaluationStep,ValuePipeEvaluationStep> operator) {
		if (argStep.isConstant()) {
			ValuePipeEvaluationStep operation = operator.apply(argStep);
			return evaluateForEmptyBindingSetAndPrecompile(operation);
		} else {
			return operator.apply(argStep);
		}
	}

	private ValuePipeEvaluationStep precompileUnaryValueOperator(UnaryValueOperator node, java.util.function.Function<ValuePipeEvaluationStep,ValuePipeEvaluationStep> operator, QueryEvaluationContext evalContext) {
		return precompileUnaryValueExpr(precompileValueExpr(node.getArg(), evalContext), operator);
	}

	private ValuePipeEvaluationStep precompileBinaryValueExpr(ValuePipeEvaluationStep leftStep, ValuePipeEvaluationStep rightStep, BiFunction<ValuePipeEvaluationStep,ValuePipeEvaluationStep,ValuePipeEvaluationStep> operator) {
		if (leftStep.isConstant() && rightStep.isConstant()) {
			ValuePipeEvaluationStep operation = operator.apply(leftStep, rightStep);
			return evaluateForEmptyBindingSetAndPrecompile(operation);
		} else {
			return operator.apply(leftStep, rightStep);
		}
	}

	private ValuePipeEvaluationStep precompileBinaryValueOperator(BinaryValueOperator node, BiFunction<ValuePipeEvaluationStep,ValuePipeEvaluationStep,ValuePipeEvaluationStep> operator, QueryEvaluationContext evalContext) {
		return precompileBinaryValueExpr(precompileValueExpr(node.getLeftArg(), evalContext), precompileValueExpr(node.getRightArg(), evalContext), operator);
	}

	private ValuePipeEvaluationStep precompileNAryValueExpr(ValuePipeEvaluationStep[] steps, java.util.function.Function<ValuePipeEvaluationStep[],ValuePipeEvaluationStep> operator) {
		boolean allConstant = true;
		for (ValuePipeEvaluationStep step : steps) {
			if (!step.isConstant()) {
				allConstant = false;
				break;
			}
		}
		if (allConstant) {
			ValuePipeEvaluationStep operation = operator.apply(steps);
			return evaluateForEmptyBindingSetAndPrecompile(operation);
		} else {
			return operator.apply(steps);
		}
	}

	private ValuePipeEvaluationStep precompileNAryValueOperator(NAryValueOperator node, java.util.function.Function<ValuePipeEvaluationStep[],ValuePipeEvaluationStep> operator, QueryEvaluationContext evalContext) {
    	List<ValueExpr> args = node.getArguments();
    	ValuePipeEvaluationStep[] argSteps = new ValuePipeEvaluationStep[args.size()];
    	for (int i=0; i<argSteps.length; i++) {
    		argSteps[i] = precompileValueExpr(args.get(i), evalContext);
    	}
		return precompileNAryValueExpr(argSteps, operator);
	}

	/**
     * Precompiles a {@link Var} query model node.
     * @param var
     */
    private ValuePipeEvaluationStep precompileVar(Var var) throws ValueExprEvaluationException, QueryEvaluationException {
        Value value = var.getValue();
        if (value != null) {
	    	return new ConstantValuePipeEvaluationStep(value);
        } else {
        	String varName = var.getName();
	    	return (parent, bindings) -> {
		        Value bvalue = bindings.getValue(varName);
		        if (bvalue != null) {
		        	parent.push(bvalue);
		        } else {
		            parent.handleValueError(String.format("Var %s has no value (%s)", varName, bindings));
		        }
	    	};
        }
    }

    /**
     * Precompiles a {@link ValueConstant} query model node.
     * @param valueConstant
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private ValuePipeEvaluationStep precompileValueConstant(ValueConstant valueConstant) throws ValueExprEvaluationException, QueryEvaluationException {
    	return new ConstantValuePipeEvaluationStep(valueConstant.getValue());
    }

    /**
     * Precompiles a {@link BNodeGenerator} node
     * @param node the node to evaluate
     * @param evalContext
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private ValuePipeEvaluationStep precompileBNodeGenerator(BNodeGenerator node, QueryEvaluationContext evalContext) throws ValueExprEvaluationException, QueryEvaluationException {
        ValueExpr nodeIdExpr = node.getNodeIdExpr();
        if (nodeIdExpr != null) {
        	ValuePipeEvaluationStep step = precompileValueExpr(nodeIdExpr, evalContext);
	    	return (parent, bindings) -> {
	    		step.evaluate(new ConvertingValuePipe(parent, nodeId -> {
    	            if (nodeId.isLiteral()) {
    	                String nodeLabel = ((Literal) nodeId).getLabel() + (bindings.toString().hashCode());
    	                return ValueOrError.ok(valueFactory.createBNode(nodeLabel));
    	            } else {
    	                return ValueOrError.fail("BNODE function argument must be a literal");
    	            }
	    		}), bindings);
	    	};
        } else {
        	return (parent, bindings) -> parent.push(valueFactory.createBNode());
        }
    }

    /**
     * Precompiles a {@link Bound} node
     * @param node the node to evaluate
     * @throws QueryEvaluationException
     */
    private ValuePipeEvaluationStep precompileBound(Bound node) throws QueryEvaluationException {
    	ValuePipeEvaluationStep step = precompileVar(node.getArg());
    	return (parent, bindings) -> {
    		step.evaluate(new ValuePipe(parent) {
    			@Override
    			protected void next(Value v) {
    				parent.push(TRUE);
    			}
    			@Override
    			public void handleValueError(String msg) {
   					parent.push(FALSE);
    			}
    		}, bindings);
    	};
    }

    /**
     * Precompiles a {@link Str} node
     * @param node the node to evaluate
     * @param evalContext
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private ValuePipeEvaluationStep precompileStr(Str node, QueryEvaluationContext evalContext) throws ValueExprEvaluationException, QueryEvaluationException {
    	return precompileUnaryValueOperator(node, step -> {
	    	return (parent, bindings) -> {
	    		step.evaluate(new ConvertingValuePipe(parent, argValue -> {
					Literal str;
			        if (argValue.isIRI()) {
			            str = valueFactory.createLiteral(argValue.toString());
			        } else if (argValue.isLiteral()) {
			            Literal literal = (Literal) argValue;
			            if (QueryEvaluationUtility.isSimpleLiteral(literal)) {
			                str = literal;
			            } else {
			                str = valueFactory.createLiteral(literal.getLabel());
			            }
			        } else {
			        	return ValueOrError.fail("Str");
			        }
		        	return ValueOrError.ok(str);
	    		}), bindings);
	    	};
    	}, evalContext);
    }

    /**
     * Precompiles a {@link Label} node
     * @param node the node to evaluate
     * @param evalContext
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private ValuePipeEvaluationStep precompileLabel(Label node, QueryEvaluationContext evalContext) throws ValueExprEvaluationException, QueryEvaluationException {
        // FIXME: deprecate Label in favour of Str(?)
    	return precompileUnaryValueOperator(node, step -> {
	    	return (parent, bindings) -> {
	    		step.evaluate(new ConvertingValuePipe(parent, argValue -> {
			        if (argValue.isLiteral()) {
			            Literal literal = (Literal) argValue;
	    				Literal str;
			            if (QueryEvaluationUtility.isSimpleLiteral(literal)) {
			                str = literal;
			            } else {
			                str = valueFactory.createLiteral(literal.getLabel());
			            }
			        	return ValueOrError.ok(str);
			        } else {
			            return ValueOrError.fail("Label - not a literal");
			        }
	    		}), bindings);
	    	};
    	}, evalContext);
    }

    /**
     * Precompiles a {@link Lang} node
     * @param node the node to evaluate
     * @param evalContext
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private ValuePipeEvaluationStep precompileLang(Lang node, QueryEvaluationContext evalContext) throws ValueExprEvaluationException, QueryEvaluationException {
    	if (langTagCache == null) {
    		langTagCache = Caffeine.newBuilder().maximumSize(100).build(valueFactory::createLiteral);
    	}

    	return precompileUnaryValueOperator(node, step -> {
	    	return (parent, bindings) -> {
	    		step.evaluate(new ConvertingValuePipe(parent, argValue -> {
			        if (argValue.isLiteral()) {
			            Literal literal = (Literal) argValue;
			            Literal langTag = literal.getLanguage().map(langTagCache::get).orElse(EMPTY_STRING);
			        	return ValueOrError.ok(langTag);
			        } else {
			        	return ValueOrError.fail("Lang - not a literal");
			        }
	    		}), bindings);
	    	};
    	}, evalContext);
    }

    /**
     * Precompiles a {@link Datatype} node
     * @param node the node to evaluate
     * @param evalContext
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private ValuePipeEvaluationStep precompileDatatype(Datatype node, QueryEvaluationContext evalContext) throws ValueExprEvaluationException, QueryEvaluationException {
    	return precompileUnaryValueOperator(node, step -> {
	    	return (parent, bindings) -> {
	    		step.evaluate(new ConvertingValuePipe(parent, v -> {
			        if (v.isLiteral()) {
			            Literal literal = (Literal) v;
	    				IRI dt;
			            if (literal.getDatatype() != null) {
			                // literal with datatype
			                dt = literal.getDatatype();
			            } else if (literal.getLanguage() != null) {
			                dt = RDF.LANGSTRING;
			            } else {
			                // simple literal
			                dt = XSD.STRING;
			            }
			        	return ValueOrError.ok(dt);
			        } else {
			        	return ValueOrError.fail("Datatype - not a literal");
			        }
	    		}), bindings);
	    	};
    	}, evalContext);
    }

    /**
     * Precompiles a {@link Namespace} node
     * @param node the node to evaluate
     * @param evalContext
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private ValuePipeEvaluationStep precompileNamespace(Namespace node, QueryEvaluationContext evalContext) throws ValueExprEvaluationException, QueryEvaluationException {
    	return precompileUnaryValueOperator(node, step -> {
	    	return (parent, bindings) -> {
	    		step.evaluate(new ConvertingValuePipe(parent, argValue -> {
			        if (argValue.isIRI()) {
			            IRI uri = (IRI) argValue;
			            return ValueOrError.ok(valueFactory.createIRI(uri.getNamespace()));
			        } else {
			            return ValueOrError.fail("Namespace - not an IRI");
			        }
	    		}), bindings);
	    	};
    	}, evalContext);
    }

    /**
     * Precompiles a LocalName node
     * @param node the node to evaluate
     * @param evalContext
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private ValuePipeEvaluationStep precompileLocalName(LocalName node, QueryEvaluationContext evalContext) throws ValueExprEvaluationException, QueryEvaluationException {
    	return precompileUnaryValueOperator(node, step -> {
	    	return (parent, bindings) -> {
	    		step.evaluate(new ConvertingValuePipe(parent, argValue -> {
			        if (argValue.isIRI()) {
			            IRI uri = (IRI) argValue;
			            return ValueOrError.ok(valueFactory.createLiteral(uri.getLocalName()));
			        } else {
			            return ValueOrError.fail("LocalName - not an IRI");
			        }
	    		}), bindings);
	    	};
    	}, evalContext);
    }

    /**
     * Determines whether the operand (a variable) contains a Resource.
     */
    private ValuePipeEvaluationStep precompileIsResource(IsResource node, QueryEvaluationContext evalContext) throws ValueExprEvaluationException, QueryEvaluationException {
    	return precompileUnaryValueOperator(node, step -> {
	    	return (parent, bindings) -> {
	    		step.evaluate(new ConvertingValuePipe(parent, argValue -> ok(argValue.isResource())), bindings);
	    	};
    	}, evalContext);
    }

    /**
     * Determines whether the operand (a variable) contains a URI.
     */
    private ValuePipeEvaluationStep precompileIsURI(IsURI node, QueryEvaluationContext evalContext) throws ValueExprEvaluationException, QueryEvaluationException {
    	return precompileUnaryValueOperator(node, step -> {
	    	return (parent, bindings) -> {
	    		step.evaluate(new ConvertingValuePipe(parent, argValue -> ok(argValue.isIRI())), bindings);
	    	};
    	}, evalContext);
    }

    /**
     * Determines whether the operand (a variable) contains a BNode.
     */
    private ValuePipeEvaluationStep precompileIsBNode(IsBNode node, QueryEvaluationContext evalContext) throws ValueExprEvaluationException, QueryEvaluationException {
    	return precompileUnaryValueOperator(node, step -> {
	    	return (parent, bindings) -> {
	    		step.evaluate(new ConvertingValuePipe(parent, argValue -> ok(argValue.isBNode())), bindings);
	    	};
    	}, evalContext);
    }

    /**
     * Determines whether the operand (a variable) contains a Literal.
     */
    private ValuePipeEvaluationStep precompileIsLiteral(IsLiteral node, QueryEvaluationContext evalContext) throws ValueExprEvaluationException, QueryEvaluationException {
    	return precompileUnaryValueOperator(node, step -> {
	    	return (parent, bindings) -> {
	    		step.evaluate(new ConvertingValuePipe(parent, argValue -> ok(argValue.isLiteral())), bindings);
	    	};
    	}, evalContext);
    }

    /**
     * Determines whether the operand (a variable) contains a numeric datatyped literal, i.e. a literal with datatype xsd:float, xsd:double, xsd:decimal, or a
     * derived datatype of xsd:decimal.
     */
    private ValuePipeEvaluationStep precompileIsNumeric(IsNumeric node, QueryEvaluationContext evalContext) throws ValueExprEvaluationException, QueryEvaluationException {
    	return precompileUnaryValueOperator(node, step -> {
	    	return (parent, bindings) -> {
	    		step.evaluate(new ConvertingValuePipe(parent, argValue -> {
					Literal result;
			        if (argValue.isLiteral()) {
			            Literal lit = (Literal) argValue;
			            IRI datatype = lit.getDatatype();
			            result = XMLDatatypeUtil.isNumericDatatype(datatype) ? TRUE : FALSE;
			        } else {
			            result = FALSE;
			        }
			        return ValueOrError.ok(result);
	    		}), bindings);
	    	};
    	}, evalContext);
    }

    /**
     * Creates a URI from the operand value (a plain literal or a URI).
     *
     * @param node the node to evaluate, represents an invocation of the SPARQL IRI function
     * @param evalContext
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private ValuePipeEvaluationStep precompileIRIFunction(IRIFunction node, QueryEvaluationContext evalContext) throws ValueExprEvaluationException, QueryEvaluationException {
    	return precompileUnaryValueOperator(node, step -> {
	    	return (parent, bindings) -> {
	    		step.evaluate(new ValuePipe(parent) {
	    			@Override
	    			protected void next(Value argValue) {
	    				IRI result = null;
	    				String errMsg = "IRIFunction";
	    		        if (argValue.isLiteral()) {
	    		            String uriString = ((Literal) argValue).getLabel();
	    		            final String baseURI = node.getBaseURI();
	    		            try {
			                    ParsedIRI iri = ParsedIRI.create(uriString);
			                    if (!iri.isAbsolute()) {
		                            // uri string may be a relative reference.
				                    if (baseURI != null) {
			                            uriString = ParsedIRI.create(baseURI).resolve(iri).toString();
				                    } else {
				                        errMsg = "not an absolute IRI reference: " + uriString;
				                    }
			                    }
	    		                result = valueFactory.createIRI(uriString);
	    		            } catch (IllegalArgumentException e) {
	    		                errMsg = "not a valid IRI reference: " + uriString;
	    		            }
	    		        } else if (argValue.isIRI()) {
	    		            result = ((IRI) argValue);
	    		        }
	
	    		        if (result != null) {
	    		        	parent.push(result);
	    		        } else {
	    		        	parent.handleValueError(errMsg);
	    		        }
	    			}
	    		}, bindings);
	    	};
    	}, evalContext);
    }

    /**
     * Determines whether the two operands match according to the <code>regex</code> operator.
     */
    private ValuePipeEvaluationStep precompileRegex(Regex node, QueryEvaluationContext evalContext) throws ValueExprEvaluationException, QueryEvaluationException {
    	BiFunction<ValuePipeEvaluationStep,ValuePipeEvaluationStep,ValuePipeEvaluationStep> patternOperator = (pargStep, flagsStep) -> {
    		return (parent, bindings) -> {
        		AtomicInteger args = new AtomicInteger(2);
        		AtomicReference<Value> pargRef = new AtomicReference<>();
        		AtomicReference<Value> fargRef = new AtomicReference<>();
        		Supplier<ValueOrError> resultSupplier = () -> {
        	        Value parg = pargRef.get();
        	        Value farg = fargRef.get();
        	        if (QueryEvaluationUtility.isSimpleLiteral(parg) && QueryEvaluationUtility.isSimpleLiteral(farg)) {
        	            String ptn = ((Literal) parg).getLabel();
        	            String flags = ((Literal) farg).getLabel();
        	            try {
    	    	            JavaObjectLiteral<Pattern> pattern = REGEX_CACHE.get(Pair.of(ptn, flags));
    	    	            return ValueOrError.ok(pattern);
        	            } catch (ValueExprEvaluationException e) {
        	            	return ValueOrError.fail(e.getCause().getMessage());
        	            }
        	        } else {
        	        	return ValueOrError.fail("Regex - pattern/flags is not a simple literal");
        	        }
        		};
        		pargStep.evaluate(new MultiValuePipe(parent, args, v -> pargRef.set(v), resultSupplier), bindings);
           		flagsStep.evaluate(new MultiValuePipe(parent, args, v -> fargRef.set(v), resultSupplier), bindings);
    		};
    	};
    	BiFunction<ValuePipeEvaluationStep,ValuePipeEvaluationStep,ValuePipeEvaluationStep> matchOperator = (argStep, patternStep) -> {
    		return (parent, bindings) -> {
    			patternStep.evaluate(new ValuePipe(parent) {
    				@Override
    				protected void next(Value v) {
    					Pattern pattern = ((JavaObjectLiteral<Pattern>)v).objectValue();
    					argStep.evaluate(new ValuePipe(parent) {
    						@Override
    						protected void next(Value arg) {
    	    	    	        if (QueryEvaluationUtility.isStringLiteral(arg)) {
    	    	    	        	String text = ((Literal) arg).getLabel();
    	    	    	            boolean result = pattern.matcher(text).find();
    	    	    	            parent.push(result ? TRUE : FALSE);
    	    	    	        } else {
    	            	        	parent.handleValueError("Regex - text is not a simple literal");
    	    	    	        }
    						}
    					}, bindings);
    				}
    			}, bindings);
    		};
    	};
    	ValuePipeEvaluationStep argStep = precompileValueExpr(node.getArg(), evalContext);
    	ValuePipeEvaluationStep pargStep = precompileValueExpr(node.getPatternArg(), evalContext);
    	ValuePipeEvaluationStep flagsStep;
    	if (node.getFlagsArg() != null) {
    		flagsStep = precompileValueExpr(node.getFlagsArg(), evalContext);
    	} else {
    		flagsStep = new ConstantValuePipeEvaluationStep(EMPTY_STRING); // default flags
    	}
    	ValuePipeEvaluationStep compilePatternStep = precompileBinaryValueExpr(pargStep, flagsStep, patternOperator);
    	ValuePipeEvaluationStep fullStep = precompileBinaryValueExpr(argStep, compilePatternStep, matchOperator);
    	return fullStep;
    }

    /**
     * Determines whether the language tag or the node matches the language argument of the node
     * @param node the node to evaluate
     * @param evalContext
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private ValuePipeEvaluationStep precompileLangMatches(LangMatches node, QueryEvaluationContext evalContext) throws ValueExprEvaluationException, QueryEvaluationException {
    	return precompileBinaryValueOperator(node, (langTagStep, langRangeStep) -> {
	    	return (parent, bindings) -> {
	    		AtomicInteger args = new AtomicInteger(2);
	    		AtomicReference<Value> langTagRef = new AtomicReference<>();
	    		AtomicReference<Value> langRangeRef = new AtomicReference<>();
	    		Supplier<ValueOrError> resultSupplier = () -> {
	    	    	Value langTagValue = langTagRef.get();
	    	        Value langRangeValue = langRangeRef.get();
	    	        if (QueryEvaluationUtility.isSimpleLiteral(langTagValue)
	    	                && QueryEvaluationUtility.isSimpleLiteral(langRangeValue)) {
	    	            String langTag = ((Literal) langTagValue).getLabel();
	    	            String langRange = ((Literal) langRangeValue).getLabel();
	    	            boolean result = false;
	    	            if (langRange.equals("*")) {
	    	                result = langTag.length() > 0;
	    	            } else if (langTag.length() == langRange.length()) {
	    	                result = langTag.equalsIgnoreCase(langRange);
	    	            } else if (langTag.length() > langRange.length()) {
	    	                // check if the range is a prefix of the tag
	    	                String prefix = langTag.substring(0, langRange.length());
	    	                result = prefix.equalsIgnoreCase(langRange) && langTag.charAt(langRange.length()) == '-';
	    	            }
	    	            return ok(result);
	    	        } else {
	    	        	return ValueOrError.fail("LangMatches");
	    	        }
	    		};
	    		langTagStep.evaluate(new MultiValuePipe(parent, args, v -> langTagRef.set(v), resultSupplier), bindings);
	    		langRangeStep.evaluate(new MultiValuePipe(parent, args, v -> langRangeRef.set(v), resultSupplier), bindings);
	    	};
    	}, evalContext);
    }

    /**
     * Evaluates a function.
     */
    private ValuePipeEvaluationStep precompileFunctionCall(FunctionCall node, QueryEvaluationContext evalContext) throws ValueExprEvaluationException, QueryEvaluationException {
    	Function function = parentStrategy.functionRegistry.get(node.getURI()).orElseThrow(() -> new QueryEvaluationException(String.format("Unknown function '%s'", node.getURI())));

		// the NOW function is a special case as it needs to keep a shared return
        // value for the duration of the query.
        if (function instanceof Now) {
            return precompileNow((Now) function, evalContext);
        }

        List<ValueExpr> args = node.getArgs();
        ValuePipeEvaluationStep[] argSteps = new ValuePipeEvaluationStep[args.size()];
        for (int i = 0; i < args.size(); i++) {
            argSteps[i] = precompileValueExpr(args.get(i), evalContext);
        }
        return (parent, bs) -> {
        	if (argSteps.length > 0) {
	        	AtomicInteger argsRemaining = new AtomicInteger(argSteps.length);
	        	AtomicReferenceArray<Value> argValues = new AtomicReferenceArray<>(argSteps.length);
	        	Supplier<ValueOrError> resultSupplier = () -> {
					Value[] arr = new Value[argValues.length()];
					for (int i=0; i<arr.length; i++) {
						arr[i] = argValues.get(i);
					}
					Value result;
			        try {
			        	result = function.evaluate(tripleSource, arr);
			        } catch (ValueExprEvaluationException e) {
			        	return ValueOrError.fail(e.getMessage());
			        }
			        return ValueOrError.ok(result);
	        	};
	        	for (int i = 0; i < argSteps.length; i++) {
	        		final int idx = i;
	        		argSteps[i].evaluate(new MultiValuePipe(parent, argsRemaining, v -> argValues.set(idx, v), resultSupplier), bs);
	        	}
        	} else {
        		try {
		        	Value result = function.evaluate(tripleSource);
	        		parent.push(result);
		        } catch (ValueExprEvaluationException e) {
		        	parent.handleValueError(e.getMessage());
		        }
        	}
        };
    }

    /**
     * Precompiles an {@link And} node
     * @param node the node to evaluate
     * @param evalContext
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private ValuePipeEvaluationStep precompileAnd(And node, QueryEvaluationContext evalContext) throws ValueExprEvaluationException, QueryEvaluationException {
    	return precompileBinaryValueOperator(node, (leftStep, rightStep) -> {
	    	return (topPipe, bindings) -> {
	    		leftStep.evaluate(new ValuePipe(topPipe) {
	    			@Override
	    			protected void next(Value leftValue) {
	    				QueryEvaluationUtility.Result result = QueryEvaluationUtility.getEffectiveBooleanValue(leftValue);
	    				switch (result) {
	    					case _true:
						        // Left argument evaluated to 'true', result is determined
						        // by the evaluation of the right argument.
		    	            	evaluateIsTrue(rightStep, topPipe, bindings);
	    						break;
	    					case _false:
		    	                // Left argument evaluates to false, we don't need to look any
		    	                // further
		    	                parent.push(FALSE);
	    						break;
	    					case incompatibleValueExpression:
	        					handleValueError("And");
	    						break;
	    					default:
	    						throw new AssertionError(result);
	    				}
	    			}
					@Override
					public void handleValueError(String msg) {
			            // Failed to evaluate the left argument. Result is 'false' when
			            // the right argument evaluates to 'false', failure otherwise.
		            	rightStep.evaluate(new ConvertingValuePipe(topPipe, rightValue -> {
							if (QueryEvaluationUtility.getEffectiveBooleanValue(rightValue) == QueryEvaluationUtility.Result._false) {
							    return OK_FALSE;
							} else {
							    return ValueOrError.fail("And");
							}
		            	}), bindings);
					}
	    		}, bindings);
	    	};
    	}, evalContext);
    }

    /**
     * Precompiles an {@link Or} node
     * @param node the node to evaluate
     * @param evalContext
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private ValuePipeEvaluationStep precompileOr(Or node, QueryEvaluationContext evalContext) throws ValueExprEvaluationException, QueryEvaluationException {
    	return precompileBinaryValueOperator(node, (leftStep, rightStep) -> {
	    	return (topPipe, bindings) -> {
	    		leftStep.evaluate(new ValuePipe(topPipe) {
	    			@Override
	    			protected void next(Value leftValue) {
	    				QueryEvaluationUtility.Result result = QueryEvaluationUtility.getEffectiveBooleanValue(leftValue);
	    				switch (result) {
	    					case _true:
		    	                // Left argument evaluates to true, we don't need to look any
		    	                // further
		    	                parent.push(TRUE);
	    						break;
	    					case _false:
		    	                // Left argument evaluated to 'false', result is determined
		    	                // by the evaluation of the right argument.
		    	            	evaluateIsTrue(rightStep, topPipe, bindings);
	    						break;
	    					case incompatibleValueExpression:
	        					handleValueError("Or");
	    						break;
	    					default:
	    						throw new AssertionError(result);
	    				}
	    			}
					@Override
					public void handleValueError(String msg) {
			            // Failed to evaluate the left argument. Result is 'true' when
			            // the right argument evaluates to 'true', failure otherwise.
		            	rightStep.evaluate(new ConvertingValuePipe(topPipe, rightValue -> {
							if (QueryEvaluationUtility.getEffectiveBooleanValue(rightValue) == QueryEvaluationUtility.Result._true) {
							    return OK_TRUE;
							} else {
							    return ValueOrError.fail("And");
							}
		            	}), bindings);
					}
	    		}, bindings);
	    	};
    	}, evalContext);
    }

    /**
     * Precompiles a {@link Not} node
     * @param node the node to evaluate
     * @param evalContext
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private ValuePipeEvaluationStep precompileNot(Not node, QueryEvaluationContext evalContext) throws ValueExprEvaluationException, QueryEvaluationException {
    	return precompileUnaryValueOperator(node, step -> {
	    	return (parent, bindings) -> {
	    		step.evaluate(new ConvertingValuePipe(parent, v -> {
	    			QueryEvaluationUtility.Result result = QueryEvaluationUtility.getEffectiveBooleanValue(v);
	    			switch (result) {
	    				case _true:
	    					return OK_FALSE; // NOT(true)
	    				case _false:
	    					return OK_TRUE; // NOT(false)
	    				case incompatibleValueExpression:
	    					return ValueOrError.fail("Not");
	    				default:
	    					throw new AssertionError(result);
	    			}
	    		}), bindings);
	    	};
    	}, evalContext);
    }

    /**
     * Precompiles a {@link Now} node. the value of 'now' is shared across the whole query and evaluation strategy
     * @param node the node to evaluate
     * @param evalContext
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private ValuePipeEvaluationStep precompileNow(Now node, QueryEvaluationContext evalContext) throws ValueExprEvaluationException, QueryEvaluationException {
    	Literal now = evalContext.getNow();
        return (parent, bindings)-> {
        	parent.push(now);
        };
    }

    /**
     * Precompiles if the left and right arguments of the {@link SameTerm} node are equal
     * @param node the node to evaluate
     * @param evalContext
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private ValuePipeEvaluationStep precompileSameTerm(SameTerm node, QueryEvaluationContext evalContext) throws ValueExprEvaluationException, QueryEvaluationException {
    	return precompileBinaryValueOperator(node, (leftStep, rightStep) -> {
	    	return (parent, bindings) -> {
	    		AtomicInteger args = new AtomicInteger(2);
	    		AtomicReference<Value> leftValue = new AtomicReference<>();
	    		AtomicReference<Value> rightValue = new AtomicReference<>();
	    		Supplier<ValueOrError> resultSupplier = () -> ok(Objects.equals(leftValue.get(), rightValue.get()));
	    		leftStep.evaluate(new MultiValuePipe(parent, args, v -> leftValue.set(v), resultSupplier), bindings);
	    		rightStep.evaluate(new MultiValuePipe(parent, args, v -> rightValue.set(v), resultSupplier), bindings);
	    	};
    	}, evalContext);
    }

    /**
     * Precompiles a {@link Coalesce} node
     * @param node the node to evaluate
     * @param evalContext
     * @throws ValueExprEvaluationException
     */
    private ValuePipeEvaluationStep precompileCoalesce(Coalesce node, QueryEvaluationContext evalContext) throws ValueExprEvaluationException {
    	return precompileNAryValueOperator(node, argSteps -> {
	    	return (parent, bindings) -> {
	    		Iterator<ValuePipeEvaluationStep> stepIter = Arrays.asList(argSteps).iterator();
	    		evaluateNextArg(parent, node, stepIter, bindings);
	    	};
    	}, evalContext);
    }

    private void evaluateNextArg(ValuePipe parent, Coalesce node, Iterator<ValuePipeEvaluationStep> stepIter, BindingSet bindings) {
    	if (stepIter.hasNext()) {
    		stepIter.next().evaluate(new ValuePipe(parent) {
    			@Override
    			protected void next(Value v) {
    				if (v != null) {
    	                // return first result that does not produce an error on evaluation.
    					parent.push(v);
    				} else {
    					nextArg();
    				}
    			}
    			@Override
    			public void handleValueError(String errMsg) {
   					nextArg();
    			}
    			private void nextArg() {
					evaluateNextArg(parent, node, stepIter, bindings);
    			}
    		}, bindings);
    	} else {
    		parent.handleValueError("COALESCE arguments do not evaluate to a value: " + node.getSignature());
    	}
    }

    /**
     * Precompiles a Compare node
     * @param node the node to evaluate
     * @param evalContext
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private ValuePipeEvaluationStep precompileCompare(Compare node, QueryEvaluationContext evalContext) throws ValueExprEvaluationException, QueryEvaluationException {
    	return precompileBinaryValueOperator(node, (leftStep, rightStep) -> {
	    	return (parent, bindings) -> {
	    		AtomicInteger args = new AtomicInteger(2);
	    		AtomicReference<Value> leftValue = new AtomicReference<>();
	    		AtomicReference<Value> rightValue = new AtomicReference<>();
	    		Supplier<ValueOrError> resultSupplier = () -> of(QueryEvaluationUtility.compare(leftValue.get(), rightValue.get(), node.getOperator(), parentStrategy.isStrict()));
	    		leftStep.evaluate(new MultiValuePipe(parent, args, v -> leftValue.set(v), resultSupplier), bindings);
	    		rightStep.evaluate(new MultiValuePipe(parent, args, v -> rightValue.set(v), resultSupplier), bindings);
	    	};
    	}, evalContext);
    }

    /**
     * Precompiles a {@link MathExpr}
     * @param node the node to evaluate
     * @param evalContext
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private ValuePipeEvaluationStep precompileMathExpr(MathExpr node, QueryEvaluationContext evalContext) throws ValueExprEvaluationException, QueryEvaluationException {
    	return precompileBinaryValueOperator(node, (leftStep, rightStep) -> {
	    	return (parent, bindings) -> {
	    		AtomicInteger args = new AtomicInteger(2);
	    		AtomicReference<Value> leftValue = new AtomicReference<>();
	    		AtomicReference<Value> rightValue = new AtomicReference<>();
	    		Supplier<ValueOrError> resultSupplier = () -> {
	    			Value leftVal = leftValue.get();
	    			Value rightVal = rightValue.get();
	    	        if (leftVal.isLiteral() && rightVal.isLiteral()) {
	    				return ValueOrError.of(() -> XMLDatatypeMathUtil.compute((Literal)leftVal, (Literal)rightVal, node.getOperator(), valueFactory));
	    	        } else {
	    	        	return ValueOrError.fail("Both arguments must be numeric literals");
	    	        }
	    		};
	    		leftStep.evaluate(new MultiValuePipe(parent, args, v -> leftValue.set(v), resultSupplier), bindings);
	    		rightStep.evaluate(new MultiValuePipe(parent, args, v -> rightValue.set(v), resultSupplier), bindings);
	    	};
    	}, evalContext);
    }

    /**
     * Precompiles an {@link If} node
     * @param node the node to evaluate
     * @param evalContext
     * @throws QueryEvaluationException
     */
    private ValuePipeEvaluationStep precompileIf(If node, QueryEvaluationContext evalContext) throws QueryEvaluationException {
    	ValuePipeEvaluationStep conditionStep = precompileValueExpr(node.getCondition(), evalContext);
    	ValuePipeEvaluationStep resultStep = precompileValueExpr(node.getResult(), evalContext);
    	ValuePipeEvaluationStep altStep = precompileValueExpr(node.getAlternative(), evalContext);
    	return (parent, bindings) -> {
    		conditionStep.evaluate(new ValuePipe(parent) {
    			@Override
    			protected void next(Value value) {
    				QueryEvaluationUtility.Result result = QueryEvaluationUtility.getEffectiveBooleanValue(value);
    				switch (result) {
    					case _true:
        		        	resultStep.evaluate(parent, bindings);
    						break;
    					case _false:
        		        	altStep.evaluate(parent, bindings);
    						break;
    					case incompatibleValueExpression:
        		            // in case of type error, if-construction should result in empty
        		            // binding.
        		        	parent.push(null);
    						break;
    					default:
    						throw new AssertionError(result);
    				}
    			}
    		}, bindings);
    	};
    }

    /**
     * Precompiles an {@link In} node
     * @param node the node to evaluate
     * @param evalContext
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private ValuePipeEvaluationStep precompileIn(In node, QueryEvaluationContext evalContext) throws ValueExprEvaluationException, QueryEvaluationException {
    	ValuePipeEvaluationStep argStep = precompileValueExpr(node.getArg(), evalContext);
		BindingSetPipeQueryEvaluationStep subQueryStep = parentStrategy.precompile(node.getSubQuery(), evalContext);
        String bindingName = node.getSubQuery().getBindingNames().iterator().next();
		return (valuePipe, bindings) -> {
			argStep.evaluate(new ValuePipe(null) {
				@Override
				protected void next(Value argValue) {
					subQueryStep.evaluate(new ValueBindingSetPipe(valuePipe) {
						volatile Value isIn = FALSE;
						@Override
						protected boolean next(BindingSet bs) {
							Value v = bs.getValue(bindingName);
							boolean matches = Objects.equals(argValue, v);
							if (matches) {
								isIn = TRUE;
							}
							return !matches;
						}
						@Override
						protected void doClose() {
							parent.push(isIn);
						}
					}, bindings);
				}
			}, bindings);
		};
    }

    /**
     * Precompiles a {@link ListMemberOperator}
     * @param node the node to evaluate
     * @param evalContext
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private ValuePipeEvaluationStep precompileListMemberOperator(ListMemberOperator node, QueryEvaluationContext evalContext) throws ValueExprEvaluationException, QueryEvaluationException {
    	return precompileNAryValueOperator(node, argSteps -> {
        	return (parent, bindings) -> {
        		Iterator<ValuePipeEvaluationStep> stepIter = Arrays.asList(argSteps).iterator();
        		ValuePipeEvaluationStep leftStep = stepIter.next();
        		leftStep.evaluate(new ValuePipe(parent) {
        			@Override
        			protected void next(Value leftValue) {
        				evaluateNextMember(parent, leftValue, null, stepIter, bindings);
        			}
        		}, bindings);
        	};
    	}, evalContext);
    }

    private void evaluateNextMember(ValuePipe parent, Value leftValue, String typeError, Iterator<ValuePipeEvaluationStep> stepIter, BindingSet bindings) {
    	if (stepIter.hasNext()) {
    		stepIter.next().evaluate(new ValuePipe(parent) {
    			@Override
    			protected void next(Value rightValue) {
                    boolean result = (leftValue == null) && (rightValue == null);
                    if (!result) {
        				QueryEvaluationUtility.Result cmp = QueryEvaluationUtility.compare(leftValue, rightValue, Compare.CompareOp.EQ, parentStrategy.isStrict());
        				switch (cmp) {
        					case _true:
        						result = true;
        						break;
        					case _false:
        					case incompatibleValueExpression:
        						result = false;
        						break;
        					default:
        						throw new AssertionError(cmp);
        				}
                    }
                    if (result) {
                        parent.push(TRUE);
                    } else {
    					evaluateNextMember(parent, leftValue, typeError, stepIter, bindings);
                    }
    			}
    			@Override
    			public void handleValueError(String errMsg) {
   					evaluateNextMember(parent, leftValue, errMsg, stepIter, bindings);
    			}
    		}, bindings);
    	} else if (typeError != null) {
            // cf. SPARQL spec a type error is thrown if the value is not in the
            // list and one of the list members caused a type error in the
            // comparison.
    		parent.handleValueError(typeError);
    	} else {
    		parent.push(FALSE);
    	}
    }

    /**
     * Precompiles a {@link CompareAny} node
     * @param node the node to evaluate
     * @param evalContext
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private ValuePipeEvaluationStep precompileCompareAny(CompareAny node, QueryEvaluationContext evalContext) throws ValueExprEvaluationException, QueryEvaluationException {
    	ValuePipeEvaluationStep argStep = precompileValueExpr(node.getArg(), evalContext);
		BindingSetPipeQueryEvaluationStep subQueryStep = parentStrategy.precompile(node.getSubQuery(), evalContext);
        String bindingName = node.getSubQuery().getBindingNames().iterator().next();
		return (valuePipe, bindings) -> {
			argStep.evaluate(new ValuePipe(null) {
				@Override
				protected void next(Value argValue) {
					subQueryStep.evaluate(new ValueBindingSetPipe(valuePipe) {
						volatile Value hasMatch = FALSE;
						@Override
						protected boolean next(BindingSet bs) {
							Value v = bs.getValue(bindingName);
							boolean matches = false;
	        				QueryEvaluationUtility.Result cmp = QueryEvaluationUtility.compare(argValue, v, node.getOperator(), parentStrategy.isStrict());
	        				switch (cmp) {
	        					case _true:
	        						matches = true;
	        						break;
	        					case _false:
	        					case incompatibleValueExpression:
	        						matches = false;
	        						break;
	        					default:
	        						throw new AssertionError(cmp);
	        				}
							if (matches) {
								hasMatch = TRUE;
			                }
							return !matches;
						}
						@Override
						protected void doClose() {
							parent.push(hasMatch);
						}
					}, bindings);
				}
			}, bindings);
		};
    }

    /**
     * Precompiles a {@link CompareAll} node
     * @param node the node to evaluate
     * @param evalContext
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private ValuePipeEvaluationStep precompileCompareAll(CompareAll node, QueryEvaluationContext evalContext) throws ValueExprEvaluationException, QueryEvaluationException {
    	ValuePipeEvaluationStep argStep = precompileValueExpr(node.getArg(), evalContext);
		BindingSetPipeQueryEvaluationStep subQueryStep = parentStrategy.precompile(node.getSubQuery(), evalContext);
        String bindingName = node.getSubQuery().getBindingNames().iterator().next();
		return (valuePipe, bindings) -> {
			argStep.evaluate(new ValuePipe(null) {
				@Override
				protected void next(Value argValue) {
					subQueryStep.evaluate(new ValueBindingSetPipe(valuePipe) {
						volatile Value isMatch = TRUE;
						@Override
						protected boolean next(BindingSet bs) {
							Value v = bs.getValue(bindingName);
							boolean matches;
	        				QueryEvaluationUtility.Result cmp = QueryEvaluationUtility.compare(argValue, v, node.getOperator(), parentStrategy.isStrict());
	        				switch (cmp) {
	        					case _true:
	        						matches = true;
	        						break;
	        					case _false:
	        					case incompatibleValueExpression:
	        						matches = false;
	        						break;
	        					default:
	        						throw new AssertionError(cmp);
	        				}
							if (!matches) {
								isMatch = FALSE;
							}
							return matches;
						}
						@Override
						protected void doClose() {
							parent.push(isMatch);
						}
					}, bindings);
				}
			}, bindings);
		};
    }

    /**
     * Precompiles a {@link Exists} node
     * @param node the node to evaluate
     * @param evalContext
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private ValuePipeEvaluationStep precompileExists(Exists node, QueryEvaluationContext evalContext) throws ValueExprEvaluationException, QueryEvaluationException {
    	TupleExpr subQuery = node.getSubQuery();
    	if ((tripleSource instanceof ExtendedTripleSource) && (subQuery instanceof StatementPattern)) {
    		StatementPattern sp = (StatementPattern) subQuery;
            final Var conVar = sp.getContextVar(); //graph or target context
	        int distinctVarCount = sp.getBindingNames().size();
	        boolean allVarsDistinct = (conVar != null && distinctVarCount == 4) || (conVar == null && distinctVarCount == 3);
	        if (allVarsDistinct) {
	        	return (valuePipe, bindings) -> {
					boolean hasStmt = parentStrategy.hasStatement(sp, bindings);
			    	valuePipe.push(hasStmt ? TRUE : FALSE);
				};
			}
		}

		BindingSetPipeQueryEvaluationStep bsStep = parentStrategy.precompile(subQuery, evalContext);
		return (valuePipe, bindings) -> {
			bsStep.evaluate(new ValueBindingSetPipe(valuePipe) {
				volatile Value hasResult = FALSE;
				@Override
				protected boolean next(BindingSet bs) {
					hasResult = TRUE;
					return false;
				}
				@Override
				protected void doClose() {
					parent.push(hasResult);
				}
			}, bindings);
		};
	}

	private ValuePipeEvaluationStep precompileValueExprTripleRef(ValueExprTripleRef node) throws QueryEvaluationException {
		ValuePipeEvaluationStep subjStep = precompileVar(node.getSubjectVar());
		ValuePipeEvaluationStep predStep = precompileVar(node.getPredicateVar());
		ValuePipeEvaluationStep objStep = precompileVar(node.getObjectVar());
		return (parent, bindings) -> {
    		AtomicInteger args = new AtomicInteger(3);
    		AtomicReference<Value> subjValue = new AtomicReference<>();
    		AtomicReference<Value> predValue = new AtomicReference<>();
    		AtomicReference<Value> objValue = new AtomicReference<>();
    		Supplier<ValueOrError> resultSupplier = () -> {
    			Value subj = subjValue.get();
    			Value pred = predValue.get();
    			Value obj = objValue.get();
    			if (subj == null || !subj.isResource()) {
    				return ValueOrError.fail("invalid subject value");
    			}
    			if (pred == null || !pred.isIRI()) {
    				return ValueOrError.fail("invalid predicate value");
    			}
    			if (obj == null) {
    				return ValueOrError.fail("missing object value");
    			}
    			return ValueOrError.ok(valueFactory.createTriple((Resource) subj, (IRI) pred, obj));
    		};
    		subjStep.evaluate(new MultiValuePipe(parent, args, v -> subjValue.set(v), resultSupplier), bindings);
    		predStep.evaluate(new MultiValuePipe(parent, args, v -> predValue.set(v), resultSupplier), bindings);
    		objStep.evaluate(new MultiValuePipe(parent, args, v -> objValue.set(v), resultSupplier), bindings);
		};
	}

	private ValueOrError get(ValuePipeEvaluationStep step, BindingSet bindings) {
		final class GetTask implements Runnable {
			final CountDownLatch done = new CountDownLatch(1);
			volatile ValueOrError result;
			@Override
			public void run() {
	        	step.evaluate(new ValuePipe(null) {
	    			@Override
	    			protected void next(Value v) {
	    				result = ValueOrError.ok(v); // maybe null
	    				done.countDown();
	    			}
	    			@Override
	    			public void handleValueError(String err) {
	    				result = ValueOrError.fail(err);
	    				done.countDown();
	    			}
	        	}, bindings);
			}
			public ValueOrError get(long timeout) {
				boolean hasResult;
				try {
					hasResult = done.await(timeout, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					throw new QueryInterruptedException(e);
				}
				if (!hasResult) {
					throw new QueryEvaluationException(String.format("Exceeded poll time-out of %dms", pollTimeoutMillis));
				}
				return result;
			}
		}
		GetTask task = new GetTask();
		// can potentially run this on an executor if needed
    	task.run();
    	return task.get(pollTimeoutMillis);
	}

	private ValuePipeEvaluationStep evaluateForEmptyBindingSetAndPrecompile(ValuePipeEvaluationStep step) {
		ValueOrError result = get(step, EmptyBindingSet.getInstance());
		if (result.isOk()) {
			return new ConstantValuePipeEvaluationStep(result.getValue());
		} else {
			return new ErrorValuePipeEvaluationStep(result.getMessage());
		}
	}

	ValueOrError ok(boolean b) {
		return ValueOrError.ok(b ? TRUE : FALSE);
	}

	ValueOrError of(QueryEvaluationUtility.Result result) {
		switch (result) {
			case _true:
				return OK_TRUE;
			case _false:
				return OK_FALSE;
			case incompatibleValueExpression:
				return ValueOrError.fail("Incompatible value expression");
			default:
				throw new AssertionError(result);
		}
	}

	private void evaluateIsTrue(ValuePipeEvaluationStep step, ValuePipe parent, BindingSet bindings) {
		step.evaluate(new ConvertingValuePipe(parent, HalyardValueExprEvaluation.this::effectiveBooleanLiteral), bindings);
	}

	ValueOrError effectiveBooleanLiteral(Value v) {
		return of(QueryEvaluationUtility.getEffectiveBooleanValue(v));
	}

	boolean isTrue(Value v) {
		return TRUE.equals(v);
	}


	static final class ConstantValuePipeEvaluationStep implements ValuePipeEvaluationStep {
    	private final Value value;
		protected ConstantValuePipeEvaluationStep(Value value) {
			this.value = value;
		}
		@Override
		public void evaluate(ValuePipe parent, BindingSet bindings) {
			parent.push(value);
		}
		@Override
		public boolean isConstant() {
			return true;
		}
    }

	static final class ErrorValuePipeEvaluationStep implements ValuePipeEvaluationStep {
    	private final String msg;
		protected ErrorValuePipeEvaluationStep(String msg) {
			this.msg = msg;
		}
		@Override
		public void evaluate(ValuePipe parent, BindingSet bindings) {
			parent.handleValueError(msg);
		}
		@Override
		public boolean isConstant() {
			return true;
		}
    }

	static final class ValueOrError {
		private final Value v;
		private final String err;
		private ValueOrError(Value v, String err) {
			this.v = v;
			this.err = err;
		}
		boolean isOk() {
			return (err == null); // Value can be null!
		}
		Value getValue() {
			return v;
		}
		String getMessage() {
			return err;
		}
		static ValueOrError ok(@Nullable Value v) {
			return new ValueOrError(v, null);
		}
		static ValueOrError fail(String msg) {
			return new ValueOrError(null, Objects.requireNonNull(msg));
		}
		static ValueOrError of(Supplier<Value> supplier) {
			try {
				return ok(supplier.get());
			} catch (ValueExprEvaluationException e) {
				return fail(e.getMessage());
			}
		}
	}

	static final class ConvertingValuePipe extends ValuePipe {
    	private final java.util.function.Function<Value,ValueOrError> map;
		protected ConvertingValuePipe(ValuePipe parent, java.util.function.Function<Value,ValueOrError> map) {
			super(parent);
			this.map = map;
		}
		@Override
		protected void next(Value v) {
			ValueOrError voe = map.apply(v);
			if (voe.isOk()) {
				parent.push(voe.getValue());
			} else {
				parent.handleValueError(voe.getMessage());
			}
		}
    }

    static final class MultiValuePipe extends ValuePipe {
    	final AtomicInteger remaining;
    	final Consumer<Value> valueConsumer;
    	final Supplier<ValueOrError> resultSupplier;
		protected MultiValuePipe(ValuePipe parent, AtomicInteger remaining, Consumer<Value> valueConsumer, Supplier<ValueOrError> resultSupplier) {
			super(parent);
			this.remaining = remaining;
			this.valueConsumer = valueConsumer;
			this.resultSupplier = resultSupplier;
		}
		@Override
		protected void next(Value v) {
			valueConsumer.accept(v);
			if (remaining.decrementAndGet() == 0) {
				ValueOrError result = resultSupplier.get();
				if (result.isOk()) {
					parent.push(result.getValue());
				} else {
					parent.handleValueError(result.getMessage());
				}
			}
		}
	}

    static abstract class ValueBindingSetPipe extends BindingSetPipe {
    	protected final ValuePipe parent;
		protected ValueBindingSetPipe(ValuePipe parent) {
			super(null);
			this.parent = parent;
		}
		@Override
		public final boolean handleException(Throwable e) {
			if (e instanceof ValueExprEvaluationException) {
				parent.handleValueError(e.getMessage());
			} else {
				parent.handleException(e);
			}
			return false;
		}
    }
}
