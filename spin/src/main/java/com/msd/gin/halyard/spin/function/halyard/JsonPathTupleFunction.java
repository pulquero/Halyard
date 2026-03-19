package com.msd.gin.halyard.spin.function.halyard;

import java.util.Collections;
import java.util.List;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.CloseableIteratorIteration;
import org.eclipse.rdf4j.common.iteration.ConvertingIteration;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunction;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.msd.gin.halyard.model.MapLiteral;
import com.msd.gin.halyard.model.vocabulary.HALYARD;
import com.msd.gin.halyard.spin.function.InverseMagicProperty;

public class JsonPathTupleFunction implements TupleFunction, InverseMagicProperty {
	@Override
	public String getURI() {
		return HALYARD.JSON_PATH_PROPERTY.stringValue();
	}

	@Override
	public CloseableIteration<? extends List<? extends Value>> evaluate(ValueFactory vf, Value... args) throws ValueExprEvaluationException {
		if (args.length != 2) {
			throw new ValueExprEvaluationException(String.format("%s requires 2 arguments, got %d", getURI(), args.length));
		}

		if (!(args[0] instanceof Literal)) {
			throw new ValueExprEvaluationException("First argument must be a JsonPath string");
		}
		if (!MapLiteral.isMapLiteral(args[1])) {
			throw new ValueExprEvaluationException("Second argument must be a JSON literal");
		}

		String path = args[0].stringValue();
		String json = args[1].stringValue();
		Configuration conf = Configuration.defaultConfiguration().addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL, Option.ALWAYS_RETURN_LIST);

		try {
			List<Object> result = JsonPath.using(conf).parse(json).read(path);
			return new ConvertingIteration<>(new CloseableIteratorIteration<>(result.iterator())) {
				@Override
				protected List<? extends Value> convert(Object s) {
					return Collections.singletonList(vf.createLiteral(result.toString()));
				}
			};
		} catch (Exception e) {
			throw new QueryEvaluationException(e);
		}
	}
}
