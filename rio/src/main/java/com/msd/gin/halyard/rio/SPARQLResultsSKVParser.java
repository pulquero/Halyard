package com.msd.gin.halyard.rio;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.resultio.AbstractTupleQueryResultParser;
import org.eclipse.rdf4j.query.resultio.QueryResultParseException;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultFormat;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultParser;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultParserFactory;
import org.eclipse.rdf4j.query.resultio.text.SPARQLResultsXSVMappingStrategy;

import com.opencsv.CSVReader;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import com.opencsv.exceptions.CsvBeanIntrospectionException;
import com.opencsv.exceptions.CsvChainedException;
import com.opencsv.exceptions.CsvFieldAssignmentException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;
import com.opencsv.exceptions.CsvValidationException;

public class SPARQLResultsSKVParser extends AbstractTupleQueryResultParser {
	public static final TupleQueryResultFormat FORMAT = new TupleQueryResultFormat("SPARQL/SKV",
			"text/x-semicolon-separated-values", StandardCharsets.UTF_8, "skv");

	public static final class Factory implements TupleQueryResultParserFactory {
        @Override
        public TupleQueryResultFormat getTupleQueryResultFormat() {
            return FORMAT;
        }

        @Override
        public TupleQueryResultParser getParser() {
            return new SPARQLResultsSKVParser();
        }
    }

	public SPARQLResultsSKVParser() {
		super();
	}

	public SPARQLResultsSKVParser(ValueFactory vf) {
		super(vf);
	}

	@Override
	public TupleQueryResultFormat getTupleQueryResultFormat() {
		return FORMAT;
	}

	@Override
	public void parse(InputStream in) throws QueryResultParseException, TupleQueryResultHandlerException {
		if (handler != null) {
			MappingStrategy strategy = new MappingStrategy(valueFactory);

			Reader reader = new InputStreamReader(in, StandardCharsets.UTF_8);
			CsvToBean<BindingSet> csvToBean = new CsvToBeanBuilder<BindingSet>(reader).withType(BindingSet.class)
					.withMappingStrategy(strategy)
					.withSeparator(';')
					.build();
			Stream<BindingSet> bindingSets = csvToBean.stream();
			List<String> bindingNames = strategy.getBindingNames();
			handler.startQueryResult(bindingNames);
			bindingSets.forEach(handler::handleSolution);
			handler.endQueryResult();
		}
	}

	static final class MappingStrategy extends SPARQLResultsXSVMappingStrategy {
		private static final Pattern numberPattern = Pattern.compile("^[-+]?\\d+(\\.\\d+)?([eE][-+]?\\d+)?");

		MappingStrategy(ValueFactory valueFactory) {
			super(valueFactory);
		}

		@Override
		public void captureHeader(CSVReader reader) throws IOException, CsvRequiredFieldEmptyException {
			try {
				bindingNames = Arrays.asList(reader.readNext());
			} catch (CsvValidationException ex) {
				throw new IOException(ex);
			}
		}

		@Override
		public BindingSet populateNewBean(String[] line) throws CsvBeanIntrospectionException, CsvFieldAssignmentException, CsvChainedException {
			QueryBindingSet bindings = new QueryBindingSet(line.length+1);
			for (int i=0; i<line.length; i++) {
				String valueString = line[i];
				if (!valueString.isEmpty()) {
					Value v;
					if (numberPattern.matcher(valueString).matches()) {
						v = parseNumberPatternMatch(valueString);
					} else {
						v = valueFactory.createLiteral(valueString);
					}
					bindings.addBinding(bindingNames.get(i), v);
				}
			}
			return bindings;
		}
	}
}
