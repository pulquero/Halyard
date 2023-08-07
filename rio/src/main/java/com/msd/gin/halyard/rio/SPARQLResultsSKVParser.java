package com.msd.gin.halyard.rio;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;
import org.eclipse.rdf4j.query.resultio.AbstractTupleQueryResultParser;
import org.eclipse.rdf4j.query.resultio.QueryResultParseException;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultFormat;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultParser;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultParserFactory;
import org.eclipse.rdf4j.query.resultio.text.tsv.SPARQLResultsTSVMappingStrategy;

import com.opencsv.CSVReader;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
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

	@Override
	public TupleQueryResultFormat getTupleQueryResultFormat() {
		return FORMAT;
	}

	@Override
	public void parse(InputStream in) throws QueryResultParseException, TupleQueryResultHandlerException {
		if (handler != null) {
			// can re-use TSV code
			SPARQLResultsTSVMappingStrategy strategy = new  SPARQLResultsTSVMappingStrategy(valueFactory);

			Reader reader = new InputStreamReader(in, StandardCharsets.UTF_8);
			CsvToBean<BindingSet> csvToBean = new CsvToBeanBuilder<BindingSet>(reader).withType(BindingSet.class)
					.withMappingStrategy(strategy)
					.withSeparator(';')
					.build();
			csvToBean.setCsvReader(new SPARQLResultsSKVReader(reader)); // We need our reader
			List<BindingSet> bindingSets = csvToBean.parse();
			List<String> bindingNames = strategy.getBindingNames();
			handler.startQueryResult(bindingNames);
			for (BindingSet bs : bindingSets) {
				handler.handleSolution(bs);
			}
			handler.endQueryResult();
		}
	}


	private static class SPARQLResultsSKVReader extends CSVReader {
		SPARQLResultsSKVReader(Reader reader) {
			super(reader);
		}

		@Override
		public String[] readNext() throws IOException {
			String line = getNextLine();
			if (line == null) {
				return null;
			}
			String[] fields = line.split(";", -1);
			try {
				validateResult(fields, linesRead);
			} catch (CsvValidationException ex) {
				throw new IOException(ex);
			}
			return fields;
		}
	}
}
