package com.msd.gin.halyard.rio;

import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.ByteArrayInputStream;
import java.util.Arrays;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.resultio.QueryResultIO;
import org.eclipse.rdf4j.query.resultio.helpers.QueryResultCollector;
import org.junit.jupiter.api.Test;

public class SKVParserTest {
    @Test
    public void testParserFactory() {
        assertNotNull(QueryResultIO.createTupleParser(SPARQLResultsSKVParser.FORMAT));
    }

    @Test
    public void testParse() throws Exception {
    	ValueFactory vf = SimpleValueFactory.getInstance();
    	String skv = "a;\"b\";c\n\"foo\";bar;5\n0.3e-6;.;\"\"";
    	SPARQLResultsSKVParser p = new SPARQLResultsSKVParser(vf);
    	QueryResultCollector results = new QueryResultCollector();
    	p.setQueryResultHandler(results);
        p.parse(new ByteArrayInputStream(skv.getBytes("UTF-8")));
        assertEquals(Arrays.asList("a", "b", "c"), results.getBindingNames());
        BindingSet bs = results.getBindingSets().get(0);
        assertEquals(vf.createLiteral("foo"), bs.getValue("a"));
        assertEquals(vf.createLiteral("bar"), bs.getValue("b"));
        assertEquals(vf.createLiteral("5", XSD.INTEGER), bs.getValue("c"));
        bs = results.getBindingSets().get(1);
        assertEquals(vf.createLiteral("."), bs.getValue("b"));
        assertNull(bs.getValue("c"));
    }
}
