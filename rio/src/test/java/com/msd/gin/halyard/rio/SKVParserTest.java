package com.msd.gin.halyard.rio;

import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayInputStream;

import org.eclipse.rdf4j.query.resultio.QueryResultIO;
import org.junit.Test;

public class SKVParserTest {
    @Test
    public void testParserFactory() {
        assertNotNull(QueryResultIO.createTupleParser(SPARQLResultsSKVParser.FORMAT));
    }

    @Test
    public void testParse() throws Exception {
    	SPARQLResultsSKVParser p = new SPARQLResultsSKVParser();
        p.parse(new ByteArrayInputStream("\"foo\";bar;5".getBytes("UTF-8")));
    }
}
