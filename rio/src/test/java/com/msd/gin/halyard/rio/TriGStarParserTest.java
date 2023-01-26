package com.msd.gin.halyard.rio;

import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.rio.nquads.NQuadsParser;
import org.eclipse.rdf4j.testsuite.rio.trig.TriGParserTestCase;

public class TriGStarParserTest extends TriGParserTestCase {
	public static junit.framework.Test suite() throws Exception {
		return new TriGStarParserTest().createTestSuite();
	}

	@Override
	protected RDFParser createTriGParser() {
		TriGStarParser parser = new TriGStarParser();
		parser.set(BasicParserSettings.VERIFY_DATATYPE_VALUES, true);
		return parser;
	}

	@Override
	protected RDFParser createNQuadsParser() {
		return new NQuadsParser();
	}

}
