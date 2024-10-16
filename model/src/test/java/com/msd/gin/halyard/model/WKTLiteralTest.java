package com.msd.gin.halyard.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.base.CoreDatatype.GEO;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;

public class WKTLiteralTest extends AbstractCustomLiteralTest {
	@Override
	protected Literal createLiteral() {
		String wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))";
		return new WKTLiteral(wkt);
	}

	@Override
	protected Literal createOtherLiteral() {
		String wkt = "POINT (30 10)";
		return new WKTLiteral(wkt);
	}

	@Test
	public void testGeometry() {
		String wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))";
		WKTLiteral l = new WKTLiteral(wkt);
		Geometry expected = l.objectValue();
		assertEquals(expected, WKTLiteral.geometryValue(l));
		assertEquals(expected, WKTLiteral.geometryValue(SimpleValueFactory.getInstance().createLiteral(wkt, GEO.WKT_LITERAL)));
	}
}
