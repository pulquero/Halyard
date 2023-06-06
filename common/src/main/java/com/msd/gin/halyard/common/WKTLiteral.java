package com.msd.gin.halyard.common;

import java.util.Arrays;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.vocabulary.GEO;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;

/**
 * Compact WKB representation of WKT literals.
 */
public class WKTLiteral extends AbstractDataLiteral implements ObjectLiteral<Geometry> {
	private static final long serialVersionUID = 2499060372102054647L;
	private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

	public static GeometryFactory getGeometryFactory() {
		return GEOMETRY_FACTORY;
	}

	public static Geometry geometryValue(Literal l) {
		if (l instanceof WKTLiteral) {
			return ((WKTLiteral)l).objectValue();
		} else {
			try {
				return geometryValue(l.getLabel());
			} catch (ParseException e) {
				throw new IllegalArgumentException("Invalid WKT content", e);
			}
		}
	}

	private static Geometry geometryValue(String wkt) throws ParseException {
		WKTReader wktReader = new WKTReader(GEOMETRY_FACTORY);
		Geometry geom = wktReader.read(wkt);
		if (geom == null) {
			throw new ParseException(String.format("Failed to parse %s", wkt));
		}
		return geom;
	}

	static byte[] writeWKB(Literal l) throws ParseException {
		if (l instanceof WKTLiteral) {
			return ((WKTLiteral)l).wkbBytes;
		} else {
			return writeWKB(l.getLabel());
		}
	}

	private static byte[] writeWKB(String wkt) throws ParseException {
		Geometry geom = geometryValue(wkt);
		return writeWKB(geom);
	}

	private static byte[] writeWKB(Geometry geom) {
		WKBWriter wkbWriter = new WKBWriter();
		return wkbWriter.write(geom);
	}


	private final byte[] wkbBytes;

	public WKTLiteral(String wkt) throws ParseException {
		this(writeWKB(wkt));
	}

	public WKTLiteral(Geometry geom) {
		this(writeWKB(geom));
	}

	public WKTLiteral(byte[] wkb) {
		this.wkbBytes = wkb;
	}

	@Override
	public String getLabel() {
		Geometry g = objectValue();
		return new WKTWriter().write(g);
	}

	@Override
	public IRI getDatatype() {
		return GEO.WKT_LITERAL;
	}

	@Override
	public CoreDatatype getCoreDatatype() {
		return CoreDatatype.GEO.WKT_LITERAL;
	}

	public Geometry objectValue() {
		try {
			return new WKBReader(GEOMETRY_FACTORY).read(wkbBytes);
		} catch (ParseException e) {
			throw new IllegalArgumentException("Invalid WKT content", e);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o instanceof WKTLiteral) {
			WKTLiteral other = (WKTLiteral) o;
			return Arrays.equals(wkbBytes, other.wkbBytes);
		} else if (o instanceof Literal) {
			Literal other = (Literal) o;

			// Compare labels
			if (!getLabel().equals(other.getLabel())) {
				return false;
			}

			// Compare datatypes
			if (!getDatatype().equals(other.getDatatype())) {
				return false;
			}
			return true;
		}
		return false;
	}
}
