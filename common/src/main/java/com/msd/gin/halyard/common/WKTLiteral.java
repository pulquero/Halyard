package com.msd.gin.halyard.common;

import java.io.IOException;
import java.util.Arrays;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.vocabulary.GEO;
import org.locationtech.jts.geom.Geometry;
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
	private final byte[] wkbBytes;

	static byte[] writeWKB(String wkt) throws ParseException, IOException {
		WKTReader wktReader = new WKTReader();
		Geometry geom = wktReader.read(wkt);
		if (geom == null) {
			throw new ParseException(String.format("Failed to parse %s", wkt));
		}
		WKBWriter wkbWriter = new WKBWriter();
		return wkbWriter.write(geom);
	}

	public static Geometry geometryValue(Literal l) {
		if (l instanceof WKTLiteral) {
			return ((WKTLiteral)l).objectValue();
		} else {
			WKTReader wktReader = new WKTReader();
			try {
				Geometry geom = wktReader.read(l.getLabel());
				if (geom == null) {
					throw new ParseException(String.format("Failed to parse %s", l.getLabel()));
				}
				return geom;
			} catch (ParseException e) {
				throw new IllegalArgumentException("Invalid WKT content", e);
			}
		}
	}

	public WKTLiteral(String wkt) throws ParseException, IOException {
		this.wkbBytes = writeWKB(wkt);
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
			return new WKBReader().read(wkbBytes);
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
