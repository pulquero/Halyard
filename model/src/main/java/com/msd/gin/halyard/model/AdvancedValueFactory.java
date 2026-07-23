package com.msd.gin.halyard.model;

import java.math.BigInteger;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.base.AbstractValueFactory;
import org.eclipse.rdf4j.model.base.CoreDatatype;

import com.msd.gin.halyard.model.vocabulary.HalyardDatatype;

public final class AdvancedValueFactory extends AbstractValueFactory implements ExtendedValueFactory {
	private static final AdvancedValueFactory INSTANCE = new AdvancedValueFactory();

	public static AdvancedValueFactory getInstance() {
		return INSTANCE;
	}

	private CoreDatatype getCoreDatatype(IRI datatype) {
		CoreDatatype cdt = CoreDatatype.from(datatype);
		if (cdt == CoreDatatype.NONE) {
			cdt = HalyardDatatype.from(datatype);
		}
		return cdt;
	}

	private Literal createAdvancedLiteral(String label, CoreDatatype coreDatatype) {
		try {
			if (coreDatatype == CoreDatatype.GEO.WKT_LITERAL) {
				return new WKTLiteral(label);
			} else if (coreDatatype == CoreDatatype.RDF.XMLLITERAL) {
				return new XMLLiteral(label);
			} else if (coreDatatype == HalyardDatatype.TUPLE) {
				return new TupleLiteral(label);
			} else if (coreDatatype == HalyardDatatype.ARRAY) {
				return AbstractArrayLiteral.create(label);
			} else if (coreDatatype == HalyardDatatype.MAP) {
				return new MapLiteral(label);
			} else if (coreDatatype == CoreDatatype.XSD.BASE64BINARY) {
				return new Base64Literal(label);
			}
		} catch (IllegalArgumentException e) {
			// catch any illegal values and fallback
		}
		return super.createLiteral(label, coreDatatype);
	}

	@Override
	public Literal createLiteral(String label, IRI datatype) {
		CoreDatatype cdt = getCoreDatatype(datatype);
		if (cdt != CoreDatatype.NONE) {
			return createAdvancedLiteral(label, cdt);
		} else {
			return super.createLiteral(label, datatype);
		}
	}

	@Override
	public Literal createLiteral(String label, CoreDatatype coreDatatype) {
		return createAdvancedLiteral(label, coreDatatype);
	}

	@Override
	public Literal createLiteral(String label, IRI datatype, CoreDatatype coreDatatype) {
		if (coreDatatype != CoreDatatype.NONE) {
			return createAdvancedLiteral(label, coreDatatype);
		} else {
			return super.createLiteral(label, datatype, coreDatatype);
		}
	}

	@Override
	public Literal createLiteral(byte value) {
		return IntLiteral.createByte(value);
	}

	@Override
	public Literal createLiteral(short value) {
		return IntLiteral.createShort(value);
	}

	@Override
	public Literal createLiteral(int value) {
		return IntLiteral.createInt(value);
	}

	@Override
	public Literal createLiteral(long value) {
		return LongLiteral.createLong(value);
	}

	@Override
	public Literal createLiteral(BigInteger value) {
		double u = value.doubleValue();
		if (u >= Integer.MIN_VALUE && u <= Integer.MAX_VALUE) {
			return IntLiteral.createInteger(value.intValueExact());
		} else if (u >= Long.MIN_VALUE && u <= Long.MAX_VALUE) {
				return LongLiteral.createInteger(value.longValueExact());
		} else {
			return super.createLiteral(value);
		}
	}
}