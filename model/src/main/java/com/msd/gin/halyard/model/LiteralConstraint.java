package com.msd.gin.halyard.model;

import java.util.Objects;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.datatypes.XMLDatatypeUtil;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XSD;

import com.msd.gin.halyard.model.vocabulary.HALYARD;

public final class LiteralConstraint extends ValueConstraint {
	private final IRI datatype;
	private final CoreDatatype coreDatatype;
	private final String lang;

	public static boolean isString(IRI dt) {
		return XSD.STRING.equals(dt) || RDF.LANGSTRING.equals(dt);
	}

	public static boolean isString(CoreDatatype dt) {
		return dt == CoreDatatype.XSD.STRING || dt == CoreDatatype.RDF.LANGSTRING;
	}

	public LiteralConstraint(IRI datatype) {
		this(ValueType.LITERAL, Objects.requireNonNull(datatype), CoreDatatype.from(datatype), null);
	}

	public LiteralConstraint(CoreDatatype datatype) {
		this(ValueType.LITERAL, datatype.getIri(), datatype, null);
	}

	public LiteralConstraint(String lang) {
		this(ValueType.LITERAL, RDF.LANGSTRING, CoreDatatype.RDF.LANGSTRING, Objects.requireNonNull(lang));
	}

	private LiteralConstraint(ValueType type, IRI datatype, CoreDatatype cdt, String lang) {
		super(type);
		this.datatype = datatype;
		this.coreDatatype = cdt;
		this.lang = lang;
	}

	public IRI getDatatype() {
		return datatype;
	}

	public CoreDatatype getCoreDatatype() {
		return coreDatatype;
	}

	public String getLanguageTag() {
		return lang;
	}

	@Override
	public boolean test(Value v) {
		if (!v.isLiteral()) {
			return false;
		}
		Literal l = (Literal) v;
		if (coreDatatype != CoreDatatype.NONE) {
			if (!l.getCoreDatatype().equals(coreDatatype)) {
				return false;
			}
			if (lang != null) {
				if (!l.getLanguage().map(lang -> lang.equals(this.lang)).orElse(Boolean.FALSE)) {
					return false;
				}
			}
		} else {
			IRI dt = l.getDatatype();
			if (!dt.equals(datatype)
				&& !(HALYARD.NON_STRING_TYPE.equals(datatype) && !isString(dt))
				&& !(HALYARD.ANY_NUMERIC_TYPE.equals(datatype) && XMLDatatypeUtil.isNumericDatatype(dt))) {
				return false;
			}
		}
		return true;
	}

	@Override
	public int hashCode() {
		int h = super.hashCode();
		h = 89 * h + Objects.hashCode(datatype);
		h = 89 * h + Objects.hashCode(lang);
		return h;
	}

	@Override
	public boolean equals(Object other) {
		if (this == other) {
			return true;
		}
		if (other == null || this.getClass() != other.getClass()) {
			return false;
		}
		LiteralConstraint that = (LiteralConstraint) other;
		return super.equals(that) && this.datatype.equals(that.datatype) && Objects.equals(this.lang, that.lang);
	}
}
