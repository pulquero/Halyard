package com.msd.gin.halyard.common;

import java.util.Objects;
import java.util.Optional;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XSD;

public final class IdentifiableLiteral extends AbstractIdentifiableLiteral {
	private final String label;
	private final IRI datatype;
	private final CoreDatatype coreDatatype;
	private final String lang;

	IdentifiableLiteral(String label, IRI datatype, CoreDatatype coreDatatype) {
		this.label = Objects.requireNonNull(label);
		this.datatype = Objects.requireNonNullElse(datatype, XSD.STRING);
		if (RDF.LANGSTRING.equals(datatype)) {
			throw new IllegalArgumentException("Missing language tag");
		}
		this.coreDatatype = coreDatatype;
		this.lang = null;
	}

	IdentifiableLiteral(String label, IRI datatype) {
		this(label, datatype, CoreDatatype.from(datatype));
	}

	IdentifiableLiteral(String label, CoreDatatype coreDatatype) {
		this(label, coreDatatype.getIri(), coreDatatype);
	}

	IdentifiableLiteral(String label, String lang) {
		this.label = Objects.requireNonNull(label);
		this.lang = Objects.requireNonNull(lang);
		if (lang.isEmpty()) {
			throw new IllegalArgumentException("Language tag cannot be empty");
		}
		this.datatype = RDF.LANGSTRING;
		this.coreDatatype = CoreDatatype.RDF.LANGSTRING;
	}

	IdentifiableLiteral(String label) {
		this.label = Objects.requireNonNull(label);
		this.lang = null;
		this.datatype = XSD.STRING;
		this.coreDatatype = CoreDatatype.XSD.STRING;
	}

	@Override
	public String getLabel() {
		return label;
	}

	@Override
	public Optional<String> getLanguage() {
		return Optional.ofNullable(lang);
	}

	@Override
	public IRI getDatatype() {
		return datatype;
	}

	@Override
	public CoreDatatype getCoreDatatype() {
		return coreDatatype;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		ValueIdentifier thatId = getCompatibleId(o);
		if (thatId != null) {
			return getId(null).equals(thatId);
		}
		if (o instanceof Literal) {
			Literal that = (Literal) o;
			String thatLang = that.getLanguage().orElse(null);
			return this.label.equals(that.getLabel())
					&& this.datatype.equals(that.getDatatype())
					&& ((this.lang == null && thatLang == null) || this.lang.equalsIgnoreCase(thatLang));
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return label.hashCode();
	}
}
