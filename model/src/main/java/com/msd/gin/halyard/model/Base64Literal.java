package com.msd.gin.halyard.model;

import java.util.Arrays;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.base.CoreDatatype.XSD;

import com.msd.gin.halyard.common.ByteUtils;

public final class Base64Literal extends AbstractDataLiteral implements ObjectLiteral<byte[]>  {
	private static final long serialVersionUID = 3891656855446151914L;
	private final byte[] bytes;

	public Base64Literal(String s) {
		this.bytes = ByteUtils.decode(s);
	}

	public Base64Literal(byte[] b) {
		this.bytes = b;
	}

	@Override
	public String getLabel() {
		return ByteUtils.encode(bytes);
	}

	@Override
	public IRI getDatatype() {
		return XSD.BASE64BINARY.getIri();
	}

	@Override
	public CoreDatatype getCoreDatatype() {
		return XSD.BASE64BINARY;
	}

	@Override
	public byte[] objectValue() {
		return bytes;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o instanceof Base64Literal) {
			Base64Literal other = (Base64Literal) o;
			return Arrays.equals(bytes, other.bytes);
		} else {
			return super.equals(o);
		}
	}

	public static Base64Literal asBase64Literal(Literal l) {
		if (l instanceof Base64Literal) {
			return (Base64Literal) l;
		} else {
			return new Base64Literal(l.getLabel());
		}
	}

	public static byte[] byteArray(Literal l) {
		return asBase64Literal(l).objectValue();
	}
}
