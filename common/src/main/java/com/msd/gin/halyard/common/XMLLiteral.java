package com.msd.gin.halyard.common;

import com.sun.xml.fastinfoset.dom.DOMDocumentParser;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Arrays;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.jvnet.fastinfoset.FastInfosetException;
import org.jvnet.fastinfoset.FastInfosetResult;
import org.jvnet.fastinfoset.FastInfosetSource;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * Compact fast-infoset representation of XML literals.
 */
public class XMLLiteral extends AbstractDataLiteral implements ObjectLiteral<Document> {
	private static final long serialVersionUID = 7055125328013989394L;
	private final byte[] fiBytes;

    private static final ThreadLocal<TransformerFactory> TRANSFORMER_FACTORY = new ThreadLocal<TransformerFactory>() {
        @Override
		protected TransformerFactory initialValue() {
        	return TransformerFactory.newInstance("net.sf.saxon.jaxp.SaxonTransformerFactory", null);
        }
    };

    private static final ThreadLocal<DocumentBuilderFactory> DOCUMENT_BUILDER_FACTORY = new ThreadLocal<DocumentBuilderFactory>() {
        @Override
		protected DocumentBuilderFactory initialValue() {
        	return DocumentBuilderFactory.newInstance();
        }
    };

	static void writeInfoset(String xml, OutputStream out) throws TransformerException {
		TRANSFORMER_FACTORY.get().newTransformer().transform(new StreamSource(new StringReader(xml)), new FastInfosetResult(out));
	}

	public static Document parseXml(String xml) throws ParserConfigurationException, SAXException, IOException {
		return DOCUMENT_BUILDER_FACTORY.get().newDocumentBuilder().parse(new InputSource(new StringReader(xml)));
	}

	public XMLLiteral(String xml) {
		try {
			ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
			writeInfoset(xml, out);
			this.fiBytes = out.toByteArray();
		} catch (TransformerException e) {
			throw new IllegalArgumentException(e);
		}
	}

	public XMLLiteral(byte[] fiBytes) {
		this.fiBytes = fiBytes;
	}

	@Override
	public String getLabel() {
		try {
			InputStream in = new ByteArrayInputStream(fiBytes);
			StringWriter writer = new StringWriter(1024);
			TRANSFORMER_FACTORY.get().newTransformer().transform(new FastInfosetSource(in), new StreamResult(writer));
			return writer.toString();
		} catch(TransformerException e) {
			throw new AssertionError(e);
		}
	}

	@Override
	public IRI getDatatype() {
		return RDF.XMLLITERAL;
	}

	@Override
	public CoreDatatype getCoreDatatype() {
		return CoreDatatype.RDF.XMLLITERAL;
	}

	public Document objectValue() {
        DocumentBuilderFactory dbf = DOCUMENT_BUILDER_FACTORY.get();
        dbf.setNamespaceAware(true);
        try {
	        DocumentBuilder db = dbf.newDocumentBuilder();
	        Document doc = db.newDocument();
			DOMDocumentParser parser = new DOMDocumentParser();
			parser.parse(doc, new ByteArrayInputStream(fiBytes));
			return doc;
        } catch(ParserConfigurationException | FastInfosetException | IOException e) {
        	throw new AssertionError(e);
        }
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o instanceof XMLLiteral) {
			XMLLiteral other = (XMLLiteral) o;
			return Arrays.equals(fiBytes, other.fiBytes);
		} else {
			return super.equals(o);
		}
	}
}
