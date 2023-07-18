package com.msd.gin.halyard.rio;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.RDFWriterFactory;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFWriter;

import com.msd.gin.halyard.common.ValueIO;

public final class HRDFWriter extends AbstractRDFWriter {

    public static final class Factory implements RDFWriterFactory {

        @Override
        public RDFFormat getRDFFormat() {
            return HRDF.FORMAT;
        }

        @Override
        public RDFWriter getWriter(OutputStream out) {
            return new HRDFWriter(out);
        }

		@Override
		public RDFWriter getWriter(OutputStream out, String baseURI)
			throws URISyntaxException
		{
			return getWriter(out);
		}

		@Override
		public RDFWriter getWriter(Writer writer) {
			throw new UnsupportedOperationException();
		}

		@Override
		public RDFWriter getWriter(Writer writer, String baseURI)
			throws URISyntaxException
		{
			throw new UnsupportedOperationException();
		}
	}


	private final DataOutputStream out;
	private static final ValueIO.Writer valueWriter = ValueIO.getDefault().createWriter();

	public HRDFWriter(OutputStream out) {
		this.out = new DataOutputStream(out);
	}

	private Resource prevContext;
	private Resource prevSubject;
	private IRI prevPredicate;

	@Override
	public RDFFormat getRDFFormat() {
		return HRDF.FORMAT;
	}

	@Override
	protected void consumeStatement(Statement st) {
		Resource subj = st.getSubject();
		IRI pred = st.getPredicate();
		Resource c = st.getContext();
		ByteBuffer tmp = ByteBuffer.allocate(ValueIO.DEFAULT_BUFFER_SIZE);
		boolean contextUnchanged = (c == null) || c.equals(prevContext);
		boolean subjUnchanged = subj.equals(prevSubject);
		boolean predUnchanged = pred.equals(prevPredicate);
		int type = HRDF.CSPO;
		if (contextUnchanged) {
			type--;
			if (subjUnchanged) {
				type--;
				if (predUnchanged) {
					type--;
				}
			}
		}
		if (c != null) {
			type += HRDF.QUADS;
		}
		try {
			out.writeByte((byte) type);
			boolean skip = contextUnchanged;
			if (!skip) {
				tmp = valueWriter.writeValueWithSizeHeader(c, out, Short.BYTES, tmp);
			}
			skip = subjUnchanged && skip;
			if (!skip) {
				tmp = valueWriter.writeValueWithSizeHeader(subj, out, Short.BYTES, tmp);
			}
			skip = predUnchanged && skip;
			if (!skip) {
				tmp = valueWriter.writeValueWithSizeHeader(pred, out, Short.BYTES, tmp);
			}
			tmp = valueWriter.writeValueWithSizeHeader(st.getObject(), out, Integer.BYTES, tmp);
		} catch (IOException ioe) {
			throw new RDFHandlerException(ioe);
		}
		prevContext = c;
		prevSubject = subj;
		prevPredicate = pred;
	}

	@Override
	public void handleComment(String comment)
		throws RDFHandlerException
	{
	}

	@Override
	public void endRDF()
		throws RDFHandlerException
	{
		prevContext = null;
		prevSubject = null;
		prevPredicate = null;
	}
}
