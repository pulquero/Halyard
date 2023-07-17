package com.msd.gin.halyard.strategy.collections;

import com.msd.gin.halyard.common.ValueIO;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.nio.ByteBuffer;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.mapdb.Serializer;

public abstract class AbstractValueSerializer<E> implements Serializer<E>, Externalizable {
	protected static final ValueIO.Writer WRITER = ValueIO.getDefaultWriter();
	protected static final ValueIO.Reader READER = ValueIO.getDefaultReader();
	protected transient ValueFactory vf;

	protected AbstractValueSerializer(ValueFactory vf) {
		this.vf = vf;
	}

	protected final ByteBuffer newTempBuffer() {
		return ByteBuffer.allocate(ValueIO.DEFAULT_BUFFER_SIZE);
	}

	protected final void writeValue(Value value, DataOutput out, ByteBuffer tmp) throws IOException {
		WRITER.writeValueWithSizeHeader(value, out, Integer.BYTES, tmp);
	}

	protected final Value readValue(DataInput in) throws IOException {
		return READER.readValueWithSizeHeader(in, vf, Integer.BYTES);
	}

	protected final void writeBindingSet(BindingSet bs, DataOutput out, ByteBuffer tmp) throws IOException {
		out.writeInt(bs.size());
		for (Binding b : bs) {
			out.writeUTF(b.getName());
			writeValue(b.getValue(), out, tmp);
		}
	}

	protected final BindingSet readBindingSet(DataInput in) throws IOException {
		int size = in.readInt();
		QueryBindingSet bs = new QueryBindingSet(size);
		for (int i=0; i<size; i++) {
			String name = in.readUTF();
			Value v = readValue(in);
			bs.addBinding(name, v);
		}
		return bs;
	}

	@Override
	public final int fixedSize() {
		return -1;
	}

	@Override
	public final void writeExternal(ObjectOutput out) throws IOException {
		if (vf instanceof Serializable) {
			out.write(1);
			out.writeObject(vf);
		} else {
			out.write(0);
		}
	}

	@Override
	public final void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		if (in.read() > 0) {
			vf = (ValueFactory) in.readObject();
		} else {
			vf = SimpleValueFactory.getInstance();
		}
	}
}