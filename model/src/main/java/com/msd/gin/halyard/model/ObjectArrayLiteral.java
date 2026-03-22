package com.msd.gin.halyard.model;

import org.eclipse.rdf4j.model.Literal;
import org.json.JSONStringer;

public final class ObjectArrayLiteral extends AbstractArrayLiteral<Object[]> {
	private static final long serialVersionUID = 6409948385114916596L;

	private final Object[] values;
	private final Class<?> componentType;

	public ObjectArrayLiteral(Object... values) {
		this(values, Object.class);
	}

	public ObjectArrayLiteral(Object[] values, Class<?> componentType) {
		this.values = values;
		this.componentType = componentType;
	}

	@Override
	public String getLabel() {
		JSONStringer writer = new JSONStringer();
		writer.array();
		for (Object o : this.values) {
			writer.value(o);
		}
		writer.endArray();
		return writer.toString();
	}

	@Override
	public Object[] objectValue() {
		return values;
	}

	@Override
	public Class<?> componentType() {
		return componentType;
	}

	@Override
	public Object[] elements() {
		return values;
	}

	@Override
	public int length() {
		return values.length;
	}

	public static Object[] objectArray(Literal l) {
		return AbstractArrayLiteral.asArrayLiteral(l).elements();
	}
}
