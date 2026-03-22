package com.msd.gin.halyard.model;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.util.Values;
import org.json.JSONTokener;

public final class JsonOrgJsonValueFactory implements JsonValueFactory {
	private final ValueFactory vf;

	public JsonOrgJsonValueFactory(ValueFactory vf) {
		this.vf = vf;
	}

	@Override
	public Literal createJsonLiteral(String json) {
		JSONTokener parser = new JSONTokener(json);
		char ch = parser.nextClean();
		parser.back();
		switch (ch) {
			case '{':
				return new MapLiteral(json);
			case '[':
				return AbstractArrayLiteral.create(json);
			default:
				return Values.literal(vf, parser.nextValue(), false);
		}
	}

}
