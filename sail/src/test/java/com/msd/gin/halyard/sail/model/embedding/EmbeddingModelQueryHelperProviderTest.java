package com.msd.gin.halyard.sail.model.embedding;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import dev.langchain4j.model.embedding.EmbeddingModel;

public class EmbeddingModelQueryHelperProviderTest {
	@Test
	public void testProvider() throws Exception {
		EmbeddingModelQueryHelperProvider provider = new EmbeddingModelQueryHelperProvider();
		Map<String, String> config = new HashMap<>();
		config.put("model.class", dev.langchain4j.model.embedding.onnx.allminilml6v2q.AllMiniLmL6V2QuantizedEmbeddingModel.class.getName());
		EmbeddingModel model = provider.createQueryHelper(config);
		model.embed("foobar");
	}
}
