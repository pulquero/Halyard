package com.msd.gin.halyard.sail.model.embedding;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import dev.langchain4j.model.localai.LocalAiEmbeddingModel;

public class EmbeddingModelQueryHelperProviderTest {
	@Test
	public void testLocalAI() throws Exception {
		EmbeddingModelQueryHelperProvider provider = new EmbeddingModelQueryHelperProvider();
		Map<String, String> config = new HashMap<>();
		config.put("model.class", LocalAiEmbeddingModel.class.getName());
		config.put("baseUrl", "http://localhost");
		config.put("modelName", "llama3");
		provider.createQueryHelper(config);
	}
}
