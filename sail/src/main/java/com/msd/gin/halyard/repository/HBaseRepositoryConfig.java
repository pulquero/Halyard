package com.msd.gin.halyard.repository;

import com.msd.gin.halyard.sail.HBaseSailConfig;

import java.util.Optional;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.util.Configurations;
import org.eclipse.rdf4j.model.util.ModelException;
import org.eclipse.rdf4j.model.vocabulary.CONFIG;
import org.eclipse.rdf4j.repository.config.AbstractRepositoryImplConfig;
import org.eclipse.rdf4j.repository.config.RepositoryConfigException;
import org.eclipse.rdf4j.sail.config.SailConfigException;
import org.eclipse.rdf4j.sail.config.SailFactory;
import org.eclipse.rdf4j.sail.config.SailRegistry;

import static org.eclipse.rdf4j.repository.sail.config.SailRepositorySchema.*;
import static org.eclipse.rdf4j.sail.config.SailConfigSchema.*;

public class HBaseRepositoryConfig extends AbstractRepositoryImplConfig {
	private HBaseSailConfig sailImplConfig;

	public HBaseRepositoryConfig() {
		super(HBaseRepositoryFactory.REPOSITORY_TYPE);
	}

	public HBaseRepositoryConfig(HBaseSailConfig cfg) {
		super(HBaseRepositoryFactory.REPOSITORY_TYPE);
		setSailImplConfig(cfg);
	}

	public HBaseSailConfig getSailImplConfig() {
		return sailImplConfig;
	}

	public void setSailImplConfig(HBaseSailConfig cfg) {
		sailImplConfig = cfg;
	}

	@Override
	public void validate() throws RepositoryConfigException {
		super.validate();
		if (sailImplConfig == null) {
			throw new RepositoryConfigException("No Sail implementation specified for Sail repository");
		}

		try {
			sailImplConfig.validate();
		} catch (SailConfigException e) {
			throw new RepositoryConfigException(e.getMessage(), e);
		}
	}

	@Override
	public Resource export(Model model) {
		Resource repImplNode = super.export(model);

		if (sailImplConfig != null) {
			model.setNamespace(CONFIG.NS);
			Resource sailImplNode = sailImplConfig.export(model);
			model.add(repImplNode, CONFIG.Sail.impl, sailImplNode);
		}

		return repImplNode;
	}

	@Override
	public void parse(Model model, Resource repImplNode) throws RepositoryConfigException {
		try {
			Optional<Resource> sailImplNode = Configurations.getResourceValue(model, repImplNode, CONFIG.Sail.impl, SAILIMPL);
			if (sailImplNode.isPresent()) {
				Configurations.getLiteralValue(model, sailImplNode.get(), CONFIG.Sail.type, SAILTYPE).ifPresent(typeLit -> {
					SailFactory factory = SailRegistry.getInstance().get(typeLit.getLabel()).orElseThrow(() -> new RepositoryConfigException("Unsupported Sail type: " + typeLit.getLabel()));

					sailImplConfig = (HBaseSailConfig) factory.getConfig();
					sailImplConfig.parse(model, sailImplNode.get());
				});
			}
		} catch (ModelException | SailConfigException e) {
			throw new RepositoryConfigException(e.getMessage(), e);
		}
	}
}
