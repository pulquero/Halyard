/*
 * Copyright 2019 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
 * Inc., Kenilworth, NJ, USA.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.msd.gin.halyard.repository;

import com.msd.gin.halyard.common.TableConfig;
import com.msd.gin.halyard.sail.HBaseSail;
import com.msd.gin.halyard.sail.HBaseSailFactory;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.http.client.HttpClient;
import org.eclipse.rdf4j.common.iteration.Iterations;
import org.eclipse.rdf4j.http.client.HttpClientDependent;
import org.eclipse.rdf4j.http.client.SessionManagerDependent;
import org.eclipse.rdf4j.http.client.SharedHttpClientSessionManager;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.vocabulary.CONFIG;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolverClient;
import org.eclipse.rdf4j.repository.DelegatingRepository;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.RepositoryResolverClient;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.config.DelegatingRepositoryImplConfig;
import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.eclipse.rdf4j.repository.config.RepositoryConfigException;
import org.eclipse.rdf4j.repository.config.RepositoryConfigSchema;
import org.eclipse.rdf4j.repository.config.RepositoryFactory;
import org.eclipse.rdf4j.repository.config.RepositoryImplConfig;
import org.eclipse.rdf4j.repository.config.RepositoryRegistry;
import org.eclipse.rdf4j.repository.manager.RepositoryInfo;
import org.eclipse.rdf4j.repository.manager.RepositoryManager;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sparql.federation.SPARQLServiceResolver;

/**
 *
 * @author Adam Sotona (MSD)
 */
public final class HBaseRepositoryManager extends RepositoryManager {
	private static final String SYSTEM_TABLE = "RDF4JSYSTEM";
	// ensure different from RepositoryConfigRepository.ID
	private static final String SYSTEM_ID = "system";

	private final File baseDir;
	private final Configuration config;
	private Connection conn;
	private volatile SharedHttpClientSessionManager client;
    private volatile SPARQLServiceResolver serviceResolver;

	public static Repository createSystemRepository(Connection hconn, Configuration conf) throws RepositoryException {
		Configuration sysRepoConfig = new Configuration(conf);
		sysRepoConfig.set(TableConfig.ID_HASH, "Murmur3-128");
		// don't bother splitting such a small table
		SailRepository repo = new SailRepository(new HBaseSail(hconn, sysRepoConfig, SYSTEM_TABLE, true, -1, true, 180, null, null));
		repo.init();
		return repo;
	}

	public HBaseRepositoryManager(File baseDir) throws IOException {
		this(baseDir, HBaseConfiguration.create());
	}

	HBaseRepositoryManager(File baseDir, Configuration conf) {
		this.baseDir = baseDir;
		this.config = conf;
    }

    @Override
	public void init() throws RepositoryException {
		try {
			conn = HBaseSailFactory.initSharedConnection(config);
		} catch (IOException ioe) {
			throw new RepositoryException(ioe);
		}
		super.init();
	}

	@Override
	public URL getLocation() throws MalformedURLException {
		return baseDir.toURI().toURL();
	}

    private SharedHttpClientSessionManager getSesameClient() {
        SharedHttpClientSessionManager result = client;
        if (result == null) {
            synchronized (this) {
                result = client;
                if (result == null) {
                    result = client = new SharedHttpClientSessionManager();
                }
            }
        }
        return result;
    }

    @Override
    public HttpClient getHttpClient() {
        SharedHttpClientSessionManager nextClient = client;
        if (nextClient == null) {
            return null;
        } else {
            return nextClient.getHttpClient();
        }
    }

    @Override
    public void setHttpClient(HttpClient httpClient) {
        getSesameClient().setHttpClient(httpClient);
    }

    private FederatedServiceResolver getFederatedServiceResolver() {
        SPARQLServiceResolver result = serviceResolver;
        if (result == null) {
            synchronized (this) {
                result = serviceResolver;
                if (result == null) {
                    result = serviceResolver = new SPARQLServiceResolver();
                    result.setHttpClientSessionManager(getSesameClient());
                }
            }
        }
        return result;
    }

    @Override
    public void shutDown() {
        try {
            super.shutDown();
			try {
				HBaseSailFactory.closeSharedConnection();
			} catch (IOException ignore) {
			}
        } finally {
            try {
                SPARQLServiceResolver toCloseServiceResolver = serviceResolver;
                serviceResolver = null;
                if (toCloseServiceResolver != null) {
                    toCloseServiceResolver.shutDown();
                }
            } finally {
                SharedHttpClientSessionManager toCloseClient = client;
                client = null;
                if (toCloseClient != null) {
                    toCloseClient.shutDown();
                }
            }
        }
    }

    @Override
    protected Repository createRepository(String id) throws RepositoryConfigException, RepositoryException {
		final Repository repository;
		if (SYSTEM_ID.equals(id)) {
			repository = createSystemRepository(conn, conn.getConfiguration());
		} else {
			RepositoryConfig repConfig = getRepositoryConfig(id);
			if (repConfig != null) {
				repConfig.validate();
				repository = createRepositoryStack(repConfig.getRepositoryImplConfig());
				repository.init();
			} else {
				repository = null;
			}
		}
		return repository;
    }

    private Repository createRepositoryStack(RepositoryImplConfig implConfig) throws RepositoryConfigException {
        RepositoryFactory factory = RepositoryRegistry.getInstance()
            .get(implConfig.getType())
            .orElseThrow(() -> new RepositoryConfigException("Unsupported repository type: " + implConfig.getType()));
        Repository repository = factory.getRepository(implConfig);
        if (repository instanceof RepositoryResolverClient) {
            ((RepositoryResolverClient) repository).setRepositoryResolver(this);
        }
        if (repository instanceof FederatedServiceResolverClient) {
            ((FederatedServiceResolverClient) repository).setFederatedServiceResolver(getFederatedServiceResolver());
        }
        if (repository instanceof SessionManagerDependent) {
            ((SessionManagerDependent) repository).setHttpClientSessionManager(client);
        } else if (repository instanceof HttpClientDependent) {
            ((HttpClientDependent) repository).setHttpClient(getHttpClient());
        }
        if (implConfig instanceof DelegatingRepositoryImplConfig) {
            RepositoryImplConfig delegateConfig = ((DelegatingRepositoryImplConfig) implConfig).getDelegate();
            Repository delegate = createRepositoryStack(delegateConfig);
            try {
                ((DelegatingRepository) repository).setDelegate(delegate);
            } catch (ClassCastException e) {
                throw new RepositoryConfigException("Delegate specified for repository that is not a DelegatingRepository: " + delegate.getClass(), e);
            }
        }
        return repository;
    }

    @Override
    public RepositoryInfo getRepositoryInfo(String id) {
		RepositoryInfo repInfo;
		RepositoryConfig config = getRepositoryConfig(id);
		if (config != null) {
			repInfo = new RepositoryInfo();
			repInfo.setId(config.getID());
			repInfo.setDescription(config.getTitle());
			repInfo.setReadable(true);
			repInfo.setWritable(true);
		} else {
			repInfo = null;
		}
        return repInfo;
    }

    @Override
    public RepositoryConfig getRepositoryConfig(String repositoryID) throws RepositoryConfigException, RepositoryException {
		if (SYSTEM_ID.equals(repositoryID)) {
			return new RepositoryConfig(SYSTEM_ID, "System repository");
		} else {
			Repository systemRepository = getSystemRepository();
			return getRepositoryConfig(systemRepository, repositoryID);
		}
	}

	@Override
	public boolean hasRepositoryConfig(String repositoryID) throws RepositoryException, RepositoryConfigException {
		return getRepositoryConfig(repositoryID) != null;
	}

	public Repository getSystemRepository() {
		if (!isInitialized()) {
			throw new IllegalStateException("Repository Manager is not initialized");
		}
		return getRepository(SYSTEM_ID);
	}

	@Override
	public void addRepositoryConfig(RepositoryConfig repoConfig) throws RepositoryException, RepositoryConfigException {
		if (!SYSTEM_ID.equals(repoConfig.getID())) {
			Repository systemRepository = getSystemRepository();
			updateRepositoryConfig(systemRepository, repoConfig);
		}
	}

	@Override
	public boolean removeRepository(String repositoryID) throws RepositoryException, RepositoryConfigException {
		boolean removed = false;
		if (!SYSTEM_ID.equals(repositoryID)) {
			removed = super.removeRepository(repositoryID);
			Repository systemRepository = getSystemRepository();
			removeRepositoryConfig(systemRepository, repositoryID);
		}
		return removed;
	}

	@Override
	public Collection<RepositoryInfo> getAllRepositoryInfos() throws RepositoryException {
		List<RepositoryInfo> result = new ArrayList<>();
		result.add(getRepositoryInfo(SYSTEM_ID));
		for (String id : getRepositoryIDs(getSystemRepository())) {
			RepositoryInfo repInfo = getRepositoryInfo(id);
			result.add(repInfo);
		}
		Collections.sort(result);
		return result;
	}

	private static RepositoryConfig getRepositoryConfig(Repository repository, String repositoryID) throws RepositoryConfigException, RepositoryException {
		try (RepositoryConnection con = repository.getConnection()) {
			Statement idStatement = getIDStatement(con, repositoryID);
			if (idStatement == null) {
				// No such config
				return null;
			}

			Resource repositoryNode = idStatement.getSubject();
			Resource context = idStatement.getContext();

			if (context == null) {
				throw new RepositoryException("No configuration context for repository " + repositoryID);
			}

			Model contextGraph = QueryResults.asModel(con.getStatements(null, null, null, true, context));

			return RepositoryConfig.create(contextGraph, repositoryNode);
		}
	}

	private static Set<String> getRepositoryIDs(Repository repository) throws RepositoryException {
		try (RepositoryConnection con = repository.getConnection()) {
			Set<String> idSet = new LinkedHashSet<>();

			try (RepositoryResult<Statement> idStatementIter = con.getStatements(null, CONFIG.Rep.id, null, true)) {
				while (idStatementIter.hasNext()) {
					Statement idStatement = idStatementIter.next();

					if (idStatement.getObject() instanceof Literal) {
						Literal idLiteral = (Literal) idStatement.getObject();
						idSet.add(idLiteral.getLabel());
					}
				}
			}

			try (RepositoryResult<Statement> idStatementIter = con.getStatements(null, RepositoryConfigSchema.REPOSITORYID, null, true)) {
				while (idStatementIter.hasNext()) {
					Statement idStatement = idStatementIter.next();

					if (idStatement.getObject() instanceof Literal) {
						Literal idLiteral = (Literal) idStatement.getObject();
						idSet.add(idLiteral.getLabel());
					}
				}
			}

			return idSet;
		}
	}

	private static void updateRepositoryConfig(Repository repository, RepositoryConfig config) throws RepositoryException, RepositoryConfigException {
		ValueFactory vf = repository.getValueFactory();

		try (RepositoryConnection con = repository.getConnection()) {
			con.begin();

			Resource context = getContext(con, config.getID());

			if (context != null) {
				con.clear(context);
			} else {
				context = vf.createBNode();
			}

			Model graph = new LinkedHashModel();
			config.export(graph);
			con.add(graph, context);

			con.commit();
		}
	}

	private static boolean removeRepositoryConfig(Repository repository, String repositoryID) throws RepositoryException, RepositoryConfigException {
		boolean changed = false;

		try (RepositoryConnection con = repository.getConnection()) {
			con.begin();

			Resource context = getContext(con, repositoryID);
			if (context != null) {
				con.clear(context);
				changed = true;
			}

			con.commit();
		}

		return changed;
	}

	private static Resource getContext(RepositoryConnection con, String repositoryID) throws RepositoryException, RepositoryConfigException {
		Resource context = null;

		Statement idStatement = getIDStatement(con, repositoryID);
		if (idStatement != null) {
			context = idStatement.getContext();
		}

		return context;
	}

	private static Statement getIDStatement(RepositoryConnection con, String repositoryID) throws RepositoryException, RepositoryConfigException {
		Literal idLiteral = con.getRepository().getValueFactory().createLiteral(repositoryID);
		List<Statement> idStatementList = Iterations.asList(con.getStatements(null, CONFIG.Rep.id, idLiteral, true));
		if (idStatementList.isEmpty()) {
			idStatementList = Iterations.asList(con.getStatements(null, RepositoryConfigSchema.REPOSITORYID, idLiteral, true));
		}

		if (idStatementList.size() == 1) {
			return idStatementList.get(0);
		} else if (idStatementList.isEmpty()) {
			return null;
		} else {
			throw new RepositoryConfigException("Multiple ID-statements for repository ID " + repositoryID);
		}
	}
}
