/*
 * Copyright 2018 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
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
package com.msd.gin.halyard.tools;

import com.msd.gin.halyard.common.IdValueFactory;
import com.msd.gin.halyard.common.RDFContext;
import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.common.RDFObject;
import com.msd.gin.halyard.common.RDFPredicate;
import com.msd.gin.halyard.common.RDFSubject;
import com.msd.gin.halyard.common.StatementIndices;
import com.msd.gin.halyard.model.ValueConstraint;
import com.msd.gin.halyard.optimizers.ConstrainedValueOptimizer;
import com.msd.gin.halyard.optimizers.InvalidConstraintException;
import com.msd.gin.halyard.query.algebra.Algebra;
import com.msd.gin.halyard.query.algebra.BGPCollector;
import com.msd.gin.halyard.query.algebra.ConstrainedStatementPattern;
import com.msd.gin.halyard.query.algebra.ExtendedQueryRoot;
import com.msd.gin.halyard.query.algebra.ExtendedTupleFunctionCall;
import com.msd.gin.halyard.query.algebra.SkipVarsQueryModelVisitor;
import com.msd.gin.halyard.query.algebra.VarConstraint;
import com.msd.gin.halyard.query.algebra.evaluation.PartitionedIndex;
import com.msd.gin.halyard.query.algebra.evaluation.function.ParallelSplitFunction;
import com.msd.gin.halyard.strategy.HalyardQueryOptimizerPipeline;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.IntPredicate;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.algebra.Extension;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.Modify;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UpdateExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.ParsedUpdate;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Adam Sotona (MSD)
 */
final class QueryInputFormat extends InputFormat<NullWritable, Void> {
	private static final Logger LOGGER = LoggerFactory.getLogger(QueryInputFormat.class);

    public static final String PREFIX =  "mapreduce.input.queryinputformat.";
    public static final String QUERIES = PREFIX + "queries";
    public static final String STAGE = PREFIX + "stage";
    public static final String QUERY_SUFFIX = ".query";
    public static final String REPEAT_SUFFIX = ".repeat";

    public static void addQuery(Configuration conf, String name, String query, int stage, BindingSet bindings) {
        Collection<String> qNames = conf.getStringCollection(QUERIES);
        qNames.add(name);
        conf.set(PREFIX + name + QUERY_SUFFIX, query);
		int repeatCount = ParallelSplitFunction.getNumberOfPartitionsFromFunctionArgument(query, stage, bindings);
        conf.setInt(PREFIX + name + REPEAT_SUFFIX, repeatCount);
        conf.setStrings(QUERIES, qNames.toArray(new String[qNames.size()]));
    }

    private static void addQuery(Configuration conf, FileStatus fileStatus, int stage, BindingSet bindings) throws IOException {
        Path path = fileStatus.getPath();
        try (FSDataInputStream in = path.getFileSystem(conf).open(path)) {
            byte buffer[] = new byte[(int)fileStatus.getLen()];
            IOUtils.readFully(in, buffer);
            String name = path.getName();
            String query = Bytes.toString(buffer);
            addQuery(conf, name, query, stage, bindings);
        }
    }

    private static void addQueryRecursively(Configuration conf, Path path, int stage, BindingSet bindings)
        throws IOException {
        RemoteIterator<LocatedFileStatus> iter = path.getFileSystem(conf).listLocatedStatus(path);
        while (iter.hasNext()) {
            LocatedFileStatus stat = iter.next();
            if (stat.isDirectory()) {
                addQueryRecursively(conf, stat.getPath(), stage, bindings);
            } else {
                addQuery(conf, stat, stage, bindings);
            }
        }
    }

    public static void setQueriesFromDirRecursive(Configuration conf, String dirs, int stage, BindingSet bindings) throws IOException {
        for (String dir : StringUtils.split(dirs)) {
            Path p = new Path(StringUtils.unEscapeString(dir));
            FileStatus[] matches = p.getFileSystem(conf).globStatus(p);
            if (matches == null) {
                throw new IOException("Input path does not exist: " + p);
            } else if (matches.length == 0) {
                throw new IOException("Input Pattern " + p + " matches 0 files");
            } else {
                for (FileStatus globStat : matches) {
                    if (globStat.isDirectory()) {
                        addQueryRecursively(conf, p, stage, bindings);
                    } else {
                        addQuery(conf, globStat, stage, bindings);
                    }
                }
            }
        }
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        List<InputSplit> splits = new ArrayList<>();
        Configuration conf = context.getConfiguration();
        RDFFactory rdfFactory = RDFFactory.create(conf);
        StatementIndices stmtIndices = new StatementIndices(conf, rdfFactory);
        int stage = conf.getInt(STAGE, -1);
        for (String qName : conf.getStringCollection(QUERIES)) {
            int repeatCount = conf.getInt(PREFIX + qName + REPEAT_SUFFIX, 1);
            String query = conf.get(PREFIX + qName + QUERY_SUFFIX);

        	BindingSet bindings = AbstractHalyardTool.getBindings(conf, new IdValueFactory(rdfFactory));
            IntPredicate indexFilter = getIndexFilter(qName, query, stage, stmtIndices, bindings);

            for (int i=0; i<repeatCount; i++) {
            	if (indexFilter == null || indexFilter.test(i)) {
            		splits.add(new QueryInputSplit(qName, query, i));
            	}
            }
        }
        return splits;
    }

    @Override
    public RecordReader<NullWritable, Void> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        final QueryInputSplit qis = (QueryInputSplit) split;
        return new RecordReader<NullWritable, Void>() {
            @Override
            public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                return true;
            }

            @Override
            public NullWritable getCurrentKey() throws IOException, InterruptedException {
                return null;
            }

            @Override
            public Void getCurrentValue() throws IOException, InterruptedException {
                return null;
            }

            @Override
            public float getProgress() throws IOException, InterruptedException {
                return qis.progress;
            }

            @Override
            public void close() throws IOException {
            }
        };
    }

    static final class QueryInputSplit extends InputSplit implements Writable {

        public static QueryInputSplit read(DataInput in) throws IOException {
            QueryInputSplit iis = new QueryInputSplit();
            iis.readFields(in);
            return iis;
        }

        private String queryName, query;
        private int repeatIndex;
        private float progress;

        public QueryInputSplit() {
        }

        public QueryInputSplit(String queryName, String query, int repeatIndex) {
            this.queryName = queryName;
            this.query = query;
            this.repeatIndex = repeatIndex;
        }

        public String getQueryName() {
            return queryName;
        }

        public String getQuery() {
            return query;
        }

        public int getRepeatIndex() {
            return repeatIndex;
        }

        public void setProgress(float p) {
            this.progress = p;
        }

        @Override
        public long getLength() throws IOException, InterruptedException {
            return 0l;
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            return new String[0];
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(queryName);
            out.writeUTF(query);
            out.writeInt(repeatIndex);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            queryName = in.readUTF();
            query = in.readUTF();
            repeatIndex = in.readInt();
        }
    }

    private static IntPredicate getIndexFilter(String queryName, String query, int stage, StatementIndices stmtIndices, BindingSet bindings) {
        TupleExpr where;
        Dataset dataset;
        if (stage >= 0) {
            ParsedUpdate pu = QueryParserUtil.parseUpdate(QueryLanguage.SPARQL, query, null);
            UpdateExpr expr = pu.getUpdateExprs().get(stage);
            if (expr instanceof Modify) {
            	where = ((Modify)expr).getWhereExpr();
            	dataset = pu.getDatasetMapping().get(expr);
            } else {
            	where = null;
            	dataset = null;
            }
        } else {
            ParsedQuery pq = QueryParserUtil.parseQuery(QueryLanguage.SPARQL, query, null);
            where = pq.getTupleExpr();
            dataset = pq.getDataset();
        }

        IntPredicate indexFilter = null;
        if (where != null) {
        	where = new ExtendedQueryRoot(where);
        	for (QueryOptimizer optimizer : HalyardQueryOptimizerPipeline.getConstraintValueOptimizerPipeline()) {
        		optimizer.optimize(where, dataset, bindings);
        	}

        	List<ConstrainedStatementPattern> partitionedCSPs = new ArrayList<>();
        	where.visit(new SkipVarsQueryModelVisitor<RuntimeException>() {
        		@Override
        		public void meet(Join node) {
        			BGPCollector<RuntimeException> collector = new BGPCollector<>(this);
        			node.visit(collector);
        			for (StatementPattern sp : collector.getStatementPatterns()) {
        				if (sp instanceof ConstrainedStatementPattern) {
        					ConstrainedStatementPattern csp = (ConstrainedStatementPattern) sp;
        					VarConstraint varConstraint = csp.getConstraint();
        					if (varConstraint.isPartitioned() && varConstraint.getValueType() != null) {
        						partitionedCSPs.add(csp);
        					}
        				}
        			}
        		}

        		@Override
        		public void meet(QueryRoot node) {
        			node.getArg().visit(this);
        		}

        		@Override
        		public void meet(Projection node) {
        			node.getArg().visit(this);
        		}

        		@Override
        		public void meet(Filter node) {
        			node.getArg().visit(this);
        		}

        		@Override
        		public void meet(Extension node) {
        			node.getArg().visit(this);
        		}

        		@Override
        		public void meet(ExtendedTupleFunctionCall node) {
        			node.getDependentExpression().visit(this);
        		}

        		@Override
        		protected void meetNode(QueryModelNode node) {
        			// stop visiting
        		}
        	});

        	if (!partitionedCSPs.isEmpty()) {
        		RDFFactory rdfFactory = stmtIndices.getRDFFactory();
        		IntPredicate[] subFilters = new IntPredicate[partitionedCSPs.size()];
        		for (int i=0; i<subFilters.length; i++) {
        			ConstrainedStatementPattern csp = partitionedCSPs.get(i);
					VarConstraint varConstraint = csp.getConstraint();
	        		PartitionedIndex partitionedIndex = new PartitionedIndex(csp.getIndexToPartition(), varConstraint.getPartitionCount());
	        		try {
	        			ValueConstraint constraint = ConstrainedValueOptimizer.toValueConstraint(varConstraint, bindings);
	            		RDFSubject subj = rdfFactory.createSubject((Resource) Algebra.getVarValue(csp.getSubjectVar(), bindings));
	            		RDFPredicate pred = rdfFactory.createPredicate((IRI) Algebra.getVarValue(csp.getPredicateVar(), bindings));
	            		RDFObject obj = rdfFactory.createObject(Algebra.getVarValue(csp.getObjectVar(), bindings));
	            		RDFContext ctx = rdfFactory.createContext((Resource) Algebra.getVarValue(csp.getContextVar(), bindings));
	            		subFilters[i] = idx ->
		            		stmtIndices.scanWithConstraint(subj, pred, obj, ctx,
		            		csp.getConstrainedRole(), idx, partitionedIndex, constraint) != null;
	        		} catch (InvalidConstraintException constraintEx) {
	        			LOGGER.warn("Invalid constraint", constraintEx);
	        			indexFilter = idx -> false;
	        			break;
	        		}
        		}
        		if (indexFilter == null) {
        			indexFilter = (subFilters.length > 1) ? new AndIntPredicate(subFilters) : subFilters[0];
        		}
        	}
        }
        return indexFilter;
    }

    static class AndIntPredicate implements IntPredicate {
    	final IntPredicate[] preds;

    	AndIntPredicate(IntPredicate... preds) {
    		this.preds = preds;
    	}

    	@Override
    	public boolean test(int x) {
			for (int i=0; i<preds.length; i++) {
				if (!preds[i].test(i)) {
					return false;
				}
			}
			return true;
    	}
    }
}
