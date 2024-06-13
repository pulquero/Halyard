package com.msd.gin.halyard.common;

import java.util.Objects;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.base.AbstractStatement;

public class TimestampedValueFactory extends IdValueFactory {
	private static final long serialVersionUID = -9091778283847281070L;

	public TimestampedValueFactory(@Nullable RDFFactory rdfFactory) {
		super(rdfFactory);
	}

	@Override
	public Statement createStatement(Resource subject, IRI predicate, Value object) {
		return new TimestampedStatement(subject, predicate, object);
	}

	@Override
	public Statement createStatement(Resource subject, IRI predicate, Value object, Resource context) {
		return new TimestampedContextStatement(subject, predicate, object, context);
	}


	static final class TimestampedStatement extends AbstractStatement implements Timestamped {
		private static final long serialVersionUID = -3767773807995225622L;
		private final Resource subject;
		private final IRI predicate;
		private final Value object;
		private long ts;

		TimestampedStatement(Resource subject, IRI predicate, Value object) {
			this.subject = Objects.requireNonNull(subject);
			this.predicate = Objects.requireNonNull(predicate);
			this.object = Objects.requireNonNull(object);
		}

		@Override
		public Resource getSubject() {
			return subject;
		}

		@Override
		public IRI getPredicate() {
			return predicate;
		}

		@Override
		public Value getObject() {
			return object;
		}

		@Override
		public Resource getContext() {
			return null;
		}

		@Override
		public long getTimestamp() {
			return ts;
		}

		@Override
		public void setTimestamp(long ts) {
			this.ts = ts;
		}

		public String toString() {
			return super.toString()+" {"+ts+"}";
		}
	}

	static final class TimestampedContextStatement extends AbstractStatement implements Timestamped {
		private static final long serialVersionUID = 6631073994472700544L;
		private final Resource subject;
		private final IRI predicate;
		private final Value object;
		private final Resource context;
		private long ts;

		TimestampedContextStatement(Resource subject, IRI predicate, Value object, Resource context) {
			this.subject = Objects.requireNonNull(subject);
			this.predicate = Objects.requireNonNull(predicate);
			this.object = Objects.requireNonNull(object);
			this.context = context;
		}

		@Override
		public Resource getSubject() {
			return subject;
		}

		@Override
		public IRI getPredicate() {
			return predicate;
		}

		@Override
		public Value getObject() {
			return object;
		}

		@Override
		public Resource getContext() {
			return context;
		}

		@Override
		public long getTimestamp() {
			return ts;
		}

		@Override
		public void setTimestamp(long ts) {
			this.ts = ts;
		}

		public String toString() {
			return super.toString()+" {"+ts+"}";
		}
	}
}
