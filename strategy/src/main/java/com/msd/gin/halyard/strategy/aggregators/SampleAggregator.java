/*
 * Copyright 2016 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
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
package com.msd.gin.halyard.strategy.aggregators;

import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.Sample;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;

public class SampleAggregator extends Aggregator {

	private final AtomicReference<Value> sample = new AtomicReference<>();

	public SampleAggregator(Sample op, EvaluationStrategy strategy) {
		super(op, strategy);
	}

	@Override
	public void process(BindingSet bs) {
		// we flip a coin to determine if we keep the current value or set a
		// new value to report.
		Optional<Value> newValue = null;
		if (sample.get() == null) {
			newValue = Optional.ofNullable(evaluate(bs));
			if (newValue.isPresent() && sample.compareAndSet(null, newValue.get())) {
				return;
			}
		}

		if (ThreadLocalRandom.current().nextFloat() < 0.5f) {
			if (newValue == null) {
				newValue = Optional.ofNullable(evaluate(bs));
			}
			newValue.ifPresent(sample::set);
		}
	}

	@Override
	public Value getValue() {
		return sample.get();
	}
}
