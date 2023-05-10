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
package com.msd.gin.halyard.strategy;

import static junit.framework.TestCase.assertNotNull;

import com.msd.gin.halyard.algebra.evaluation.EmptyTripleSource;

import org.apache.hadoop.conf.Configuration;
import org.eclipse.rdf4j.repository.sparql.federation.SPARQLServiceResolver;
import org.junit.Test;

/**
 * @author Adam Sotona (MSD)
 */
public class HalyardStrategyServiceTest {

    @Test
    public void testGetService() {
        assertNotNull(new HalyardEvaluationStrategy(new Configuration(), new EmptyTripleSource(), null, new SPARQLServiceResolver(), null).getService("http://whatever/endpoint"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testServiceEvaluateFail() {
        new HalyardEvaluationStrategy(new Configuration(), new EmptyTripleSource(), null, null, null).evaluate(null, null, null);
    }
}
