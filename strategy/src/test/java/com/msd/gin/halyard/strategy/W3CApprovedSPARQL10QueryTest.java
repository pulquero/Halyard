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
package com.msd.gin.halyard.strategy;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.dataset.DatasetRepository;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.testsuite.query.parser.sparql.manifest.SPARQLQueryComplianceTest;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class W3CApprovedSPARQL10QueryTest extends SPARQLQueryComplianceTest {

	private static final String[] defaultIgnoredTests = {
			// incompatible with SPARQL 1.1 - syntax for decimals was modified
			"Basic - Term 6",
			// incompatible with SPARQL 1.1 - syntax for decimals was modified
			"Basic - Term 7",
			// Test is incorrect: assumes timezoned date is comparable with non-timezoned
			"date-2",
			// Incompatible with SPARQL 1.1 - string-typed literals and plain literals are identical
			"Strings: Distinct",
			// Incompatible with SPARQL 1.1 - string-typed literals and plain literals are identical
			"All: Distinct",
			// Incompatible with SPARQL 1.1 - string-typed literals and plain literals are identical
			"SELECT REDUCED ?x with strings",
			// incompatible with non-strict date comparisons
			"date-3",
			"open-cmp-01",
			"open-cmp-02",
			// incompatible with non-sequential retrieval
			"SELECT REDUCED *"
	};

	private static final List<String> excludedSubdirs = Arrays.asList("service");

    public W3CApprovedSPARQL10QueryTest() {
    	super(excludedSubdirs);
		for (String ig : defaultIgnoredTests) {
			addIgnoredTest(ig);
		}
    }

    @TestFactory
	public Collection<DynamicTest> tests() {
		return getTestData("testcases-sparql-1.0-w3c/data-r2/manifest-evaluation.ttl");
	}

    @Override
    protected Repository newRepository() {
        return new DatasetRepository(new SailRepository(new MockSailWithHalyardStrategy()));
    }
}
