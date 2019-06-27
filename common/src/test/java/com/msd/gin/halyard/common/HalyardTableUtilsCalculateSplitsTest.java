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
package com.msd.gin.halyard.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.codec.binary.Hex;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 * @author Adam Sotona (MSD)
 */
@RunWith(Parameterized.class)
public class HalyardTableUtilsCalculateSplitsTest {

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {0, new String[]{}},
            {1, new String[]{"80"}},
            {2, new String[]{"40", "80", "c0"}},
            {3, new String[]{"20", "40", "60", "80", "a0", "c0", "e0"}},
            {4, new String[]{"10", "20", "30", "40", "50", "60", "70", "80", "90", "a0", "b0", "c0", "d0", "e0", "f0"}},
            {5, new String[]{"008000", "10", "108000", "20", "208000", "30", "308000", "40", "408000", "50", "508000", "60", "608000", "70", "708000", "80", "808000", "90", "908000", "a0", "a08000", "b0", "b08000", "c0", "c08000", "d0", "d08000", "e0", "e08000", "f0", "f08000"}},
            {6, new String[]{"004000", "008000", "00c000", "10", "104000", "108000", "10c000", "20", "204000", "208000", "20c000", "30", "304000", "308000", "30c000", "40", "404000", "408000", "40c000", "50", "504000", "508000", "50c000", "60", "604000", "608000", "60c000", "70", "704000", "708000", "70c000", "80", "804000", "808000", "80c000", "90", "904000", "908000", "90c000", "a0", "a04000", "a08000", "a0c000", "b0", "b04000", "b08000", "b0c000", "c0", "c04000", "c08000", "c0c000", "d0", "d04000", "d08000", "d0c000", "e0", "e04000", "e08000", "e0c000", "f0", "f04000", "f08000", "f0c000"}},
        });
    }

    private final int splits;
    private final String[] expected;

    public HalyardTableUtilsCalculateSplitsTest(int splits, String[] expected) {
        this.splits = splits;
        this.expected = expected;
    }

    @Test
    public void testCalculateSplits() {
        byte bb[][] = HalyardTableUtils.calculateSplits(splits);
        if (expected == null) {
            assertNull(bb);
        } else {
            assertEquals(expected.length, bb.length);
            for (int i = 0; i < expected.length; i++) {
                assertEquals(expected[i], Hex.encodeHexString(bb[i]));
            }
        }
    }

}
