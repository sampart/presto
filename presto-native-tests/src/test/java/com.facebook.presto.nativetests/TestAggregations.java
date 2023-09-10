/*
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
package com.facebook.presto.nativetests;

import com.facebook.presto.nativeworker.NativeQueryRunnerUtils;
import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.H2QueryRunner;

public class TestAggregations
        extends AbstractTestAggregationsNative
{
    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner() throws Exception
    {
        return new H2QueryRunner();
    }

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        return PrestoNativeQueryRunnerUtils.createNativeQueryRunner(false);
    }

    @Override
    protected void createTables()
    {
        QueryRunner javaQueryRunner = null;
        try {
            javaQueryRunner = PrestoNativeQueryRunnerUtils.createJavaQueryRunner(true);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        NativeQueryRunnerUtils.createAllTables(javaQueryRunner);
        javaQueryRunner.close();
    }
}
