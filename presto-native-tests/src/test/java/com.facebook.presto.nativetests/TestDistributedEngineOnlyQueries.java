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

import com.facebook.presto.Session;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.nativeworker.NativeQueryRunnerUtils;
import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import org.intellij.lang.annotations.Language;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkState;
import static org.testng.Assert.assertEquals;

public class TestDistributedEngineOnlyQueries
        extends AbstractTestEngineOnlyQueries
{
    private String failureMsg = "inferredType Failed to parse type .*time.* Type not registered";
    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        return PrestoNativeQueryRunnerUtils.createNativeQueryRunner(false);
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner() throws Exception
    {
        return PrestoNativeQueryRunnerUtils.createJavaQueryRunner();
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

    @Override
    public void testTimeLiterals()
    {
        Session chicago = Session.builder(getSession()).setTimeZoneKey(TimeZoneKey.getTimeZoneKey("America/Chicago")).build();
        Session kathmandu = Session.builder(getSession()).setTimeZoneKey(TimeZoneKey.getTimeZoneKey("Asia/Kathmandu")).build();

        assertEquals(computeScalar("SELECT DATE '2013-03-22'"), LocalDate.of(2013, 3, 22));
        assertQuery("SELECT DATE '2013-03-22'");
        assertQuery(chicago, "SELECT DATE '2013-03-22'");
        assertQuery(kathmandu, "SELECT DATE '2013-03-22'");

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT TIME '3:04:05'", failureMsg, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT TIME '3:04:05.123'", failureMsg, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT TIME '3:04:05'", failureMsg, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT TIME '0:04:05'", failureMsg, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(chicago, "SELECT TIME '3:04:05'", failureMsg, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(kathmandu, "SELECT TIME '3:04:05'", failureMsg, true);

        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT TIME '01:02:03.400 Z'", failureMsg, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT TIME '01:02:03.400 UTC'", failureMsg, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT TIME '3:04:05 +06:00'", failureMsg, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT TIME '3:04:05 +0507'", failureMsg, true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("SELECT TIME '3:04:05 +03'", failureMsg, true);

        assertEquals(computeScalar("SELECT TIMESTAMP '1960-01-22 3:04:05'"), LocalDateTime.of(1960, 1, 22, 3, 4, 5));
        assertEquals(computeScalar("SELECT TIMESTAMP '1960-01-22 3:04:05.123'"), LocalDateTime.of(1960, 1, 22, 3, 4, 5, 123_000_000));
        assertQuery("SELECT TIMESTAMP '1960-01-22 3:04:05'");
        assertQuery("SELECT TIMESTAMP '1960-01-22 3:04:05.123'");
        // TODO #7122 assertQuery(chicago, "SELECT TIMESTAMP '1960-01-22 3:04:05.123'");
        // TODO #7122 assertQuery(kathmandu, "SELECT TIMESTAMP '1960-01-22 3:04:05.123'");

        assertEquals(computeScalar("SELECT TIMESTAMP '1960-01-22 3:04:05 +06:00'"), ZonedDateTime.of(1960, 1, 22, 3, 4, 5, 0, ZoneOffset.ofHoursMinutes(6, 0)));
    }

    @Override
    public void testLocallyUnrepresentableTimeLiterals()
    {
        LocalDateTime localTimeThatDidNotExist = LocalDateTime.of(2017, 4, 2, 2, 10);
        checkState(ZoneId.systemDefault().getRules().getValidOffsets(localTimeThatDidNotExist).isEmpty(), "This test assumes certain JVM time zone");
        // This tests that both Presto runner and H2 can return TIMESTAMP value that never happened in JVM's zone (e.g. is not representable using java.sql.Timestamp)
        @Language("SQL") String sql = DateTimeFormatter.ofPattern("'SELECT TIMESTAMP '''uuuu-MM-dd HH:mm:ss''").format(localTimeThatDidNotExist);
        assertEquals(computeScalar(sql), localTimeThatDidNotExist); // this tests Presto and the QueryRunner
        assertQuery(sql); // this tests H2QueryRunner

        LocalDate localDateThatDidNotHaveMidnight = LocalDate.of(1970, 1, 1);
        checkState(ZoneId.systemDefault().getRules().getValidOffsets(localDateThatDidNotHaveMidnight.atStartOfDay()).isEmpty(), "This test assumes certain JVM time zone");
        // This tests that both Presto runner and H2 can return DATE value for a day which midnight never happened in JVM's zone (e.g. is not exactly representable using java.sql.Date)
        sql = DateTimeFormatter.ofPattern("'SELECT DATE '''uuuu-MM-dd''").format(localDateThatDidNotHaveMidnight);
        assertEquals(computeScalar(sql), localDateThatDidNotHaveMidnight); // this tests Presto and the QueryRunner
        assertQuery(sql); // this tests H2QueryRunner

        LocalTime localTimeThatDidNotOccurOn19700101 = LocalTime.of(0, 10);
        checkState(ZoneId.systemDefault().getRules().getValidOffsets(localTimeThatDidNotOccurOn19700101.atDate(LocalDate.ofEpochDay(0))).isEmpty(), "This test assumes certain JVM time zone");
        checkState(!Objects.equals(java.sql.Time.valueOf(localTimeThatDidNotOccurOn19700101).toLocalTime(), localTimeThatDidNotOccurOn19700101), "This test assumes certain JVM time zone");
        sql = DateTimeFormatter.ofPattern("'SELECT TIME '''HH:mm:ss''").format(localTimeThatDidNotOccurOn19700101);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails(sql, failureMsg, true);
    }
}
