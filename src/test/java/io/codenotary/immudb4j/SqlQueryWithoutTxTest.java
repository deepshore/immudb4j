/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package io.codenotary.immudb4j;

import io.codenotary.immudb4j.exceptions.VerificationException;
import io.codenotary.immudb4j.sql.SQLException;
import io.codenotary.immudb4j.sql.SQLQueryResult;
import io.codenotary.immudb4j.sql.SQLValue;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

public class SqlQueryWithoutTxTest extends ImmuClientIntegrationTest {

    @Test(testName = "sqlQueryWithoutTx - simple query without parameters")
    public void t1() throws VerificationException, InterruptedException, SQLException {
        immuClient.openSession("defaultdb", "immudb", "immudb");

        try {
            // Create table and insert data
            immuClient.beginTransaction();
            immuClient.sqlExec(
                    "CREATE TABLE test_simple(id INTEGER, title VARCHAR[256], PRIMARY KEY id)");
            
            for (int i = 0; i < 5; i++) {
                immuClient.sqlExec("INSERT INTO test_simple(id, title) VALUES (?, ?)",
                        new SQLValue(i),
                        new SQLValue(String.format("title%d", i)));
            }
            immuClient.commitTransaction();

            // Query without transaction using sqlQueryWithoutTx
            SQLQueryResult res = immuClient.sqlQueryWithoutTx("SELECT id, title FROM test_simple ORDER BY id");

            Assert.assertEquals(res.getColumnsCount(), 2);
            Assert.assertEquals(res.getColumnName(0), "id");
            Assert.assertEquals(res.getColumnType(0), "INTEGER");
            Assert.assertEquals(res.getColumnName(1), "title");
            Assert.assertEquals(res.getColumnType(1), "VARCHAR");

            int count = 0;
            while (res.next()) {
                Assert.assertEquals(count, res.getInt(0), "Row " + count + " id mismatch");
                Assert.assertEquals(String.format("title%d", count), res.getString(1), 
                        "Row " + count + " title mismatch");
                count++;
            }

            Assert.assertEquals(5, count, "Expected exactly 5 rows");
            res.close();

        } finally {
            // Clean up: drop table after test
            try {
                immuClient.beginTransaction();
                try {
                    immuClient.sqlExec("DROP TABLE test_simple");
                } catch (Exception e) {
                    // Ignore if table doesn't exist
                }
                immuClient.commitTransaction();
            } catch (Exception e) {
                // Ignore cleanup errors
            }
            immuClient.closeSession();
        }
    }

    @Test(testName = "sqlQueryWithoutTx - query with varargs parameters")
    public void t2() throws VerificationException, InterruptedException, SQLException {
        immuClient.openSession("defaultdb", "immudb", "immudb");

        try {
            // Create table and insert data
            immuClient.beginTransaction();
            immuClient.sqlExec(
                    "CREATE TABLE test_varargs(id INTEGER, value INTEGER, PRIMARY KEY id)");
            
            for (int i = 0; i < 10; i++) {
                immuClient.sqlExec("INSERT INTO test_varargs(id, value) VALUES (?, ?)",
                        new SQLValue(i),
                        new SQLValue(i * 10));
            }
            immuClient.commitTransaction();

            // Query with parameters using sqlQueryWithoutTx
            SQLQueryResult res = immuClient.sqlQueryWithoutTx(
                    "SELECT id, value FROM test_varargs WHERE id >= ? AND id < ? ORDER BY id",
                    new SQLValue(3),
                    new SQLValue(7));

            int count = 0;
            int expectedId = 3;
            while (res.next()) {
                int actualId = res.getInt(0);
                int actualValue = res.getInt(1);
                
                Assert.assertEquals(expectedId, actualId, "Row " + count + " id mismatch");
                Assert.assertEquals(expectedId * 10, actualValue, "Row " + count + " value mismatch");
                
                expectedId++;
                count++;
            }

            Assert.assertEquals(4, count, "Expected exactly 4 rows (id 3,4,5,6)");
            res.close();

        } finally {
            // Clean up: drop table after test
            try {
                immuClient.beginTransaction();
                try {
                    immuClient.sqlExec("DROP TABLE test_varargs");
                } catch (Exception e) {
                    // Ignore if table doesn't exist
                }
                immuClient.commitTransaction();
            } catch (Exception e) {
                // Ignore cleanup errors
            }
            immuClient.closeSession();
        }
    }

    @Test(testName = "sqlQueryWithoutTx - query with named parameters")
    public void t3() throws VerificationException, InterruptedException, SQLException {
        immuClient.openSession("defaultdb", "immudb", "immudb");

        try {
            // Create table and insert data
            immuClient.beginTransaction();
            immuClient.sqlExec(
                    "CREATE TABLE test_named(id INTEGER, name VARCHAR[256], PRIMARY KEY id)");
            
            for (int i = 0; i < 10; i++) {
                immuClient.sqlExec("INSERT INTO test_named(id, name) VALUES (?, ?)",
                        new SQLValue(i),
                        new SQLValue(String.format("name%d", i)));
            }
            immuClient.commitTransaction();

            // Query with named parameters using sqlQueryWithoutTx
            Map<String, SQLValue> params = new HashMap<>();
            params.put("minId", new SQLValue(5));
            params.put("maxId", new SQLValue(8));

            SQLQueryResult res = immuClient.sqlQueryWithoutTx(
                    "SELECT id, name FROM test_named WHERE id >= @minId AND id <= @maxId ORDER BY id",
                    params);

            int count = 0;
            int expectedId = 5;
            while (res.next()) {
                int actualId = res.getInt(0);
                String actualName = res.getString(1);
                
                Assert.assertEquals(expectedId, actualId, "Row " + count + " id mismatch");
                Assert.assertEquals(String.format("name%d", expectedId), actualName, 
                        "Row " + count + " name mismatch");
                
                expectedId++;
                count++;
            }

            Assert.assertEquals(4, count, "Expected exactly 4 rows (id 5,6,7,8)");
            res.close();

        } finally {
            // Clean up: drop table after test
            try {
                immuClient.beginTransaction();
                try {
                    immuClient.sqlExec("DROP TABLE test_named");
                } catch (Exception e) {
                    // Ignore if table doesn't exist
                }
                immuClient.commitTransaction();
            } catch (Exception e) {
                // Ignore cleanup errors
            }
            immuClient.closeSession();
        }
    }

    @Test(testName = "sqlQueryWithoutTx - HISTORY OF query returns all versions")
    public void t4() throws VerificationException, InterruptedException, SQLException {
        immuClient.openSession("defaultdb", "immudb", "immudb");

        try {
            // Create table
            immuClient.beginTransaction();
            immuClient.sqlExec(
                    "CREATE TABLE test_history(id INTEGER, data VARCHAR[256], PRIMARY KEY id)");
            
            // Insert 3 rows (version 1)
            for (int i = 1; i <= 3; i++) {
                immuClient.sqlExec("INSERT INTO test_history(id, data) VALUES (?, ?)",
                        new SQLValue(i),
                        new SQLValue(String.format("version1_row%d", i)));
            }
            immuClient.commitTransaction();

            // Update each row to create version 2
            immuClient.beginTransaction();
            for (int i = 1; i <= 3; i++) {
                immuClient.sqlExec("UPDATE test_history SET data = ? WHERE id = ?",
                        new SQLValue(String.format("version2_row%d", i)),
                        new SQLValue(i));
            }
            immuClient.commitTransaction();

            // Update each row to create version 3
            immuClient.beginTransaction();
            for (int i = 1; i <= 3; i++) {
                immuClient.sqlExec("UPDATE test_history SET data = ? WHERE id = ?",
                        new SQLValue(String.format("version3_row%d", i)),
                        new SQLValue(i));
            }
            immuClient.commitTransaction();

            // Test 1: Query only original versions (_rev = 1)
            SQLQueryResult res = immuClient.sqlQueryWithoutTx(
                    "SELECT id, data, _rev FROM (HISTORY OF test_history) WHERE _rev = 1 ORDER BY id");

            int count = 0;
            int expectedId = 1;
            while (res.next()) {
                int actualId = res.getInt(0);
                String actualData = res.getString(1);
                int actualRev = res.getInt(2);
                
                Assert.assertEquals(expectedId, actualId, "Original version row " + count + " id mismatch");
                Assert.assertEquals(String.format("version1_row%d", expectedId), actualData, 
                        "Original version row " + count + " data mismatch");
                Assert.assertEquals(1, actualRev, "Expected _rev = 1 for original version");
                
                expectedId++;
                count++;
            }

            Assert.assertEquals(3, count, "Expected exactly 3 original versions");
            res.close();

            // Test 2: Query all versions (should be 9 total: 3 rows × 3 versions)
            res = immuClient.sqlQueryWithoutTx(
                    "SELECT id, data, _rev FROM (HISTORY OF test_history) ORDER BY id, _rev");

            count = 0;
            int[] revCounts = new int[4]; // Count versions per _rev (index 0 unused, 1-3 used)
            int[] idCounts = new int[4];  // Count versions per id (index 0 unused, 1-3 used)
            
            while (res.next()) {
                int actualId = res.getInt(0);
                String actualData = res.getString(1);
                int actualRev = res.getInt(2);
                
                // Verify data matches expected version
                String expectedData = String.format("version%d_row%d", actualRev, actualId);
                Assert.assertEquals(expectedData, actualData, 
                        "Version " + actualRev + " row " + actualId + " data mismatch");
                
                // Verify _rev is valid (1, 2, or 3)
                Assert.assertTrue(actualRev >= 1 && actualRev <= 3, 
                        "Invalid _rev value: " + actualRev);
                
                // Verify id is valid (1, 2, or 3)
                Assert.assertTrue(actualId >= 1 && actualId <= 3, 
                        "Invalid id value: " + actualId);
                
                revCounts[actualRev]++;
                idCounts[actualId]++;
                count++;
            }

            Assert.assertEquals(9, count, "Expected exactly 9 total versions (3 rows × 3 versions)");
            Assert.assertEquals(3, revCounts[1], "Expected 3 rows with _rev = 1");
            Assert.assertEquals(3, revCounts[2], "Expected 3 rows with _rev = 2");
            Assert.assertEquals(3, revCounts[3], "Expected 3 rows with _rev = 3");
            Assert.assertEquals(3, idCounts[1], "Expected exactly 3 versions for id = 1");
            Assert.assertEquals(3, idCounts[2], "Expected exactly 3 versions for id = 2");
            Assert.assertEquals(3, idCounts[3], "Expected exactly 3 versions for id = 3");
            res.close();

        } finally {
            // Clean up: drop table after test
            try {
                immuClient.beginTransaction();
                try {
                    immuClient.sqlExec("DROP TABLE test_history");
                } catch (Exception e) {
                    // Ignore if table doesn't exist
                }
                immuClient.commitTransaction();
            } catch (Exception e) {
                // Ignore cleanup errors
            }
            immuClient.closeSession();
        }
    }

    @Test(testName = "sqlQueryWithoutTx - works without active transaction")
    public void t5() throws VerificationException, InterruptedException, SQLException {
        immuClient.openSession("defaultdb", "immudb", "immudb");

        try {
            // Create table and insert data
            immuClient.beginTransaction();
            immuClient.sqlExec(
                    "CREATE TABLE test_notx(id INTEGER, value INTEGER, PRIMARY KEY id)");
            
            for (int i = 0; i < 5; i++) {
                immuClient.sqlExec("INSERT INTO test_notx(id, value) VALUES (?, ?)",
                        new SQLValue(i),
                        new SQLValue(i * 100));
            }
            immuClient.commitTransaction();

            // Test 1: sqlQueryWithoutTx() works WITHOUT starting a transaction
            SQLQueryResult res = immuClient.sqlQueryWithoutTx("SELECT id, value FROM test_notx ORDER BY id");

            int count = 0;
            while (res.next()) {
                int actualId = res.getInt(0);
                int actualValue = res.getInt(1);
                
                Assert.assertEquals(count, actualId, "Row " + count + " id mismatch");
                Assert.assertEquals(count * 100, actualValue, "Row " + count + " value mismatch");
                
                count++;
            }

            Assert.assertEquals(5, count, "Expected exactly 5 rows");
            res.close();

            // Test 2: sqlQuery() requires an active transaction
            boolean exceptionThrown = false;
            try {
                // This should fail because no transaction is active
                res = immuClient.sqlQuery("SELECT id, value FROM test_notx");
                res.close();
            } catch (IllegalStateException e) {
                // Expected: sqlQuery() requires beginTransaction() first
                exceptionThrown = true;
                Assert.assertTrue(e.getMessage().contains("transaction"), 
                        "Expected exception about missing transaction");
            }

            Assert.assertTrue(exceptionThrown, 
                    "sqlQuery() should throw exception when called without active transaction");

        } finally {
            // Clean up: drop table after test
            try {
                immuClient.beginTransaction();
                try {
                    immuClient.sqlExec("DROP TABLE test_notx");
                } catch (Exception e) {
                    // Ignore if table doesn't exist
                }
                immuClient.commitTransaction();
            } catch (Exception e) {
                // Ignore cleanup errors
            }
            immuClient.closeSession();
        }
    }

    @Test(testName = "sqlQueryWithoutTx - error handling without open session")
    public void t6() {
        // Ensure no session is open
        try {
            immuClient.closeSession();
        } catch (Exception e) {
            // Ignore if already closed
        }

        // Test: sqlQueryWithoutTx() should throw exception when no session is open
        boolean exceptionThrown = false;
        try {
            immuClient.sqlQueryWithoutTx("SELECT 1");
        } catch (IllegalStateException e) {
            // Expected: no open session
            exceptionThrown = true;
            Assert.assertTrue(e.getMessage().contains("session"), 
                    "Expected exception about missing session");
        } catch (Exception e) {
            // Also acceptable if it throws a different exception
            exceptionThrown = true;
        }

        Assert.assertTrue(exceptionThrown, 
                "sqlQueryWithoutTx() should throw exception when called without open session");
    }

    @Test(testName = "sqlQueryWithoutTx - query on empty table returns zero rows")
    public void t7() throws VerificationException, InterruptedException, SQLException {
        immuClient.openSession("defaultdb", "immudb", "immudb");

        try {
            // Create table but don't insert any data
            immuClient.beginTransaction();
            immuClient.sqlExec(
                    "CREATE TABLE test_empty(id INTEGER, data VARCHAR[256], PRIMARY KEY id)");
            immuClient.commitTransaction();

            // Query empty table using sqlQueryWithoutTx
            SQLQueryResult res = immuClient.sqlQueryWithoutTx("SELECT id, data FROM test_empty");

            Assert.assertEquals(res.getColumnsCount(), 2);
            Assert.assertEquals(res.getColumnName(0), "id");
            Assert.assertEquals(res.getColumnName(1), "data");

            int count = 0;
            while (res.next()) {
                count++;
            }

            Assert.assertEquals(0, count, "Expected zero rows from empty table");
            res.close();

            // Also test HISTORY OF on empty table
            res = immuClient.sqlQueryWithoutTx("SELECT id, data FROM (HISTORY OF test_empty)");

            count = 0;
            while (res.next()) {
                count++;
            }

            Assert.assertEquals(0, count, "Expected zero rows from HISTORY OF empty table");
            res.close();

        } finally {
            // Clean up: drop table after test
            try {
                immuClient.beginTransaction();
                try {
                    immuClient.sqlExec("DROP TABLE test_empty");
                } catch (Exception e) {
                    // Ignore if table doesn't exist
                }
                immuClient.commitTransaction();
            } catch (Exception e) {
                // Ignore cleanup errors
            }
            immuClient.closeSession();
        }
    }

}
