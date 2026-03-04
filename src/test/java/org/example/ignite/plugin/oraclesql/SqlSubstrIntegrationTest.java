package org.example.ignite.plugin.oraclesql;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SqlSubstrIntegrationTest {
    public static final String DEFAULT_CACHE_NAME = "default";

    private static final Logger LOG = LogManager.getLogger(SqlSubstrIntegrationTest.class);

    private IgniteConfiguration createConfiguration(String nodeName) {
        return new IgniteConfiguration()
            .setIgniteInstanceName(nodeName)
            .setConsistentId(nodeName)
            .setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setSqlSchema("PUBLIC"))
            .setPeerClassLoadingEnabled(false)
            .setConnectorConfiguration(null)
            .setSqlConfiguration(new SqlConfiguration()
                .setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration()))
            .setCommunicationSpi(new TcpCommunicationSpi()
                .setLocalAddress("127.0.0.1")
                .setLocalPort(47100)
                .setLocalPortRange(5))
            .setDiscoverySpi(new TcpDiscoverySpi()
                .setLocalAddress("127.0.0.1")
                .setLocalPort(47500)
                .setLocalPortRange(5)
                .setIpFinder(new TcpDiscoveryVmIpFinder()
                    .setAddresses(List.of(
                        "127.0.0.1:47500",
                        "127.0.0.1:47501",
                        "127.0.0.1:47502",
                        "127.0.0.1:47503",
                        "127.0.0.1:47504"
                    ))))
            .setPluginProviders(new OracleLikeSqlPluginProvider());
    }

    /** Verifies Oracle-compatible SUBSTR behavior. */
    @Test
    public void testSubstr() {
        try (Ignite ignite = Ignition.start(createConfiguration("node-1"))) {
            assertEquals("cdef", queryAndPrint(ignite, "SELECT SUBSTR('abcdef', 3)").get(0).get(0));
            assertEquals("cd", queryAndPrint(ignite, "SELECT SUBSTR('abcdef', 3, 2)").get(0).get(0));
            assertEquals("ef", queryAndPrint(ignite, "SELECT SUBSTR('abcdef', -2)").get(0).get(0));
            assertEquals("abc", queryAndPrint(ignite, "SELECT SUBSTR('abcdef', 0, 3)").get(0).get(0));
            assertEquals("abc", queryAndPrint(ignite, "SELECT SUBSTR('abcdef', -20, 3)").get(0).get(0));
            assertNull(queryAndPrint(ignite, "SELECT SUBSTR('abcdef', 2, 0)").get(0).get(0));
            assertNull(queryAndPrint(ignite, "SELECT SUBSTR(NULL, 2)").get(0).get(0));
        }
    }

    /** Verifies Oracle-compatible REGEXP_COUNT behavior. */
    @Test
    public void testRegexpCount() {
        try (Ignite ignite = Ignition.start(createConfiguration("node-1"))) {
            assertEquals(2, queryAndPrint(ignite, "SELECT REGEXP_COUNT('abcabc', 'a')").get(0).get(0));
            assertEquals(1, queryAndPrint(ignite, "SELECT REGEXP_COUNT('abcabc', 'a', 3)").get(0).get(0));
            assertEquals(3, queryAndPrint(ignite, "SELECT REGEXP_COUNT('AaA', 'a', 1, 'i')").get(0).get(0));
            assertEquals(1, queryAndPrint(ignite, "SELECT REGEXP_COUNT('AaA', 'a', 1, 'ic')").get(0).get(0));
            assertEquals(0, queryAndPrint(ignite, "SELECT REGEXP_COUNT('abc', 'z')").get(0).get(0));
            assertNull(queryAndPrint(ignite, "SELECT REGEXP_COUNT(NULL, 'a')").get(0).get(0));
            assertNull(queryAndPrint(ignite, "SELECT REGEXP_COUNT('abc', NULL)").get(0).get(0));
        }
    }

    /** Verifies Oracle-compatible SYSTIMESTAMP behavior. */
    @Test
    public void testSystimestamp() {
        try (Ignite ignite = Ignition.start(createConfiguration("node-1"))) {
            Object first = queryAndPrint(ignite, "SELECT SYSTIMESTAMP()").get(0).get(0);
            Object second = queryAndPrint(ignite, "SELECT SYSTIMESTAMP()").get(0).get(0);

            assertNotNull(first);
            assertNotNull(second);
            assertTrue(first instanceof Timestamp);
            assertTrue(second instanceof Timestamp);
            assertTrue(!((Timestamp)second).before((Timestamp)first));
        }
    }


    /** Verifies Oracle-compatible ADD_MONTHS behavior. */
    @Test
    public void testAddMonths() {
        try (Ignite ignite = Ignition.start(createConfiguration("node-1"))) {
            assertEquals(Date.valueOf("2024-02-29"), queryAndPrint(ignite,
                "SELECT ADD_MONTHS(DATE '2024-01-31', 1)").get(0).get(0));
            assertEquals(Date.valueOf("2023-02-28"), queryAndPrint(ignite,
                "SELECT ADD_MONTHS(DATE '2023-03-30', -1)").get(0).get(0));
            assertEquals(Date.valueOf("2024-03-15"), queryAndPrint(ignite,
                "SELECT ADD_MONTHS(DATE '2024-01-15', 2)").get(0).get(0));
            assertNull(queryAndPrint(ignite, "SELECT ADD_MONTHS(CAST(NULL AS DATE), 1)").get(0).get(0));

            Object ts = queryAndPrint(ignite,
                "SELECT ADD_MONTHS(TIMESTAMP '2023-01-31 10:15:30', 1)").get(0).get(0);

            assertTrue(ts instanceof Timestamp);
            assertEquals(Timestamp.valueOf("2023-02-28 10:15:30"), ts);
            assertNull(queryAndPrint(ignite, "SELECT ADD_MONTHS(CAST(NULL AS TIMESTAMP), 1)").get(0).get(0));
        }
    }

    /** */
    private List<List<?>> queryAndPrint(Ignite ign, String sql, Object... args) {
        List<List<?>> result = ign.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(sql).setArgs(args)).getAll();

        printResult(sql, result);

        return result;
    }

    /**
     * Prints query results in readable format.
     *
     * @param sql SQL query.
     * @param result Query result.
     */
    private void printResult(String sql, List<List<?>> result) {
        LOG.info("Executing query: " + sql);

        if (sql.trim().toUpperCase().startsWith("INSERT") ||
            sql.trim().toUpperCase().startsWith("UPDATE") ||
            sql.trim().toUpperCase().startsWith("DELETE") ||
            sql.trim().toUpperCase().startsWith("CREATE") ||
            sql.trim().toUpperCase().startsWith("DROP")) {
            LOG.info("Command executed successfully");
            if (!result.isEmpty()) {
                LOG.info("Rows affected: " + result.get(0).get(0));
            }
            return;
        }

        if (result.isEmpty()) {
            LOG.info("No results");
            return;
        }

        // Print header
        StringBuilder header = new StringBuilder();
        header.append("\nResults (").append(result.size()).append(" rows):\n");

        // Create separator
        StringBuilder separator = new StringBuilder();
        for (int i = 0; i < 80; i++) {
            separator.append("-");
        }

        LOG.info(header.toString());
        LOG.info(separator.toString());

        // Print rows
        int rowNum = 1;
        for (List<?> row : result) {
            StringBuilder rowBuilder = new StringBuilder();
            rowBuilder.append("Row ").append(rowNum++).append(": ");

            for (int i = 0; i < row.size(); i++) {
                if (i > 0) {
                    rowBuilder.append(" | ");
                }

                Object value = row.get(i);
                if (value == null) {
                    rowBuilder.append("NULL");
                } else {
                    // Format the value based on type
                    if (value instanceof String) {
                        rowBuilder.append("'").append(value).append("'");
                    } else if (value instanceof Long || value instanceof Integer) {
                        rowBuilder.append(value);
                    } else {
                        rowBuilder.append(value.toString());
                    }
                }
            }

            LOG.info(rowBuilder.toString());
        }

        LOG.info(separator.toString());
    }
}
