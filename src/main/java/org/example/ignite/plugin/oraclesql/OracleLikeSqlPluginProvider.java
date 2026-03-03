package org.example.ignite.plugin.oraclesql;

import java.io.Serializable;
import java.util.UUID;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RexImpTable;
import org.apache.ignite.plugin.CachePluginContext;
import org.apache.ignite.plugin.CachePluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginConfiguration;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.PluginValidationException;
import org.jetbrains.annotations.Nullable;

public class OracleLikeSqlPluginProvider implements PluginProvider<PluginConfiguration> {
    /** Extended operator table. */
    private static final OracleLikeSqlOperatorTable OPERATOR_TABLE = new OracleLikeSqlOperatorTable();

    /** Empty plugin instance required by Ignite plugin processor. */
    private static final IgnitePlugin PLUGIN = new IgnitePlugin() {
        // No-op.
    };

    /** {@inheritDoc} */
    @Override public String name() {
        return "oracle-like-sql";
    }

    /** {@inheritDoc} */
    @Override public String version() {
        return "1.0.0";
    }

    /** {@inheritDoc} */
    @Override public String copyright() {
        return "Example";
    }

    /** {@inheritDoc} */
    @Override public <T extends IgnitePlugin> T plugin() {
        return (T)PLUGIN;
    }

    /** {@inheritDoc} */
    @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> T createComponent(PluginContext ctx, Class<T> cls) {
        if (FrameworkConfig.class.equals(cls)) {
            FrameworkConfig cfg = Frameworks.newConfigBuilder(CalciteQueryProcessor.FRAMEWORK_CONFIG)
                .operatorTable(SqlOperatorTables.chain(
                    OPERATOR_TABLE.init(),
                    CalciteQueryProcessor.FRAMEWORK_CONFIG.getOperatorTable()
                ))
                .build();

            return (T)cfg;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public CachePluginProvider<?> createCacheProvider(CachePluginContext ctx) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void start(PluginContext ctx) throws IgniteCheckedException {
        RexImpTable.INSTANCE.define(
            OracleLikeSqlOperatorTable.SUBSTR,
            RexImpTable.createRexCallImplementor((translator, call, translatedOperands) -> {
                if (translatedOperands.size() == 2) {
                    return Expressions.call(
                        OracleLikeSqlPluginProvider.class,
                        "substr",
                        translatedOperands.get(0),
                        translatedOperands.get(1),
                        Expressions.constant(null, Integer.class)
                    );
                }

                return Expressions.call(
                    OracleLikeSqlPluginProvider.class,
                    "substr",
                    translatedOperands.get(0),
                    translatedOperands.get(1),
                    translatedOperands.get(2)
                );
            }, NullPolicy.ARG0, false)
        );
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onIgniteStart() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onIgniteStop(boolean cancel) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Nullable @Override public Serializable provideDiscoveryData(UUID nodeId) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void receiveDiscoveryData(UUID nodeId, Serializable data) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Deprecated
    @Override public void validateNewNode(ClusterNode node) throws PluginValidationException {
        // No-op.
    }

    /**
     * Oracle-compatible SUBSTR overload for primitive {@code int} length.
     *
     * @param str Source string.
     * @param pos Start position (1-based, negative values count from the end).
     * @param len Length.
     */
    public static String substr(String str, int pos, int len) {
        return substr(str, pos, Integer.valueOf(len));
    }

    /**
     * Oracle-compatible SUBSTR implementation.
     *
     * @param str Source string.
     * @param pos Start position (1-based, negative values count from the end).
     * @param len Optional length.
     * @return Extracted substring or {@code null}.
     */
    public static String substr(String str, int pos, @Nullable Integer len) {
        if (str == null)
            return null;

        if (len != null && len < 1)
            return null;

        int strLen = str.length();

        int startPos = pos > 0 ? pos - 1 : strLen + pos;

        if (pos == 0)
            startPos = 0;

        if (startPos < 0)
            startPos = 0;

        if (startPos >= strLen)
            return "";

        int endPos = len == null ? strLen : Math.min(strLen, startPos + len);

        if (endPos <= startPos)
            return null;

        return str.substring(startPos, endPos);
    }

    /** Operator table with Oracle-like functions. */
    public static class OracleLikeSqlOperatorTable extends ReflectiveSqlOperatorTable {
        /** Oracle-compatible SUBSTR function. */
        public static final SqlFunction SUBSTR = new SqlFunction(
            "SUBSTR",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.ARG0_NULLABLE_VARYING,
            null,
            OperandTypes.or(OperandTypes.STRING_INTEGER, OperandTypes.STRING_INTEGER_INTEGER),
            SqlFunctionCategory.STRING
        );
    }
}
