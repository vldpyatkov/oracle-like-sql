package org.example.ignite.plugin.oraclesql;

import java.io.Serializable;
import java.util.UUID;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.linq4j.tree.Expressions;
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
                var str = Expressions.convert_(translatedOperands.get(0), String.class);

                if (translatedOperands.size() == 2) {
                    return Expressions.call(
                        SqlFunctions.class,
                        "substr",
                        str,
                        translatedOperands.get(1),
                        Expressions.constant(null, Integer.class)
                    );
                }

                return Expressions.call(
                    SqlFunctions.class,
                    "substr",
                    str,
                    translatedOperands.get(1),
                    translatedOperands.get(2)
                );
            }, NullPolicy.ARG0, false)
        );

        RexImpTable.INSTANCE.define(
            OracleLikeSqlOperatorTable.ADD_MONTHS,
            RexImpTable.createRexCallImplementor((translator, call, translatedOperands) -> {
                var source = translatedOperands.get(0);
                var months = translatedOperands.get(1);

                if (call.getType().getSqlTypeName() == org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP) {
                    return Expressions.call(
                        SqlFunctions.class,
                        "addMonths",
                        Expressions.convert_(source, java.sql.Timestamp.class),
                        months
                    );
                }

                return Expressions.call(
                    SqlFunctions.class,
                    "addMonths",
                    Expressions.convert_(source, java.sql.Date.class),
                    months
                );
            }, NullPolicy.STRICT, false)
        );

        RexImpTable.INSTANCE.define(
            OracleLikeSqlOperatorTable.REGEXP_COUNT,
            RexImpTable.createRexCallImplementor((translator, call, translatedOperands) -> {
                var source = Expressions.convert_(translatedOperands.get(0), String.class);
                var pattern = Expressions.convert_(translatedOperands.get(1), String.class);

                if (translatedOperands.size() == 2) {
                    return Expressions.call(
                        SqlFunctions.class,
                        "regexpCount",
                        source,
                        pattern
                    );
                }

                if (translatedOperands.size() == 3) {
                    return Expressions.call(
                        SqlFunctions.class,
                        "regexpCount",
                        source,
                        pattern,
                        translatedOperands.get(2)
                    );
                }

                return Expressions.call(
                    SqlFunctions.class,
                    "regexpCount",
                    source,
                    pattern,
                    translatedOperands.get(2),
                    translatedOperands.get(3)
                );
            }, NullPolicy.ARG0, false)
        );
        RexImpTable.INSTANCE.define(
            OracleLikeSqlOperatorTable.SYSTIMESTAMP,
            RexImpTable.createRexCallImplementor((translator, call, translatedOperands) ->
                    Expressions.call(SqlFunctions.class, "systimestamp"),
                NullPolicy.NONE,
                false)
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
}
