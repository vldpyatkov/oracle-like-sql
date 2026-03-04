package org.example.ignite.plugin.oraclesql;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;

public class OracleLikeSqlOperatorTable  extends ReflectiveSqlOperatorTable {
    /** Oracle-compatible SUBSTR function. */
    public static final SqlFunction SUBSTR = new SqlFunction(
        "SUBSTR",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_NULLABLE_VARYING,
        null,
        OperandTypes.or(OperandTypes.STRING_INTEGER, OperandTypes.STRING_INTEGER_INTEGER),
        SqlFunctionCategory.STRING
    );

    /** Oracle-compatible SYSTIMESTAMP function. */
    public static final SqlFunction SYSTIMESTAMP = new SqlFunction(
        "SYSTIMESTAMP",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE),
        null,
        OperandTypes.NILADIC,
        SqlFunctionCategory.TIMEDATE
    );
}
