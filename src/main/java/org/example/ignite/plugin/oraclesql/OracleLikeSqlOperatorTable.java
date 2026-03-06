package org.example.ignite.plugin.oraclesql;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;

public class OracleLikeSqlOperatorTable extends ReflectiveSqlOperatorTable {
    /** Oracle-compatible SUBSTR function. */
    public static final SqlFunction SUBSTR = new SqlFunction(
        "SUBSTR",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_NULLABLE_VARYING,
        null,
        OperandTypes.or(OperandTypes.STRING_INTEGER, OperandTypes.STRING_INTEGER_INTEGER),
        SqlFunctionCategory.STRING
    );

    /**
     * Oracle-compatible SYSTIMESTAMP function.
     *
     * <p>Ignite 2 does not support TIMESTAMP WITH LOCAL TIME ZONE type,
     * therefore this function is exposed as plain TIMESTAMP.</p>
     */
    public static final SqlFunction SYSTIMESTAMP = new SqlFunction(
        "SYSTIMESTAMP",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.TIMESTAMP),
        null,
        OperandTypes.NILADIC,
        SqlFunctionCategory.TIMEDATE
    );

    /**
     * Oracle-compatible SYSDATE function.
     *
     * <p>In Oracle, SYSDATE returns DATE which includes time with second precision.
     * Ignite DATE type does not keep time, so this function is exposed as TIMESTAMP(0)
     * to preserve Oracle observable behavior for time components.</p>
     */
    public static final SqlFunction SYSDATE = new SqlFunction(
        "SYSDATE",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.TIMESTAMP),
        null,
        OperandTypes.NILADIC,
        SqlFunctionCategory.TIMEDATE
    );

    /** Oracle-compatible ADD_MONTHS function. */
    public static final SqlFunction ADD_MONTHS = new SqlFunction(
        "ADD_MONTHS",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.cascade(ReturnTypes.ARG0, SqlTypeTransforms.TO_NULLABLE),
        null,
        OperandTypes.or(
            OperandTypes.family(SqlTypeFamily.DATE, SqlTypeFamily.INTEGER),
            OperandTypes.family(SqlTypeFamily.TIMESTAMP, SqlTypeFamily.INTEGER)
        ),
        SqlFunctionCategory.TIMEDATE
    );

    /** Oracle-compatible REGEXP_COUNT function. */
    public static final SqlFunction REGEXP_COUNT = new SqlFunction(
        "REGEXP_COUNT",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER_NULLABLE,
        null,
        OperandTypes.or(
            OperandTypes.STRING_STRING,
            OperandTypes.STRING_STRING_INTEGER,
            OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER, SqlTypeFamily.CHARACTER)
        ),
        SqlFunctionCategory.STRING
    );
}
