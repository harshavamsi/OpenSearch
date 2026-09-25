/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.TimestampString;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.util.Locale;

/**
 * Folds PPL {@code TIMESTAMP('yyyy-MM-dd[ HH:mm:ss[.fff]]')} calls over a string literal into a
 * {@code TIMESTAMP} {@link RexLiteral}.
 *
 * <p>PPL lowers {@code where ts >= '2013-07-01 00:00:00'} to {@code >=($ts, TIMESTAMP('...':VARCHAR))}.
 * Calcite's reduce-expressions phase does not evaluate the PPL UDF, so the filter rule sees a scalar
 * call and requires a backend with scalar-function support; on a Lucene-only index there is none and
 * the query fails. As a literal the predicate is a plain field-vs-constant range that every backend,
 * including the Lucene filter path, handles. Strings that do not parse are left untouched.
 */
public final class OpenSearchTimestampLiteralFolder {

    private OpenSearchTimestampLiteralFolder() {}

    public static RelNode rewrite(RelNode root) {
        RexBuilder rexBuilder = root.getCluster().getRexBuilder();
        RexShuttle rexShuttle = new RexShuttle() {
            @Override
            public RexNode visitCall(RexCall call) {
                RexNode visited = super.visitCall(call);
                if (visited instanceof RexCall c) {
                    RexNode folded = fold(c, rexBuilder);
                    if (folded != null) {
                        return folded;
                    }
                }
                return visited;
            }
        };
        return root.accept(new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(RelNode node) {
                return super.visit(node).accept(rexShuttle);
            }
        });
    }

    static RexNode fold(RexCall call, RexBuilder rexBuilder) {
        SqlTypeName resultType = call.getType().getSqlTypeName();
        boolean timestampCall = (resultType == SqlTypeName.TIMESTAMP || resultType == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
            && call.getOperands().size() == 1
            && "TIMESTAMP".equals(call.getOperator().getName().toUpperCase(Locale.ROOT));
        if (timestampCall == false) {
            return null;
        }
        RexNode operand = call.getOperands().getFirst();
        if (operand instanceof RexLiteral literal
            && literal.isNull() == false
            && literal.getType().getSqlTypeName().getFamily() == SqlTypeFamily.CHARACTER) {
            TimestampString ts = parse(literal.getValueAs(String.class));
            return ts == null ? null : rexBuilder.makeLiteral(ts, call.getType(), false);
        }
        return null;
    }

    /**
     * Accepts {@code yyyy-MM-dd}, {@code yyyy-MM-dd HH:mm:ss[.fffffffff]} and the same with a {@code T}
     * separator, with strict field validation (no month 13, no second 61) and only inside the
     * i64-nanosecond range Arrow/DataFusion can represent. Anything else returns {@code null} and is
     * left unfolded, so the DataFusion {@code TimestampFunctionAdapter} rejects it with its normal
     * plan-time message instead of the fold producing a literal that fails at execution.
     */
    static TimestampString parse(String value) {
        if (value == null) {
            return null;
        }
        String s = value.trim().replace('T', ' ');
        if (s.length() == 10) {
            s = s + " 00:00:00";
        }
        LocalDateTime ldt;
        try {
            ldt = LocalDateTime.parse(s, STRICT_FORMAT);
        } catch (DateTimeParseException e) {
            return null;
        }
        if (ldt.isAfter(I64_NS_MAX) || ldt.isBefore(I64_NS_MIN)) {
            return null;
        }
        TimestampString ts = new TimestampString(
            ldt.getYear(),
            ldt.getMonthValue(),
            ldt.getDayOfMonth(),
            ldt.getHour(),
            ldt.getMinute(),
            ldt.getSecond()
        );
        return ldt.getNano() == 0 ? ts : ts.withNanos(ldt.getNano());
    }

    private static final DateTimeFormatter STRICT_FORMAT = new DateTimeFormatterBuilder().appendPattern("uuuu-MM-dd HH:mm:ss")
        .optionalStart()
        .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
        .optionalEnd()
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    /** Exact i64-ns ends, matching the DataFusion adapter's accepted range. */
    private static final LocalDateTime I64_NS_MAX = LocalDateTime.ofEpochSecond(
        Long.MAX_VALUE / 1_000_000_000L,
        (int) (Long.MAX_VALUE % 1_000_000_000L),
        ZoneOffset.UTC
    );
    private static final LocalDateTime I64_NS_MIN = LocalDateTime.ofEpochSecond(
        Math.floorDiv(Long.MIN_VALUE, 1_000_000_000L),
        (int) Math.floorMod(Long.MIN_VALUE, 1_000_000_000L),
        ZoneOffset.UTC
    );
}
