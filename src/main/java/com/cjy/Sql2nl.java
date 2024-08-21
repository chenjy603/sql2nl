package com.cjy;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.util.TablesNamesFinder;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * @author tang
 * @date 2024/8/21 15:37
 */
public class Sql2nl {
    public static void main(String[] args) {
        String sql = "SELECT\n" +
                "  data_dt as '日期', rate_rmb as '人民币存款时点价格' \n" +
                "FROM\n" +
                "  bs_chatbi_data_a03_corp_main_org_index\n" +
                "WHERE\n" +
                "  data_dt between DATE_SUB(date(now()), INTERVAL 30 day)\n" +
                "  and DATE_SUB(date(now()), INTERVAL 0 day)\n" +
                "ORDER BY\n" +
                "  1 ASC;";
        try {
            Select selectStatement = (Select) CCJSqlParserUtil.parse(sql);

            SelectBody selectBody = selectStatement.getSelectBody();
            if (selectBody instanceof PlainSelect plainSelect) {

                // Extract table names
                TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
                List<String> tableNames = tablesNamesFinder.getTableList(selectStatement);
                String dataSources = tableNames.stream().reduce((t1, t2) -> t1 + ", " + t2).orElse("");

                // Extract select columns
                String fields = plainSelect.getSelectItems().stream()
                        .flatMap(item -> {
                            if (item instanceof SelectExpressionItem selectItem) {
                                String fieldName = selectItem.getExpression().toString();
                                String alias = selectItem.getAlias() == null ? fieldName : selectItem.getAlias().getName();
                                return Stream.of(alias);
                            } else {
                                return Stream.of(item.toString());
                            }
                        })
                        .reduce((t1, t2) -> t1 + ", " + t2).orElse("");

                // Extract conditions
                Expression whereExpression = plainSelect.getWhere();
                System.out.println(whereExpression);
                String conditions = parseCondition(whereExpression);
                String stringBuffer = "数据源: " + dataSources + "\n" +
                        "查询字段: " + fields + "\n" +
                        "条件: " + conditions + "\n";
                System.out.println(stringBuffer);
            }
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }
    }

    public static String parseCondition(Expression expression) {
        if (expression instanceof InExpression) {
            return parseInExpression((InExpression) expression);
        } else if (expression instanceof AndExpression) {
            return parseAndExpression((AndExpression) expression);
        } else if (expression instanceof OrExpression) {
            return parseOrExpression((OrExpression) expression);
        } else if (expression instanceof Between) {
            return parseBetweenExpression((Between) expression);
        } else if (expression instanceof ComparisonOperator) {
            return parseComparisonExpression(expression);
        } else {
            // Handle unknown expression types gracefully
            return expression.toString();
        }
    }

    private static String parseBetweenExpression(Between betweenExpr) {
        Expression column = betweenExpr.getLeftExpression();
        Expression startValue = betweenExpr.getBetweenExpressionStart();
        Expression endValue = betweenExpr.getBetweenExpressionEnd();
        if (column instanceof Column && startValue instanceof Function && endValue instanceof Function) {
            String start = extractParameterFromExpression(startValue);
            String end = extractParameterFromExpression(endValue);
            return "字段" + ((Column) column).getColumnName() + ": " + start + "至" + end;
        }


        // if (column instanceof Column && startValue instanceof Function startDateFunc && endValue instanceof Function endDateFunc) {
        //
        //     if ("DATE_SUB".equals(startDateFunc.getName()) && "DATE_SUB".equals(endDateFunc.getName())) {
        //         Expression startSubExpr = startDateFunc.getParameters().getExpressions().get(1);
        //         Expression endSubExpr = endDateFunc.getParameters().getExpressions().get(1);
        //         if (startSubExpr instanceof IntervalExpression startInterval && endSubExpr instanceof IntervalExpression endInterval) {
        //             if ("day".equals(startInterval.getIntervalType())) {
        //                 String startDays = startInterval.getParameter();
        //                 String endDays = endInterval.getParameter();
        //                 if ("0".equals(endDays)) {
        //                     return "字段" + ((Column) column).getColumnName() + ": " + startDays + "天前至今天";
        //                 } else {
        //                     return "字段" + ((Column) column).getColumnName() + ": " + startDays + "天前至" + endDays + "天前";
        //                 }
        //             }
        //         }
        //     }
        // }

        // 如果条件不匹配上述情况，直接返回BETWEEN表达式的字符串表示
        return column.toString() + ": 在 " + startValue.toString() + " 和 " + endValue.toString() + " 之间";
    }

    private static String extractParameterFromExpression(Expression expr) {
        if (expr instanceof Function dateSubFunc && "DATE_SUB".equals(dateSubFunc.getName())) {
            Expression subExpr = dateSubFunc.getParameters().getExpressions().get(1);
            if (subExpr instanceof IntervalExpression intervalExpr) {
                String intervalType = intervalExpr.getIntervalType();
                if ("0".equals(intervalExpr.getParameter())){
                    return "今天";
                }
                if ("day".equals(intervalType)) {
                    return intervalExpr.getParameter() + "天前";
                } else if ("month".equals(intervalType)) {
                    return intervalExpr.getParameter() + "月前";
                } else if ("year".equals(intervalType)) {
                    return intervalExpr.getParameter() + "年前";
                }
                return expr.toString();
            }
        }
        return expr.toString();
    }

    private static String parseComparisonExpression(Expression expression) {
        StringBuilder result = new StringBuilder();
        if (expression instanceof EqualsTo eqExpr) {
            result.append(eqExpr.getLeftExpression().toString());
            result.append(" 为 ");
            result.append(extractParameterFromExpression(eqExpr.getRightExpression()));
        } else if (expression instanceof NotEqualsTo neExpr) {
            result.append(neExpr.getLeftExpression().toString());
            result.append(" 不等于 ");
            result.append(extractParameterFromExpression(neExpr.getRightExpression()));
        } else if (expression instanceof GreaterThan gtExpr) {
            result.append(gtExpr.getLeftExpression().toString());
            result.append(" 大于 ");
            result.append(extractParameterFromExpression(gtExpr.getRightExpression()));
        } else if (expression instanceof MinorThan ltExpr) {
            result.append(ltExpr.getLeftExpression().toString());
            result.append(" 小于 ");
            result.append(extractParameterFromExpression(ltExpr.getRightExpression()));
        } else if (expression instanceof GreaterThanEquals gteExpr) {
            result.append(gteExpr.getLeftExpression().toString());
            result.append(" 大于等于 ");
            result.append(extractParameterFromExpression(gteExpr.getRightExpression()));
        } else if (expression instanceof MinorThanEquals lteExpr) {
            result.append(lteExpr.getLeftExpression().toString());
            result.append(" 小于等于 ");
            result.append(extractParameterFromExpression(lteExpr.getRightExpression()));
        } else {
            // Handle unknown expression types
            result.append(expression.toString());
        }
        return result.toString();
    }

    private static String parseInExpression(InExpression inExpr) {
        Expression column = inExpr.getLeftExpression();
        ItemsList rightItemsList = inExpr.getRightItemsList();
        if (column instanceof Column && rightItemsList instanceof ExpressionList) {
            List<Expression> expressions = ((ExpressionList) rightItemsList).getExpressions();
            if (expressions.size() == 1) {
                return "字段" + ((Column) column).getColumnName() + ": 是 " + expressions.get(0);
            } else {
                return "字段" + ((Column) column).getColumnName() + ": 在 " + expressions + " 之中";
            }
        }
        return inExpr.toString();
    }


    private static String parseAndExpression(AndExpression andExpr) {
        List<String> conditions = new ArrayList<>();
        conditions.add(parseCondition(andExpr.getLeftExpression()));
        conditions.add(parseCondition(andExpr.getRightExpression()));
        return String.join(" 以及 ", conditions);
    }

    private static String parseOrExpression(OrExpression orExpr) {
        List<String> conditions = new ArrayList<>();
        recurseOrExpression(orExpr, conditions);
        return String.join(" 或者 ", conditions);
    }

    private static void recurseOrExpression(OrExpression orExpr, List<String> conditions) {
        conditions.add(parseCondition(orExpr.getLeftExpression()));
        if (orExpr.getRightExpression() instanceof OrExpression) {
            recurseOrExpression((OrExpression) orExpr.getRightExpression(), conditions);
        } else {
            conditions.add(parseCondition(orExpr.getRightExpression()));
        }
    }
}
