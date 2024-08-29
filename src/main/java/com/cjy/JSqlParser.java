package com.cjy;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author tang
 * @date 2024/8/29 9:30
 */
public class JSqlParser {
    public static void main(String[] args) {
        try {
            String sql = """
                    SELECT
                    org_name as '机构名称',
                    valid_cust_cnt as '有效客户数'
                    FROM
                    bs_chatbi_data_a03_corp_main_org_index
                    WHERE
                    data_dt between DATE_SUB(date(now()),INTERVAL 1 day)
                    and DATE_SUB(date(now()),INTERVAL 7 day)
                    AND org_name = 'xx股份有限公司'
                    or org_name = 'xx股份有限公司'
                    GROUP BY 1,2;""";
            Statement statement = CCJSqlParserUtil.parse(sql);
            Select select = (Select) statement;
            PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
            Expression where = plainSelect.getWhere();
            Set<String> columnNames = new HashSet<>();
            extractColumnNamesFromExpression(where, columnNames);
            System.out.println(columnNames);
            String type = "0";
            String column = null;
            if (columnNames.contains("org_name")) {
                type = "2";
            }
            String sql2 = getSelect(sql, column, type);
            System.out.println(sql2);
            // GroupByElement groupBy = plainSelect.getGroupBy();
            // System.out.println("groupBy: " + groupBy);
            // List<SelectItem> selectItems = plainSelect.getSelectItems();
            // System.out.println("selectItems:" + selectItems);
            // List<String> selectItemsS = selectItems.stream()
            //         .filter(item -> item instanceof SelectExpressionItem)
            //         .map(item -> {
            //             SelectExpressionItem selectExpressionItem = (SelectExpressionItem) item;
            //             Expression expression = selectExpressionItem.getExpression();
            //             if (expression instanceof Column) {
            //                 return ((Column) expression).getColumnName();
            //             } else {
            //                 return null;
            //             }
            //         })
            //         .filter(Objects::nonNull)
            //         .collect(Collectors.toList());
            // System.out.println("selectItemsS:  " + selectItemsS);
            //
            // Expression where = plainSelect.getWhere();
            // System.out.println("where:" + where);
            // Set<String> columnNames = new HashSet<>();
            // extractColumnNamesFromExpression(where, columnNames);
            // System.out.println(columnNames);
            // ExpressionList groupByExpressionList = groupBy.getGroupByExpressionList();
            // System.out.println(groupByExpressionList.toString());
            // if (groupByExpressionList.getExpressions().contains("a")) {
            //     System.out.println("1111");
            // }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String getSelect(String sql, String column, String type) throws JSQLParserException {
        Statement statement = CCJSqlParserUtil.parse(sql);
        Select select = (Select) statement;
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
        List<SelectItem> selectItems = plainSelect.getSelectItems();
        List<String> selectItemsString = selectItems.stream()
                .filter(item -> item instanceof SelectExpressionItem)
                .map(item -> {
                    SelectExpressionItem selectExpressionItem = (SelectExpressionItem) item;
                    Expression expression = selectExpressionItem.getExpression();
                    if (expression instanceof Column) {
                        return ((Column) expression).getColumnName();
                    } else {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        switch (type) {
            case "1":
                // 插入一个新的字段
                Expression newField = new Column(column);
                SelectItem newSelectItem = new SelectExpressionItem(newField);
                selectItems.add(newSelectItem);
                break;
            case "2":
                if (!selectItemsString.contains("org_name")) {
                    Expression newField2 = new Column("org_name AS '机构名称'");
                    SelectItem newSelectItem2 = new SelectExpressionItem(newField2);
                    selectItems.add(newSelectItem2);
                }
                if (!selectItemsString.contains("sup_org_name")) {
                    Expression newField2 = new Column("sup_org_name AS '上级机构名称'");
                    SelectItem newSelectItem2 = new SelectExpressionItem(newField2);
                    selectItems.add(newSelectItem2);
                }
                Expression where = plainSelect.getWhere();
                where.accept(new ExpressionVisitorAdapter() {
                    @Override
                    public void visit(EqualsTo expr) {
                        if (expr.getLeftExpression() instanceof Column column) {
                            if ("org_name".equals(column.getColumnName())) {
                                column.setColumnName("sup_org_name");
                            }
                        }
                    }
                });
                plainSelect.setWhere(where);
                GroupByElement groupBy = plainSelect.getGroupBy();
                if (null != groupBy) {
                    ExpressionList groupByExpressionList = groupBy.getGroupByExpressionList();
                    List<Expression> expressions = groupByExpressionList.getExpressions();
                    List<String> groupByColumn = new ArrayList<>();
                    List<Integer> collect = expressions.stream()
                            .map(a -> {
                                try {
                                    return Integer.parseInt(a.toString());
                                } catch (Exception e) {
                                    groupByColumn.add(a.toString());
                                    return null;
                                }
                            })
                            .collect(Collectors.toList());
                    groupByColumn.addAll(collect.stream()
                            .map(a -> selectItemsString.get(a - 1))
                            .collect(Collectors.toList()));
                    if (!groupByColumn.contains("org_name")) {
                        expressions.add(new Column("org_name"));
                    }
                    if (!groupByColumn.contains("sup_org_name")) {
                        expressions.add(new Column("sup_org_name"));
                    }
                    groupByExpressionList.setExpressions(expressions);
                    groupBy.setGroupByExpressionList(groupByExpressionList);
                    plainSelect.setGroupByElement(groupBy);
                }
                break;
            default:
                break;
        }
        return select.toString();
    }

    private static void extractColumnNamesFromExpression(Expression expression, Set<String> columnNames) {
        if (expression instanceof Column) {
            columnNames.add(((Column) expression).getColumnName());
        } else if (expression instanceof BinaryExpression) {
            BinaryExpression binaryExpression = (BinaryExpression) expression;
            extractColumnNamesFromExpression(binaryExpression.getLeftExpression(), columnNames);
            extractColumnNamesFromExpression(binaryExpression.getRightExpression(), columnNames);
        }
    }
}
