package com.code.service.impl;

import com.code.model.CodeVo;
import com.code.service.SqlParseService;
import com.code.utils.FreemarkerUtils;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class SqlParseServiceImpl implements SqlParseService {

    @Autowired
    FreemarkerUtils freemarkerUtils;

    @Override
    public CodeVo parseCreateSql(String sql) throws JSQLParserException {
        CodeVo vo = new CodeVo();
        Map<String, Object> data = getJsqlMapData(sql);
        List<String> templateFileNameList = new ArrayList<>(Arrays.asList("entityTemplate.ftl", "dtoTemplate.ftl",
                "mapperTemplate.ftl", "serviceTemplate.ftl", "serviceImplTemplate.ftl", "repositoryTemplate.ftl"));
        int length = "Template.ftl".length();
        Map<String, String> freemarkerResultMap = new HashMap<>();

        for (String templateFileName : templateFileNameList) {
            String templateName = templateFileName.substring(0, templateFileName.length() - length);
            String freemarkerResult = FreemarkerUtils.process(templateFileName,data);
            freemarkerResultMap.put(templateName, freemarkerResult);
        }
        vo.setEntity(freemarkerResultMap.get("entity"));
        vo.setDto(freemarkerResultMap.get("dto"));
        vo.setMapper(freemarkerResultMap.get("mapper"));
        vo.setService(freemarkerResultMap.get("service"));
        vo.setServiceImpl(freemarkerResultMap.get("serviceImpl"));
        vo.setRepository(freemarkerResultMap.get("repository"));
        return vo;
    }

//    private String getFreemarkerResult(Map<String, Object> data, Configuration freemarkerConfig, String TemplateFileName) {
//        StringWriter writer = new StringWriter();
//        Template template = freemarkerConfig.getTemplate(TemplateFileName);
//        template.process(data, writer);
//        return writer.toString();
//    }

    private Map<String, Object> getJsqlMapData(String sql) throws JSQLParserException {
        CreateTable createTable = (CreateTable) CCJSqlParserUtil.parse(sql);
        Map<String, Object> data = new HashMap<>();
        data.put("tableName", createTable.getTable().getName().replaceAll("`", ""));
        List<Map<String, String>> columns = new ArrayList<>();
        for (ColumnDefinition column : createTable.getColumnDefinitions()) {
            Map<String, String> columnData = new HashMap<>();
            String columnName = column.getColumnName().replaceAll("`", "");
            String colDataType = column.getColDataType().toString().toLowerCase();
            String javaType = mapSqlTypeToJavaType(colDataType);
            columnData.put("name", columnName);
            columnData.put("javaType", javaType);
            columns.add(columnData);
        }
        data.put("columns", columns);
        return data;
    }

    public String mapSqlTypeToJavaType(String sqlType) {
        String javaType = "String"; // 默认类型，如果未知或不常见类型

        // MySQL到Java类型的基本映射，使用包装类
        if (sqlType.startsWith("int") || sqlType.startsWith("integer")) {
            javaType = "Integer";
        } else if (sqlType.startsWith("tinyint")) {
            javaType = "Byte";
        } else if (sqlType.startsWith("smallint")) {
            javaType = "Short";
        } else if (sqlType.startsWith("mediumint")) {
            javaType = "Integer";
        } else if (sqlType.startsWith("bigint")) {
            javaType = "Long";
        } else if (sqlType.startsWith("float")) {
            javaType = "Float";
        } else if (sqlType.startsWith("double")) {
            javaType = "Double";
        } else if (sqlType.startsWith("varchar") || sqlType.startsWith("text")) {
            javaType = "String";
        } else if (sqlType.startsWith("timestamp")) {
            javaType = "LocalDateTime";
        } else if (sqlType.startsWith("datetime")) {
            javaType = "LocalDateTime";
        } else if (sqlType.startsWith("date")) {
            javaType = "LocalDate";
        } else if (sqlType.startsWith("time")) {
            javaType = "LocalTime";
        } else if (sqlType.startsWith("binary") || sqlType.startsWith("varbinary") || sqlType.startsWith("blob")) {
            javaType = "byte[]";
        }

        return javaType;
    }
}
