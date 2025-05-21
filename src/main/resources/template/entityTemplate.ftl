<#assign tableName=StringUtils.snakeToCamel(tableName?lower_case)>
public class ${tableName?cap_first} {
    <#list columns as column>
    <#assign columnName=StringUtils.snakeToCamel(column.name?lower_case)>
    <#if columnName=='id'>@TableId(value = "${column.name}", type = IdType.AUTO)<#else>@TableField("${column.name}")</#if>
    private ${column.javaType} ${columnName};

    </#list>

    <#list columns as column>
    <#assign columnName=StringUtils.snakeToCamel(column.name?lower_case)>
    public ${column.javaType} get${columnName?cap_first}() {
        return ${columnName};
    }

    public void set${columnName?cap_first}(${column.javaType} ${columnName}) {
        this.${columnName} =${columnName};
    }

    </#list>
}