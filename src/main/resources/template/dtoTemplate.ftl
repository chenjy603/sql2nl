<#assign tableName=StringUtils.snakeToCamel(tableName?lower_case)>
public class ${tableName?cap_first}Dto {
<#list columns as column>
    <#assign columnName=StringUtils.snakeToCamel(column.name?lower_case)>
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

    public static ${tableName?cap_first} convertEntity(${tableName?cap_first}Dto dto) {
        ${tableName?cap_first} ${tableName} =new ${tableName?cap_first}();
        <#list columns as column>
        <#assign columnName=StringUtils.snakeToCamel(column.name?lower_case)>
        ${tableName}.set${columnName?cap_first}(dto.get${columnName?cap_first}());
        </#list>
        return ${tableName};
    }
}