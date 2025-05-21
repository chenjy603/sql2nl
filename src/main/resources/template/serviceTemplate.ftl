<#assign tableName=StringUtils.snakeToCamel(tableName?lower_case)>
public interface ${tableName?cap_first}Service extends IService< ${tableName?cap_first}> {}