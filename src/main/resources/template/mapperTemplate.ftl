<#assign tableName=StringUtils.snakeToCamel(tableName?lower_case)>
@Mapper
public interface ${tableName?cap_first}Mapper extends BaseMapper < ${tableName?cap_first}> {}