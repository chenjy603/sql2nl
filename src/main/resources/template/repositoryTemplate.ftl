<#assign tableName=StringUtils.snakeToCamel(tableName?lower_case)>
@Repository
public class ${tableName?cap_first}Repository {

    private final ${tableName?cap_first}Mapper ${tableName}Mapper;
    private final ${tableName?cap_first}ServiceImpl ${tableName}Service;

    public ${tableName?cap_first}Repository(${tableName?cap_first}Mapper ${tableName}Mapper, ${tableName?cap_first}ServiceImpl ${tableName}Service) {
        this.${tableName}Mapper = ${tableName}Mapper;
        this.${tableName}Service = ${tableName}Service;
    }

    private LambdaQueryWrapper< ${tableName?cap_first}> getQueryWrapper(${tableName?cap_first}Dto dto) {
        LambdaQueryWrapper< ${tableName?cap_first}> query = new LambdaQueryWrapper<>();
    <#list columns as column>
    <#assign columnName=StringUtils.snakeToCamel(column.name?lower_case)>
        if (StringUtils.isNotBlank(dto.get${columnName?cap_first}())) {
        query.eq(${tableName?cap_first}::get${columnName?cap_first}, dto.get${columnName?cap_first}());
        }
    </#list>
        return query;
    }

    public boolean create(${tableName?cap_first}Dto dto) {
        ${tableName?cap_first} ${tableName} = ${tableName?cap_first}Dto.convertEntity(dto);
        boolean result = this.${tableName}Service.save(${tableName});
        return result;
    }

    public boolean delete(Long id) {
        boolean result = false;
        ${tableName?cap_first} ${tableName} = this.${tableName}Service.getById(id);
        if (Objects.nonNull(${tableName})) {
            result = this.${tableName}Service.removeById(id);
        }
        return result;
    }

    public boolean update(${tableName?cap_first}Dto dto) {
        ${tableName?cap_first} ${tableName} = ${tableName?cap_first}Dto.convertEntity(dto);
        boolean result = this.${tableName}Service.updateById(${tableName});
        return result;
    }

    public List< ${tableName?cap_first}> list(${tableName?cap_first}Dto dto) {
        LambdaQueryWrapper< ${tableName?cap_first}> query = getQueryWrapper(dto);
        return ${tableName}Mapper.selectList(query);
    }
}