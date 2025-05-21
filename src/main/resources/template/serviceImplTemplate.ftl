<#assign tableName=StringUtils.snakeToCamel(tableName?lower_case)>
@Service
public class ${tableName?cap_first}ServiceImpl extends ServiceImpl< ${tableName?cap_first}Mapper, ${tableName?cap_first}> implements ${tableName?cap_first}Service {

    @Autowired
    private ${tableName?cap_first}Mapper ${tableName}Mapper;

}
