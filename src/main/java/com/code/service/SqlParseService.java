package com.code.service;

import com.code.model.CodeVo;
import net.sf.jsqlparser.JSQLParserException;

public interface SqlParseService {
    CodeVo  parseCreateSql(String sql) throws JSQLParserException;
}
