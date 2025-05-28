package com.code.controller;

import com.code.model.CodeVo;
import com.code.model.Dto;
import com.code.service.impl.SqlParseServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;


@Slf4j
@RestController
public class SqlParseController {
    private static final Logger log = LoggerFactory.getLogger(SqlParseController.class);
    @Autowired
    SqlParseServiceImpl sqlParseService;
    @PostMapping(value = "/code")
    public CodeVo chat(@RequestBody Dto dto) throws Exception {
        log.info("接口请求参数：\n{}",dto);
        String sql = dto.getSql();
        return sqlParseService.parseCreateSql(sql);
    }
}
