package com.code.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
@Slf4j
public class Index {

    @GetMapping(value = "/")
    public String chat() {
        return "index"; // 返回视图名称，不需要加文件扩展名
    }
}
