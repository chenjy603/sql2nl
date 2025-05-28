package com.code.model;

import lombok.Data;

@Data
public class Dto {
    String sql;

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
}
