package com.cjy;

import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.event.AnalysisEventListener;
import com.alibaba.excel.support.ExcelTypeEnum;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class ExcelUtil {

    /**
     * 通用导出方法
     */
    public static <T> void export(HttpServletResponse response,
                                  List<T> dataList,
                                  List<String> fieldList,
                                  Map<String, String> fieldNameMap,
                                  String fileName) throws IOException {
        if (dataList == null || dataList.isEmpty()) {
            throw new IllegalArgumentException("数据不能为空");
        }

        Class<?> clazz = dataList.get(0).getClass();

        // 确保 id 在第一列，其他字段跟在后面（去重）
        List<String> actualFields = new ArrayList<>();
        actualFields.add("id");
        actualFields.addAll(fieldList.stream().filter(f -> !"id".equalsIgnoreCase(f)).collect(Collectors.toList()));

        // 设置响应头
        response.setContentType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
        response.setCharacterEncoding("utf-8");
        String encodedFileName = URLEncoder.encode(fileName, StandardCharsets.UTF_8).replaceAll("\\+", "%20");
        response.setHeader("Content-disposition", "attachment;filename*=utf-8''" + encodedFileName + ".xlsx");

        // 表头
        List<List<String>> head = actualFields.stream()
                .map(f -> Collections.singletonList(fieldNameMap.getOrDefault(f, f)))
                .collect(Collectors.toList());

        // 构建数据
        List<List<Object>> rows = new ArrayList<>();
        for (T item : dataList) {
            List<Object> row = new ArrayList<>();
            for (String field : actualFields) {
                try {
                    Field declaredField = getField(clazz, field);
                    declaredField.setAccessible(true);
                    Object value = declaredField.get(item);
                    row.add(value != null ? value : "");
                } catch (Exception e) {
                    row.add("");
                }
            }
            rows.add(row);
        }

        // 写入 Excel
        EasyExcel.write(response.getOutputStream())
                .head(head)
                .sheet("导出数据")
                .doWrite(rows);

//        try(ByteArrayOutputStream outputStream = new ByteArrayOutputStream()){
//            EasyExcel.write(outputStream)
//                    .head(head)
//                    .excelType(ExcelTypeEnum.CSV)
//                    .autoCloseStream(true)
//                    .sheet("导出数据")
//                    .doWrite(rows);
//            try(ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray())){
//                s3.upload()
//            }
//            response.getOutputStream().write(outputStream.toByteArray());
//            response.getOutputStream().flush();
//        }


    }

    // 支持父类字段获取
    private static Field getField(Class<?> clazz, String fieldName) throws NoSuchFieldException {
        Class<?> current = clazz;
        while (current != null) {
            try {
                return current.getDeclaredField(fieldName);
            } catch (NoSuchFieldException e) {
                current = current.getSuperclass();
            }
        }
        throw new NoSuchFieldException("字段未找到: " + fieldName);
    }

    public void exportUsers(@RequestParam List<String> fields, HttpServletResponse response) throws IOException {
        // 1. 获取数据（假设 MP 查出来）
//        List<User> userList = userService.list(); // 这里用你自己的查询逻辑

        // 2. 中文字段名映射
        Map<String, String> fieldNameMap = Map.of(
                "id", "序号",
                "name", "姓名",
                "age", "年龄",
                "email", "邮箱",
                "phone", "手机号",
                "gender", "性别",
                "createTime", "注册时间"
        );

        // 3. 调用通用导出方法（id 会自动在第一列）
//        ExcelUtil.export(response, userList, fields, fieldNameMap, "用户信息导出");
    }

    public static <T> void importAndUpdate(
            InputStream inputStream,
            Map<String, String> fieldToHeaderMap,
            BiConsumer<String, Map<String, String>> updateHandler) {

        Map<String, String> headerToFieldMap = fieldToHeaderMap.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

        List<String> headers = new ArrayList<>();
        List<Map<String, String>> dataList = new ArrayList<>();

        EasyExcel.read(inputStream, new AnalysisEventListener<Map<Integer, String>>() {
            @Override
            public void invoke(Map<Integer, String> row, AnalysisContext context) {
                if (context.readRowHolder().getRowIndex() == 0) {
                    // 第一行为表头
                    headers.clear();
                    for (int i = 0; i < row.size(); i++) {
                        String header = row.get(i);
                        String field = headerToFieldMap.get(header);
                        if (field == null) {
                            throw new RuntimeException("无法识别的表头: " + header);
                        }
                        headers.add(field);
                    }
                } else {
                    Map<String, String> rowMap = new HashMap<>();
                    for (int i = 0; i < headers.size(); i++) {
                        rowMap.put(headers.get(i), row.get(i));
                    }
                    dataList.add(rowMap);
                }
            }

            @Override
            public void doAfterAllAnalysed(AnalysisContext context) {
//                log.info("读取完成，共 {} 行", dataList.size());
            }
        }).sheet().doRead();

        for (Map<String, String> row : dataList) {
            String id = row.get("id");
            if (id == null || id.isBlank()) continue;
            Map<String, String> fields = new HashMap<>(row);
            fields.remove("id");
            updateHandler.accept(id, fields);
        }
    }

    public String importUsers(@RequestParam("file") MultipartFile file) throws IOException {
        Map<String, String> fieldToHeaderMap = Map.of(
                "id", "序号",
                "name", "姓名",
                "email", "邮箱",
                "age", "年龄"
        );

        ExcelUtil.importAndUpdate(
                file.getInputStream(),
                fieldToHeaderMap,
                (id, fieldMap) -> {
                    // updateHandler 示例：根据 id 更新字段
//                    userService.updateFieldsById(id, fieldMap);
                }
        );

        return "导入成功";
    }

}
