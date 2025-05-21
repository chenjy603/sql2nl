package com.cjy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;
import java.util.stream.Collectors;

public class VegaJson2EChart {
    public static Map<String, Object> buildEChartsOption(Map<String, Object> spec) {
        List<Map<String, Object>> layer = (List<Map<String, Object>>) spec.get("layer");
        String type = ((Map<String, Object>) layer.get(0).get("mark")).get("type").toString();

        Map<String, Object> option = new HashMap<>();

        if ("arc".equals(type)) {
            String nameKey = ((Map<String, Object>) ((Map<String, Object>) layer.get(0).get("encoding")).get("color")).get("field").toString();
            String valueKey = layer.get(0).get("_yField").toString();

            List<Map<String, Object>> values = (List<Map<String, Object>>) ((Map<String, Object>) spec.get("data")).get("values");

            List<Map<String, Object>> data = values.stream().map(item -> {
                Map<String, Object> map = new HashMap<>();
                map.put("name", item.get(nameKey));
                map.put("value", item.get(valueKey));
                return map;
            }).collect(Collectors.toList());

            option.put("grid", Map.of("containLabel", true));
            option.put("legend", Map.of("show", true));
            option.put("tooltip", Map.of(
                    "show", true,
                    "trigger", "item",
                    "position", List.of("10%", "10%")
            ));
            option.put("series", List.of(Map.of(
                    "type", "pie",
                    "radius", "50%",
                    "data", data,
                    "emphasis", Map.of("itemStyle", Map.of(
                            "shadowBlur", 10,
                            "shadowOffsetX", 0,
                            "shadowColor", "rgba(0, 0, 0, 0.5)"
                    ))
            )));
        } else {
            Map<String, Object> encoding = (Map<String, Object>) layer.get(0).get("encoding");
            Map<String, Object> x = (Map<String, Object>) encoding.get("x");
            Map<String, Object> y = (Map<String, Object>) encoding.get("y");
            Map<String, Object> xOffset = (Map<String, Object>) encoding.get("xOffset");

            String xField = x.get("field").toString();
            String yField = y.get("field").toString();
            String xOffsetField = xOffset != null ? xOffset.get("field").toString() : null;

            double maxValue = 0;
            Set<String> xDataSet = new LinkedHashSet<>();
            List<Map<String, Object>> seriesData = new ArrayList<>();

            List<Map<String, Object>> values = (List<Map<String, Object>>) ((Map<String, Object>) spec.get("data")).get("values");

            for (Map<String, Object> n : values) {
                double yValue = Double.parseDouble(n.get(yField).toString());
                if (yValue > maxValue) {
                    maxValue = yValue;
                }

                xDataSet.add(n.get(xField).toString());

                if (xOffsetField != null) {
                    String name = n.get(xOffsetField).toString();
                    Map<String, Object> series = seriesData.stream()
                            .filter(s -> name.equals(s.get("name")))
                            .findFirst()
                            .orElse(null);
                    if (series == null) {
                        Map<String, Object> newSeries = new HashMap<>();
                        newSeries.put("name", name);
                        newSeries.put("type", type);
                        newSeries.put("data", new ArrayList<>(List.of(List.of(n.get(xField), yValue))));
                        seriesData.add(newSeries);
                    } else {
                        ((List<Object>) series.get("data")).add(List.of(n.get(xField), yValue));
                    }
                } else {
                    if (seriesData.isEmpty()) {
                        Map<String, Object> newSeries = new HashMap<>();
                        newSeries.put("type", type);
                        newSeries.put("data", new ArrayList<>(List.of(List.of(n.get(xField), yValue))));
                        if ("bar".equals(type)) {
                            newSeries.put("barMaxWidth", 50);
                        }
                        seriesData.add(newSeries);
                    } else {
                        ((List<Object>) seriesData.get(0).get("data")).add(List.of(n.get(xField), yValue));
                    }
                }
            }

            int rightPadding = xField.length() * 16;
            int leftPadding = String.valueOf((int) maxValue).length() * 11;

            Map<String, Object> grid = Map.of(
                    "bottom", 100,
                    "right", rightPadding,
                    "left", leftPadding
            );

            option.put("grid", grid);

            if (seriesData.size() > 1) {
                option.put("legend", Map.of(
                        "show", true,
                        "type", "scroll",
                        "selector", List.of(
                                Map.of("type", "all", "title", "全选"),
                                Map.of("type", "inverse", "title", "反选")
                        )
                ));
            }

            option.put("tooltip", Map.of(
                    "show", true,
                    "position", List.of("10%", "10%")
            ));

            option.put("xAxis", Map.of(
                    "name", xField,
                    "type", "category",
                    "boundaryGap", true,
                    "axisLabel", Map.of(
                            "rotate", ((Map<String, Object>) x.get("axis")).get("labelAngle"),
                            "interval", 0
                    )
            ));

            option.put("yAxis", Map.of(
                    "type", "value",
                    "name", yField
            ));

            option.put("dataZoom", List.of(
                    Map.of("type", "inside", "start", 0, "end", 100),
                    Map.of("start", 0, "end", 100)
            ));

            option.put("series", seriesData);
        }

        return option;
    }

    public static void main(String[] args) throws JsonProcessingException {
        String s = " {\n" +
                "    \"schema\": \"https://vega.github.io/schema/vega-lite/v5.json\",\n" +
                "    \"_type\": \"vega-json\",\n" +
                "    \"data\": {\n" +
                "        \"values\": [\n" +
                "            {\n" +
                "                \"数据日期\": \"2024-09-01\",\n" +
                "                \"机构名称\": \"上海北新泾支行\",\n" +
                "                \"当月访客客户数（户）\": 0\n" +
                "            },\n" +
                "            {\n" +
                "                \"数据日期\": \"2024-09-03\",\n" +
                "                \"机构名称\": \"上海北新泾支行\",\n" +
                "                \"当月访客客户数（户）\": 0\n" +
                "            },\n" +
                "            {\n" +
                "                \"数据日期\": \"2024-09-01\",\n" +
                "                \"机构名称\": \"上海黄浦支行\",\n" +
                "                \"当月访客客户数（户）\": 47\n" +
                "            },\n" +
                "            {\n" +
                "                \"数据日期\": \"2024-09-02\",\n" +
                "                \"机构名称\": \"上海黄浦支行\",\n" +
                "                \"当月访客客户数（户）\": 47\n" +
                "            },\n" +
                "            {\n" +
                "                \"数据日期\": \"2024-09-01\",\n" +
                "                \"机构名称\": \"上海分行营业部\",\n" +
                "                \"当月访客客户数（户）\": 48\n" +
                "            },\n" +
                "            {\n" +
                "                \"数据日期\": \"2024-09-02\",\n" +
                "                \"机构名称\": \"上海分行营业部\",\n" +
                "                \"当月访客客户数（户）\": 48\n" +
                "            }\n" +
                "        ]\n" +
                "    },\n" +
                "    \"layer\": [\n" +
                "        {\n" +
                "            \"mark\": {\n" +
                "                \"type\": \"line\",\n" +
                "                \"tooltip\": true,\n" +
                "                \"point\": {\n" +
                "                    \"size\": 30\n" +
                "                }\n" +
                "            },\n" +
                "            \"_yField\": \"机构名称-当月访客客户数（户）\",\n" +
                "            \"encoding\": {\n" +
                "                \"x\": {\n" +
                "                    \"field\": \"数据日期\",\n" +
                "                    \"type\": \"nominal\",\n" +
                "                    \"aggregate\": null,\n" +
                "                    \"stack\": null,\n" +
                "                    \"sort\": \"x\",\n" +
                "                    \"axis\": {\n" +
                "                        \"title\": \"数据日期\",\n" +
                "                        \"symbolLimit\": null,\n" +
                "                        \"labelExpr\": \"datum.value\",\n" +
                "                        \"labelAngle\": 45\n" +
                "                    }\n" +
                "                },\n" +
                "                \"y\": {\n" +
                "                    \"field\": \"当月访客客户数（户）\",\n" +
                "                    \"type\": \"quantitative\",\n" +
                "                    \"aggregate\": \"sum\",\n" +
                "                    \"stack\": null,\n" +
                "                    \"axis\": {\n" +
                "                        \"tickCount\": 10,\n" +
                "                        \"title\": \"当月访客客户数（户）\",\n" +
                "                        \"symbolLimit\": null,\n" +
                "                        \"format\": \",.2f\",\n" +
                "                        \"labelExpr\": \"datum.value\",\n" +
                "                        \"labelAngle\": 0\n" +
                "                    }\n" +
                "                },\n" +
                "                \"theta\": null,\n" +
                "                \"xOffset\": {\n" +
                "                    \"field\": \"机构名称\",\n" +
                "                    \"type\": \"nominal\"\n" +
                "                },\n" +
                "                \"color\": {\n" +
                "                    \"field\": \"机构名称\",\n" +
                "                    \"type\": \"nominal\",\n" +
                "                    \"aggregate\": null,\n" +
                "                    \"stack\": null,\n" +
                "                    \"sort\": null,\n" +
                "                    \"axis\": {\n" +
                "                        \"title\": \"机构名称\",\n" +
                "                        \"symbolLimit\": null,\n" +
                "                        \"labelExpr\": \"datum.value\",\n" +
                "                        \"labelAngle\": 0\n" +
                "                    },\n" +
                "                    \"legend\": {\n" +
                "                        \"title\": \"机构名称\",\n" +
                "                        \"columns\": 5,\n" +
                "                        \"symbolLimit\": 1000,\n" +
                "                        \"orient\": \"bottom\",\n" +
                "                        \"labelExpr\": \"datum.value\",\n" +
                "                        \"labelAngle\": 0\n" +
                "                    }\n" +
                "                },\n" +
                "                \"opacity\": {\n" +
                "                    \"condition\": {\n" +
                "                        \"param\": \"机构名称\"\n" +
                "                    },\n" +
                "                    \"value\": 0\n" +
                "                }\n" +
                "            },\n" +
                "            \"params\": [\n" +
                "                {\n" +
                "                    \"name\": \"机构名称\",\n" +
                "                    \"select\": {\n" +
                "                        \"type\": \"point\",\n" +
                "                        \"fields\": [\n" +
                "                            \"机构名称\"\n" +
                "                        ],\n" +
                "                        \"on\": \"click\"\n" +
                "                    },\n" +
                "                    \"bind\": \"legend\"\n" +
                "                }\n" +
                "            ]\n" +
                "        }\n" +
                "    ],\n" +
                "    \"title\": null\n" +
                "}\n";
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> map = objectMapper.readValue(s, Map.class);
        Map<String, Object> stringObjectMap = VegaJson2EChart.buildEChartsOption(map);
        String s1 = objectMapper.writeValueAsString(stringObjectMap);
        System.out.println(s1);
    }
}


