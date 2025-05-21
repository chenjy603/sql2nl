package com.code.utils;

import freemarker.cache.StringTemplateLoader;
import freemarker.template.*;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.StringWriter;
import java.util.Map;

@Component
@Slf4j
public class FreemarkerUtils {
    private static final Logger log = LoggerFactory.getLogger(FreemarkerUtils.class);
    private static Configuration freemarkerConfig;
    // 创建字符串模板加载器
    private static final StringTemplateLoader templateLoader = new StringTemplateLoader();

    static {
        try {
            initFreemarkerConfig();
        } catch (TemplateModelException e) {
            throw new RuntimeException(e);
        }
    }

    private static void initFreemarkerConfig() throws TemplateModelException {
        if (freemarkerConfig == null) {
            freemarkerConfig = new Configuration(Configuration.VERSION_2_3_32);
            freemarkerConfig.setDefaultEncoding("UTF-8");
            freemarkerConfig.setSharedVariable("StringUtils",
                    new DefaultObjectWrapperBuilder(Configuration.VERSION_2_3_32).build().wrap(new StringUtils()));
            freemarkerConfig.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
            freemarkerConfig.setClassForTemplateLoading(FreemarkerUtils.class, "/template");
        }
        //使用resources下的模板不能使用StringTemplateLoader否则会覆盖
//        freemarkerConfig.setTemplateLoader(templateLoader);
    }

    public static void setTemplateLoader(String templateName, String templateContent) {
        // 动态生成的模板字符串
        templateLoader.putTemplate(templateName, templateContent);
    }

    public static String process(String templateName, Map<String, Object> model) {
        try (StringWriter writer = new StringWriter()) {
            // 加载模板
            Template myTemplate = freemarkerConfig.getTemplate(templateName);
            // 渲染模板到字符串
            myTemplate.process(model, writer);
            return writer.toString();
        } catch (TemplateNotFoundException e) {
            log.error("Template not found: {}", templateName, e);
        } catch (Exception e) {
            log.error("FreemarkerMonitor setTemplateLoader error:", e);
        }
        return null;
    }
}
