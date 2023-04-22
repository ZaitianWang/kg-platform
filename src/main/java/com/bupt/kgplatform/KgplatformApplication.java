package com.bupt.kgplatform;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan("com.bupt.kgplatform.mapper")
public class KgplatformApplication {

    public static void main(String[] args) {
        SpringApplication.run(KgplatformApplication.class, args);
    }

}
