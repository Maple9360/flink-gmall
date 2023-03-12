package com.maple.gmallpublisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@MapperScan(basePackages = "com.maple.gmallpublisher.mapper") // 扫描包，mapper包
@SpringBootApplication
public class GmallPublisherApplication {
    public static void main(String[] args) {
        SpringApplication.run(GmallPublisherApplication.class);
    }
}
