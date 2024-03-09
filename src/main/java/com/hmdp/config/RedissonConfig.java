package com.hmdp.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RedissonConfig {

    @Bean
    public RedissonClient redissonClient() {
        Config config = new Config();
        // 配置
        config.useSingleServer().setAddress("redis://192.168.48.132:6379")
        .setPassword("000207");
        // 创建对象
        return Redisson.create(config);
    }
}
