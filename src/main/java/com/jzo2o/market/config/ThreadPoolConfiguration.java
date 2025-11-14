package com.jzo2o.market.config;

import com.jzo2o.redis.properties.RedisSyncProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Configuration
public class ThreadPoolConfiguration {
    @Bean("syncThreadPool")
    public ThreadPoolExecutor synchronizedThreadPool(RedisSyncProperties redisSyncProperties){
        int corePoolSize=1;
        int maxPoolSize=redisSyncProperties.getQueueNum();
        long keepAliveTime=120;
        TimeUnit unit=TimeUnit.SECONDS;
        RejectedExecutionHandler rejectedExecutionHandler=new ThreadPoolExecutor.DiscardPolicy();
        return new ThreadPoolExecutor(corePoolSize,maxPoolSize,keepAliveTime,unit
                ,new SynchronousQueue<>(),rejectedExecutionHandler);
    }
}