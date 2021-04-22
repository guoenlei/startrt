package com.ald.stat.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractRedisCache implements BaseRedisCache{
    private static Logger Log = LoggerFactory.getLogger(AbstractRedisCache.class);
    private static final String SUCCESS = "OK";

    public abstract BaseRedisCache getResource();
}
