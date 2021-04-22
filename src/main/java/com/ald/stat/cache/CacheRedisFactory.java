package com.ald.stat.cache;

import com.ald.stat.cache.impl.JedisClientPool;
import com.ald.stat.utils.ConfigUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class CacheRedisFactory {
    private static final Logger log = LoggerFactory.getLogger(CacheRedisFactory.class.getSimpleName());
    static ConcurrentHashMap<String, JedisClientPool> redisCaches = new ConcurrentHashMap();
    static ConcurrentHashMap<String, JedisCluster> clusterCaches = new ConcurrentHashMap();


    public static BaseRedisCache getInstances() {
        String noCluster = ConfigUtils.getProperty("client.redis.no.cluster");
        if (noCluster != null && noCluster.equals("false")) {
            return getClusterInstancesFromProperties(null);
        } else {
            return getInstancesFromProperties(null);
        }
    }

    public static BaseRedisCache getInstances(String prefix) {
        String noCluster = ConfigUtils.getProperty(prefix + ".client.redis.no.cluster");
        if (noCluster != null && noCluster.equals("false")) {
            return getClusterInstancesFromProperties(prefix);
        } else {
            return getInstancesFromProperties(prefix);
        }
    }

    public synchronized static BaseRedisCache getInstancesFromProperties(String perfix) {
        String writeMaxIdle = "client.redis.write.pool.maxIdle";
        if (!StringUtils.isEmpty(perfix)) writeMaxIdle = perfix + "." + writeMaxIdle;
        String writeTestOnBorrow = "client.redis.pool.testOnBorrow";
        if (!StringUtils.isEmpty(perfix)) writeTestOnBorrow = perfix + "." + writeTestOnBorrow;
        String writePoolHost = "client.redis.write.pool";
        if (!StringUtils.isEmpty(perfix)) writePoolHost = perfix + "." + writePoolHost;
        String writePoolHostPassword = "client.redis.write.pool.password";
        if (!StringUtils.isEmpty(perfix)) writePoolHostPassword = perfix + "." + writePoolHostPassword;

        String readMaxIdle = "client.redis.write.pool.maxIdle";
        if (!StringUtils.isEmpty(perfix)) readMaxIdle = perfix + "." + readMaxIdle;
//        String readTestOnBorrow = "client.redis.pool.testOnBorrow";
//        if (!StringUtils.isEmpty(perfix)) writeMaxIdle=perfix+"."+readTestOnBorrow;
        String readPoolHost = "client.redis.write.pool";
        if (!StringUtils.isEmpty(perfix)) readPoolHost = perfix + "." + readPoolHost;
        String readPoolHostPassword = "client.redis.write.pool.password";
        if (!StringUtils.isEmpty(perfix)) readPoolHostPassword = perfix + "." + readPoolHostPassword;

        String poolId = "default";
        if (StringUtils.isNotEmpty(perfix)) {
            poolId = perfix;
        }
        if (redisCaches.get(poolId) == null) {
            try {
                GenericObjectPoolConfig readPoolConfig = new GenericObjectPoolConfig();
                JedisClientPool readPool = null;

                GenericObjectPoolConfig writePoolConfig = new GenericObjectPoolConfig();

                writePoolConfig.setMaxIdle(Integer.valueOf(ConfigUtils.getProperty(writeMaxIdle)));
                writePoolConfig.setTestOnBorrow(Boolean.valueOf(ConfigUtils.getProperty(writeTestOnBorrow)));
                writePoolConfig.setMaxWaitMillis(480000);
                String host = ConfigUtils.getProperty(writePoolHost);
                //System.out.println("host:" + host + " password:" + ConfigUtils.getProperty(writePoolHostPassword));
                JedisClientPool writePool = new JedisClientPool(readPoolConfig, ConfigUtils.getProperty(writePoolHost), ConfigUtils.getProperty(writePoolHostPassword), 480000);
                if (ConfigUtils.getProperty(readPoolHost) != null) {
                    readPoolConfig.setMaxIdle(Integer.valueOf(ConfigUtils.getProperty(readMaxIdle)));
                    readPoolConfig.setTestOnBorrow(Boolean.valueOf(ConfigUtils.getProperty(writeTestOnBorrow)));
                    readPoolConfig.setMaxWaitMillis(480000);
                    readPool = new JedisClientPool(readPoolConfig, ConfigUtils.getProperty(readPoolHost), ConfigUtils.getProperty(readPoolHostPassword), 480000);
                } else {
                    readPool = writePool;
                }
//                DefaultRedis defaultRedis = new DefaultRedis();
//                defaultRedis.setJedis(readPool);
                redisCaches.put(poolId, readPool);
                //CacheClientFactory.redisCache = redisCache;
                return new DefaultRedis(redisCaches.get(poolId).getResource());
            } catch (Exception e) {
                log.error("getInstancesFromProperties init redis connecton failed");
                e.printStackTrace();
                return null;
            }
        } else {
            return new DefaultRedis(redisCaches.get(poolId).getResource());
        }
    }

    public synchronized static BaseRedisCache getClusterInstancesFromProperties(String perfix) {
        String writeMaxIdle = "client.redis.write.pool.maxIdle";
        if (!StringUtils.isEmpty(perfix)) writeMaxIdle = perfix + "." + writeMaxIdle;
        String writeTestOnBorrow = "client.redis.pool.testOnBorrow";
        if (!StringUtils.isEmpty(perfix)) writeTestOnBorrow = perfix + "." + writeTestOnBorrow;
        String writePoolHost = "client.redis.write.pool";
        if (!StringUtils.isEmpty(perfix)) writePoolHost = perfix + "." + writePoolHost;
        String writePoolHostPassword = "client.redis.write.pool.password";
        if (!StringUtils.isEmpty(perfix)) writePoolHostPassword = perfix + "." + writePoolHostPassword;
        String poolId = "default";
        if (StringUtils.isNotEmpty(perfix)) {
            poolId = perfix;
        }
        if (redisCaches.get(poolId) == null) {
            try {
                Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
                String nodesstr = ConfigUtils.getProperty(writePoolHost);
                String[] nodes = nodesstr.split("[,]");
                for (String nodestr : nodes) {
                    String node[] = nodestr.split(":");
                    jedisClusterNodes.add(new HostAndPort(node[0], Integer.parseInt(node[1])));
                }
                GenericObjectPoolConfig writePoolConfig = new GenericObjectPoolConfig();
                writePoolConfig.setMaxWaitMillis(360000);
                writePoolConfig.setMaxIdle(Integer.valueOf(ConfigUtils.getProperty(writeMaxIdle)));
                writePoolConfig.setTestOnBorrow(Boolean.valueOf(ConfigUtils.getProperty(writeTestOnBorrow)));
                //Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout,int maxAttempts, String password, final GenericObjectPoolConfig poolConfig
                JedisCluster jc = new JedisCluster(jedisClusterNodes, 360000, 10000, 3, ConfigUtils.getProperty(writePoolHostPassword), writePoolConfig);
                clusterCaches.put(poolId, jc);
                //CacheClientFactory.redisCache = redisCache;
                return new ClusterRedis(clusterCaches.get(poolId));
            } catch (Exception e) {
                log.error("getClusterInstancesFromProperties init redis connecton failed");
                e.printStackTrace();
                return null;
            }
        } else {
            return new ClusterRedis(clusterCaches.get(poolId));
        }
    }
}
