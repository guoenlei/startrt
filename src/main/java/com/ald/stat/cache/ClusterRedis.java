package com.ald.stat.cache;

import redis.clients.jedis.*;
import redis.clients.jedis.commands.JedisCommands;
import redis.clients.jedis.params.GeoRadiusParam;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.params.ZAddParams;
import redis.clients.jedis.params.ZIncrByParams;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ClusterRedis extends AbstractRedisCache {
    JedisCluster jedis;

    public ClusterRedis(JedisCluster jedis) {
        this.jedis = jedis;
    }

    @Override
    public void close() throws IOException {
        this.jedis.close();
    }

    @Override
    public BaseRedisCache getResource() {
        return this;
    }

    @Override
    public String set(String key, String value) {
        return this.jedis.set(key,value);
    }

    @Override
    public String set(String key, String value, SetParams params) {
        return this.jedis.set(key,value,params);
    }

    @Override
    public String get(String key) {
        return this.jedis.get(key);
    }

    @Override
    public Boolean exists(String key) {
        return this.jedis.exists(key);
    }

    @Override
    public Long persist(String key) {
        return this.jedis.persist(key);
    }

    @Override
    public String type(String key) {
        return this.jedis.type(key);
    }

    @Override
    public byte[] dump(String key) {
        return this.jedis.dump(key);
    }

    @Override
    public String restore(String key, int ttl, byte[] serializedValue) {
        return this.jedis.restore(key,ttl,serializedValue);
    }

    @Override
    public Long expire(String key, int seconds) {
        return this.jedis.expire(key,seconds);
    }

    @Override
    public Long pexpire(String key, long milliseconds) {
        return this.jedis.pexpire(key,milliseconds);
    }

    @Override
    public Long expireAt(String key, long unixTime) {
        return this.jedis.expireAt(key,unixTime);
    }

    @Override
    public Long pexpireAt(String key, long millisecondsTimestamp) {
        return this.jedis.pexpireAt(key,millisecondsTimestamp);
    }

    @Override
    public Long ttl(String key) {
        return this.jedis.ttl(key);
    }

    @Override
    public Long pttl(String key) {
        return this.jedis.pttl(key);
    }

    @Override
    public Long touch(String key) {
        return this.jedis.touch(key);
    }

    @Override
    public Boolean setbit(String key, long offset, boolean value) {
        return this.jedis.setbit(key,offset,value);
    }

    @Override
    public Boolean setbit(String key, long offset, String value) {
        return this.jedis.setbit(key,offset,value);
    }

    @Override
    public Boolean getbit(String key, long offset) {
        return this.jedis.getbit(key,offset);
    }

    @Override
    public Long setrange(String key, long offset, String value) {
        return this.jedis.setrange(key,offset,value);
    }

    @Override
    public String getrange(String key, long startOffset, long endOffset) {
        return this.jedis.getrange(key,startOffset,endOffset);
    }

    @Override
    public String getSet(String key, String value) {
        return this.jedis.getSet(key,value);
    }

    @Override
    public Long setnx(String key, String value) {
        return this.jedis.setnx(key,value);
    }

    @Override
    public String setex(String key, int seconds, String value) {
        return this.jedis.setex(key,seconds,value);
    }

    @Override
    public String psetex(String key, long milliseconds, String value) {
        return this.jedis.psetex(key,milliseconds,value);
    }

    @Override
    public Long decrBy(String key, long decrement) {
        return this.jedis.decrBy(key,decrement);
    }

    @Override
    public Long decr(String key) {
        return this.jedis.decr(key);
    }

    @Override
    public Long incrBy(String key, long increment) {
        return this.jedis.incrBy(key,increment);
    }

    @Override
    public Double incrByFloat(String key, double increment) {
        return this.jedis.incrByFloat(key,increment);
    }

    @Override
    public Long incr(String key) {
        return this.jedis.incr(key);
    }

    @Override
    public Long append(String key, String value) {
        return this.jedis.append(key,value);
    }

    @Override
    public String substr(String key, int start, int end) {
        return this.jedis.substr(key,start,end);
    }

    @Override
    public Long hset(String key, String field, String value) {
        return this.jedis.hset(key,field,value);
    }

    @Override
    public Long hset(String key, Map<String, String> hash) {
        return this.jedis.hset(key,hash);
    }

    @Override
    public String hget(String key, String field) {
        return this.jedis.hget(key,field);
    }

    @Override
    public Long hsetnx(String key, String field, String value) {
        return this.jedis.hsetnx(key,field,value);
    }

    @Override
    public String hmset(String key, Map<String, String> hash) {
        return this.jedis.hmset(key,hash);
    }

    @Override
    public List<String> hmget(String key, String... fields) {
        return this.jedis.hmget(key,fields);
    }

    @Override
    public Long hincrBy(String key, String field, long value) {
        return this.jedis.hincrBy(key,field,value);
    }

    @Override
    public Boolean hexists(String key, String field) {
        return this.jedis.hexists(key,field);
    }

    @Override
    public Long hdel(String key, String... field) {
        return this.jedis.hdel(key,field);
    }

    @Override
    public Long hlen(String key) {
        return this.jedis.hlen(key);
    }

    @Override
    public Set<String> hkeys(String key) {
        return this.jedis.hkeys(key);
    }

    @Override
    public List<String> hvals(String key) {
        return this.jedis.hvals(key);
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        return this.jedis.hgetAll(key);
    }

    @Override
    public Long rpush(String key, String... string) {
        return this.jedis.rpush(key,string);
    }

    @Override
    public Long lpush(String key, String... string) {
        return this.jedis.lpush(key,string);
    }

    @Override
    public Long llen(String key) {
        return this.jedis.llen(key);
    }

    @Override
    public List<String> lrange(String key, long start, long stop) {
        return this.jedis.lrange(key,start,stop);
    }

    @Override
    public String ltrim(String key, long start, long stop) {
        return this.jedis.ltrim(key,start,stop);
    }

    @Override
    public String lindex(String key, long index) {
        return this.jedis.lindex(key,index);
    }

    @Override
    public String lset(String key, long index, String value) {
        return this.jedis.lset(key,index,value);
    }

    @Override
    public Long lrem(String key, long count, String value) {
        return this.jedis.lrem(key,count,value);
    }

    @Override
    public String lpop(String key) {
        return this.jedis.lpop(key);
    }

    @Override
    public String rpop(String key) {
        return this.jedis.rpop(key);
    }

    @Override
    public Long sadd(String key, String... member) {
        return this.jedis.sadd(key,member);
    }

    @Override
    public Set<String> smembers(String key) {
        return this.jedis.smembers(key);
    }

    @Override
    public Long srem(String key, String... member) {
        return this.jedis.srem(key,member);
    }

    @Override
    public String spop(String key) {
        return this.jedis.spop(key);
    }

    @Override
    public Set<String> spop(String key, long count) {
        return this.jedis.spop(key,count);
    }

    @Override
    public Long scard(String key) {
        return this.jedis.scard(key);
    }

    @Override
    public Boolean sismember(String key, String member) {
        return this.jedis.sismember(key,member);
    }

    @Override
    public String srandmember(String key) {
        return this.jedis.srandmember(key);
    }

    @Override
    public List<String> srandmember(String key, int count) {
        return this.jedis.srandmember(key,count);
    }

    @Override
    public Long strlen(String key) {
        return this.jedis.strlen(key);
    }

    @Override
    public Long zadd(String key, double score, String member) {
        return this.jedis.zadd(key,score,member);
    }

    @Override
    public Long zadd(String key, double score, String member, ZAddParams params) {
        return this.jedis.zadd(key,score,member,params);
    }

    @Override
    public Long zadd(String key, Map<String, Double> scoreMembers) {
        return this.jedis.zadd(key,scoreMembers);
    }

    @Override
    public Long zadd(String key, Map<String, Double> scoreMembers, ZAddParams params) {
        return this.jedis.zadd(key,scoreMembers,params);
    }

    @Override
    public Set<String> zrange(String key, long start, long stop) {
        return this.jedis.zrange(key,start,stop);
    }

    @Override
    public Long zrem(String key, String... members) {
        return this.jedis.zrem(key,members);
    }

    @Override
    public Double zincrby(String key, double increment, String member) {
        return this.jedis.zincrby(key,increment,member);
    }

    @Override
    public Double zincrby(String key, double increment, String member, ZIncrByParams params) {
        return this.jedis.zincrby(key,increment,member,params);
    }

    @Override
    public Long zrank(String key, String member) {
        return this.jedis.zrank(key,member);
    }

    @Override
    public Long zrevrank(String key, String member) {
        return this.jedis.zrevrank(key,member);
    }

    @Override
    public Set<String> zrevrange(String key, long start, long stop) {
        return this.jedis.zrevrange(key,start,stop);
    }

    @Override
    public Set<Tuple> zrangeWithScores(String key, long start, long stop) {
        return this.jedis.zrangeWithScores(key,start,stop);
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(String key, long start, long stop) {
        return this.jedis.zrevrangeWithScores(key,start,stop);
    }

    @Override
    public Long zcard(String key) {
        return this.jedis.zcard(key);
    }

    @Override
    public Double zscore(String key, String member) {
        return this.jedis.zscore(key,member);
    }

    @Override
    public List<String> sort(String key) {
        return this.jedis.sort(key);
    }

    @Override
    public List<String> sort(String key, SortingParams sortingParameters) {
        return this.jedis.sort(key,sortingParameters);
    }

    @Override
    public Long zcount(String key, double min, double max) {
        return this.jedis.zcount(key,min,max);
    }

    @Override
    public Long zcount(String key, String min, String max) {
        return this.jedis.zcount(key,min,max);
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max) {
        return this.jedis.zrangeByScore(key,min,max);
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max) {
        return this.jedis.zrangeByScore(key,min,max);
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min) {
        return this.jedis.zrevrangeByScore(key,max,min);
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
        return this.jedis.zrangeByScore(key,min,max,offset,count);
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String max, String min) {
        return this.jedis.zrevrangeByScore(key,max,min);
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max, int offset, int count) {
        return this.jedis.zrangeByScore(key,min,max,offset,count);
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
        return this.jedis.zrevrangeByScore(key,max,min,offset,count);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
        return this.jedis.zrangeByScoreWithScores(key,min,max);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min) {
        return this.jedis.zrevrangeByScoreWithScores(key,max,min);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
        return this.jedis.zrangeByScoreWithScores(key,min,max,offset,count);
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String max, String min, int offset, int count) {
        return this.jedis.zrevrangeByScore(key,max,min,offset,count);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max) {
        return this.jedis.zrangeByScoreWithScores(key,min,max);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min) {
        return this.jedis.zrevrangeByScoreWithScores(key,max,min);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max, int offset, int count) {
        return this.jedis.zrangeByScoreWithScores(key,min,max,offset,count);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
        return this.jedis.zrevrangeByScoreWithScores(key,max,min,offset,count);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min, int offset, int count) {
        return this.jedis.zrevrangeByScoreWithScores(key,max,min,offset,count);
    }

    @Override
    public Long zremrangeByRank(String key, long start, long stop) {
        return this.jedis.zremrangeByRank(key,start,stop);
    }

    @Override
    public Long zremrangeByScore(String key, double min, double max) {
        return this.jedis.zremrangeByScore(key,min,max);
    }

    @Override
    public Long zremrangeByScore(String key, String min, String max) {
        return this.jedis.zremrangeByScore(key,min,max);
    }

    @Override
    public Long zlexcount(String key, String min, String max) {
        return this.jedis.zlexcount(key,min,max);
    }

    @Override
    public Set<String> zrangeByLex(String key, String min, String max) {
        return this.jedis.zrangeByLex(key,min,max);
    }

    @Override
    public Set<String> zrangeByLex(String key, String min, String max, int offset, int count) {
        return this.jedis.zrangeByLex(key,min,max,offset,count);
    }

    @Override
    public Set<String> zrevrangeByLex(String key, String max, String min) {
        return this.jedis.zrevrangeByLex(key,max,min);
    }

    @Override
    public Set<String> zrevrangeByLex(String key, String max, String min, int offset, int count) {
        return this.jedis.zrevrangeByLex(key,max,min,offset,count);
    }

    @Override
    public Long zremrangeByLex(String key, String min, String max) {
        return this.jedis.zremrangeByLex(key,min,max);
    }

    @Override
    public Long linsert(String key, ListPosition where, String pivot, String value) {
        return this.jedis.linsert(key,where,pivot,value);
    }

    @Override
    public Long lpushx(String key, String... string) {
        return this.jedis.lpushx(key,string);
    }

    @Override
    public Long rpushx(String key, String... string) {
        return this.jedis.rpushx(key,string);
    }

    @Override
    public List<String> blpop(int timeout, String key) {
        return this.jedis.blpop(timeout,key);
    }

    @Override
    public List<String> brpop(int timeout, String key) {
        return this.jedis.brpop(timeout,key);
    }

    @Override
    public Long del(String key) {
        return this.jedis.del(key);
    }

    @Override
    public Long unlink(String key) {
        return this.jedis.unlink(key);
    }

    @Override
    public String echo(String string) {
        return this.jedis.echo(string);
    }

    @Override
    public Long bitcount(String key) {
        return this.jedis.bitcount(key);
    }

    @Override
    public Long bitcount(String key, long start, long end) {
        return this.jedis.bitcount(key,start,end);
    }

    @Override
    public ScanResult<Map.Entry<String, String>> hscan(String key, String cursor) {
        return this.jedis.hscan(key,cursor);
    }

    @Override
    public ScanResult<String> sscan(String key, String cursor) {
        return this.jedis.sscan(key,cursor);
    }

    @Override
    public ScanResult<Tuple> zscan(String key, String cursor) {
        return this.jedis.zscan(key,cursor);
    }

    @Override
    public Long pfadd(String key, String... elements) {
        return this.jedis.pfadd(key,elements);
    }

    @Override
    public long pfcount(String key) {
        return this.jedis.pfcount(key);
    }

    @Override
    public Long geoadd(String key, double longitude, double latitude, String member) {
        return this.jedis.geoadd(key,longitude,latitude,member);
    }

    @Override
    public Long geoadd(String key, Map<String, GeoCoordinate> memberCoordinateMap) {
        return this.jedis.geoadd(key,memberCoordinateMap);
    }

    @Override
    public Double geodist(String key, String member1, String member2) {
        return this.jedis.geodist(key,member1,member2);
    }

    @Override
    public Double geodist(String key, String member1, String member2, GeoUnit unit) {
        return this.jedis.geodist(key,member1,member2,unit);
    }

    @Override
    public List<String> geohash(String key, String... members) {
        return this.jedis.geohash(key,members);
    }

    @Override
    public List<GeoCoordinate> geopos(String key, String... members) {
        return this.jedis.geopos(key,members);
    }

    @Override
    public List<GeoRadiusResponse> georadius(String key, double longitude, double latitude, double radius, GeoUnit unit) {
        return this.jedis.georadius(key,longitude,latitude,radius,unit);
    }

    @Override
    public List<GeoRadiusResponse> georadiusReadonly(String key, double longitude, double latitude, double radius, GeoUnit unit) {
        return this.jedis.georadiusReadonly(key,longitude,latitude,radius,unit);
    }

    @Override
    public List<GeoRadiusResponse> georadius(String key, double longitude, double latitude, double radius, GeoUnit unit, GeoRadiusParam param) {
        return this.jedis.georadius(key,longitude,latitude,radius,unit,param);
    }

    @Override
    public List<GeoRadiusResponse> georadiusReadonly(String key, double longitude, double latitude, double radius, GeoUnit unit, GeoRadiusParam param) {
        return this.jedis.georadiusReadonly(key,longitude,latitude,radius,unit,param);
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(String key, String member, double radius, GeoUnit unit) {
        return this.jedis.georadiusByMember(key,member,radius,unit);
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMemberReadonly(String key, String member, double radius, GeoUnit unit) {
        return this.jedis.georadiusByMemberReadonly(key,member,radius,unit);
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(String key, String member, double radius, GeoUnit unit, GeoRadiusParam param) {
        return this.jedis.georadiusByMember(key,member,radius,unit,param);
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMemberReadonly(String key, String member, double radius, GeoUnit unit, GeoRadiusParam param) {
        return this.jedis.georadiusByMemberReadonly(key,member,radius,unit,param);
    }
}
