package com.ald.stat.cache;

import redis.clients.jedis.Connection;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.commands.ProtocolCommand;

/**
 * Created by zhaofw on 2018/7/30.
 */
public class RedisCommandConn extends Connection {
    public RedisCommandConn(){
        super("10.0.220.11",6379);
    }

    public RedisCommandConn(String host, int port){
        super(host,port);
    }

    @Override
    public void sendCommand(ProtocolCommand cmd, String... args) {
        super.sendCommand(cmd, args);
    }

}
