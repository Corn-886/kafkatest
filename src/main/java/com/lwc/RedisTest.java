package com.lwc;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2017/4/21.
 */
public class RedisTest {
    public static void main(String[] args) {
        HashSet redisClusterNodes =new HashSet<HostAndPort>();
        redisClusterNodes.add(new HostAndPort("192.168.100.100",6379));
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxTotal(100);
        poolConfig.setMinIdle(20);
        poolConfig.setMaxIdle(80);
        poolConfig.setMaxWaitMillis(60 * 1000);
        JedisCluster jedis = new JedisCluster(redisClusterNodes, 5000, 50, poolConfig);
        Map<String,JedisPool> clusterNodes = jedis.getClusterNodes();
        for(String k : clusterNodes.keySet()){
            Jedis connection = clusterNodes.get(k).getResource();
            String nodesStr=connection.clusterNodes();
            for (String node:nodesStr.split("\n")){
                if (node.contains("myself")&&node.contains("master")){
                    System.out.println(node);
                    String cursor="0";
                    List<String> result;
                    do{
                        ScanResult<String> kk=connection.scan(cursor,new ScanParams().match("DAY*").count(5000));
                        cursor=kk.getStringCursor();
                        result=kk.getResult();
                        for (String key:result){
                            System.out.println(key);
                        }

                    }while (!cursor.equals("0"));


                    //ScanResult<Map.Entry<String, String>> result = connection.hscan("STA",String.valueOf(cursor));

                }
            }
            System.out.println("--------------");
        }
    }
}
