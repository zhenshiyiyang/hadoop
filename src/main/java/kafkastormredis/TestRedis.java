package kafkastormredis;

/**
 * Created by hadoop on 2018/3/29.
 */
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.Map;
public class TestRedis {
    public static void main(String[] args) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        // 最大连接数
        poolConfig.setMaxTotal(1);
        // 最大空闲数
        poolConfig.setMaxIdle(1);
        // 最大允许等待时间，如果超过这个时间还未获取到连接，则会报JedisException异常：
        // Could not get a resource from the pool
        poolConfig.setMaxWaitMillis(1000);
        Set<HostAndPort> nodes = new LinkedHashSet<HostAndPort>();
        nodes.add(new HostAndPort("192.168.217.128", 7001));
        nodes.add(new HostAndPort("192.168.217.128", 7002));
        nodes.add(new HostAndPort("192.168.217.128", 7003));
        nodes.add(new HostAndPort("192.168.217.128", 7004));
        nodes.add(new HostAndPort("192.168.217.128", 7005));
        nodes.add(new HostAndPort("192.168.217.128", 7006));
        //JedisCluster jedis = new JedisCluster(nodes, poolConfig);
        Jedis jedis = new Jedis("192.168.217.128",7006);
//        Map<String,String> wordCountMap = new HashMap<String,String>();
//        wordCountMap.put("liuxin","2");
//        wordCountMap.put("book","5");
//        jedis.hmset("word",wordCountMap);
        Map<String,String> wordcount = jedis.hgetAll("wordcount");
        System.out.println(wordcount);
    }

}
