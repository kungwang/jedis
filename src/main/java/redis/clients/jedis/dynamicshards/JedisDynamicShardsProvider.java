package redis.clients.jedis.dynamicshards;

import java.util.List;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;

/**
 * Created by kungwang on 12/25/19.
 */
public class JedisDynamicShardsProvider extends AbstractDynamicShardsProvider<Jedis, JedisShardInfo> {

    /**
     * Default constructor that initialize an empty list of shards / sharded.
     */
    public JedisDynamicShardsProvider() {
        super();
    }

    /**
     * Default constructor with initial shards list.
     * @param initialShards initial shards list
     */
    public JedisDynamicShardsProvider(final List<JedisShardInfo> initialShards) {
        super(initialShards);
    }

}
