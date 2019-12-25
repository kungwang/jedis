package redis.clients.jedis.dynamicshards;

import java.util.List;
import redis.clients.jedis.util.ShardInfo;
import redis.clients.jedis.util.Sharded;

/**
 * Created by kungwang on 12/25/19.
 */
public interface DynamicShardsProvider<R, S extends ShardInfo<R>> {

    List<S> getShards();

    void setShards(final List<S> shards);

    void addShard(final S newShard);

    void removeShard(final S oldShard);

    void register(final Sharded<R, S> sharded);

    void unregister(final Sharded<R, S> sharded);

    void unregisterAll();

    void notifyShardeds();


}
