package redis.clients.jedis;

import java.util.List;
import java.util.regex.Pattern;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.dynamicshards.JedisDynamicShardsProvider;
import redis.clients.jedis.util.Hashing;
import redis.clients.jedis.util.Pool;

public class ShardedJedisPool extends Pool<ShardedJedis> {

    public ShardedJedisPool(final GenericObjectPoolConfig poolConfig,
        final JedisDynamicShardsProvider shardProvider) {
        this(poolConfig, shardProvider, Hashing.MURMUR_HASH, null);
    }

    public ShardedJedisPool(final GenericObjectPoolConfig poolConfig, List<JedisShardInfo> shards) {
        this(poolConfig, shards, Hashing.MURMUR_HASH);
    }

    public ShardedJedisPool (final GenericObjectPoolConfig poolConfig,
                             final JedisDynamicShardsProvider shardProvider,
                             final Pattern keyTagPattern) {
        this(poolConfig, shardProvider, Hashing.MURMUR_HASH, keyTagPattern);
    }

    public ShardedJedisPool(final GenericObjectPoolConfig poolConfig, List<JedisShardInfo> shards,
        Hashing algo) {
        this(poolConfig, shards, algo, null);
    }

    public ShardedJedisPool(final GenericObjectPoolConfig poolConfig, List<JedisShardInfo> shards,
        Pattern keyTagPattern) {
        this(poolConfig, shards, Hashing.MURMUR_HASH, keyTagPattern);
    }

    public ShardedJedisPool (final GenericObjectPoolConfig poolConfig,
                             final JedisDynamicShardsProvider shardProvider,
                             final Hashing algo,
                             final Pattern keyTagPattern) {
        super(poolConfig, new ShardedJedisFactory(shardProvider, algo, keyTagPattern));
    }

    public ShardedJedisPool(final GenericObjectPoolConfig poolConfig, List<JedisShardInfo> shards,
        Hashing algo, Pattern keyTagPattern) {
        super(poolConfig, new ShardedJedisFactory(shards, algo, keyTagPattern));
    }

    @Override
    public ShardedJedis getResource() {
        ShardedJedis jedis = super.getResource();
        jedis.setDataSource(this);
        return jedis;
    }

    @Override
    protected void returnBrokenResource(final ShardedJedis resource) {
        if (resource != null) {
            returnBrokenResourceObject(resource);
        }
    }

    @Override
    protected void returnResource(final ShardedJedis resource) {
        if (resource != null) {
            resource.resetState();
            returnResourceObject(resource);
        }
    }

    /**
     * PoolableObjectFactory custom impl.
     */
    private static class ShardedJedisFactory implements PooledObjectFactory<ShardedJedis> {

        private List<JedisShardInfo> shards;
        private Hashing algo;
        private Pattern keyTagPattern;

        private final JedisDynamicShardsProvider provider;
        private final boolean isDynamic;

        public ShardedJedisFactory(final JedisDynamicShardsProvider provider,
                                   final Hashing algo,
                                   final Pattern keyTagPattern) {
            this.isDynamic = true;
            this.algo = algo;
            this.provider = provider;
            this.shards = null;
            this.keyTagPattern = keyTagPattern;
        }

        public ShardedJedisFactory(List<JedisShardInfo> shards, Hashing algo,
            Pattern keyTagPattern) {
            this.isDynamic = false;
            this.provider = null;
            this.shards = shards;
            this.algo = algo;
            this.keyTagPattern = keyTagPattern;
        }

        @Override
        public PooledObject<ShardedJedis> makeObject() throws Exception {
            if(isDynamic) {
                ShardedJedis jedis = new ShardedJedis(provider, algo, keyTagPattern);
                return new DefaultPooledObject<ShardedJedis>(jedis);
            } else {
                ShardedJedis jedis = new ShardedJedis(shards, algo, keyTagPattern);
                return new DefaultPooledObject<ShardedJedis>(jedis);
            }
        }

        @Override
        public void destroyObject(PooledObject<ShardedJedis> pooledShardedJedis) throws Exception {
            final ShardedJedis shardedJedis = pooledShardedJedis.getObject();
            if(isDynamic) {
                provider.unregister(shardedJedis);
            }
            for (Jedis jedis : shardedJedis.getAllShards()) {
                if (jedis.isConnected()) {
                    try {
                        try {
                            jedis.quit();
                        } catch (Exception e) {

                        }
                        jedis.disconnect();
                    } catch (Exception e) {

                    }
                }
            }
        }

        @Override
        public boolean validateObject(PooledObject<ShardedJedis> pooledShardedJedis) {
            try {
                ShardedJedis jedis = pooledShardedJedis.getObject();
                for (Jedis shard : jedis.getAllShards()) {
                    if (!shard.ping().equals("PONG")) {
                        return false;
                    }
                }
                return true;
            } catch (Exception ex) {
                return false;
            }
        }

        @Override
        public void activateObject(PooledObject<ShardedJedis> p) throws Exception {

        }

        @Override
        public void passivateObject(PooledObject<ShardedJedis> p) throws Exception {

        }
    }
}