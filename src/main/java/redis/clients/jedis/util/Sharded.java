package redis.clients.jedis.util;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import redis.clients.jedis.dynamicshards.AbstractDynamicShardsProvider;

public class Sharded<R, S extends ShardInfo<R>> {

    public static final int DEFAULT_WEIGHT = 1;
    private TreeMap<Long, S> nodes;
    private final Hashing algo;
    private final Map<ShardInfo<R>, R> resources = new LinkedHashMap<ShardInfo<R>, R>();

    private final boolean useProvider;
    private final ReentrantReadWriteLock rwlock = new ReentrantReadWriteLock();
    private final Lock readLock = rwlock.readLock();
    private final Lock writeLock = rwlock.writeLock();

    /**
     * The default pattern used for extracting a key tag. The pattern must have a group (between
     * parenthesis), which delimits the tag to be hashed. A null pattern avoids applying the regular
     * expression for each lookup, improving performance a little bit is key tags aren't being
     * used.
     */
    private Pattern tagPattern = null;
    // the tag is anything between {}
    public static final Pattern DEFAULT_KEY_TAG_PATTERN = Pattern.compile("\\{(.+?)\\}");


    public Sharded(final AbstractDynamicShardsProvider<R, S> provider) {
        this(provider, Hashing.MURMUR_HASH);
    }

    public Sharded(List<S> shards) {
        this(shards, Hashing.MURMUR_HASH); // MD5 is really not good as we works
        // with 64-bits not 128
    }

    /**
     * Constructor that depends on a shard provider.
     *
     * @param provider provider the shard provider
     * @param algo algo key hashing algorithm to be used
     */
    public Sharded(final AbstractDynamicShardsProvider<R, S> provider, final Hashing algo) {
        this(provider, algo, null);
    }

    public Sharded(List<S> shards, Hashing algo) {
        this.useProvider = false;
        this.algo = algo;
        initialize(shards);
    }

    /**
     * Constructor that depends on a shard provider.
     *
     * @param provider provider the shard provider
     * @param tagPattern agPattern pattern to be used for key hashing
     */
    public Sharded(final AbstractDynamicShardsProvider<R, S> provider, final Pattern tagPattern) {
        this(provider, Hashing.MURMUR_HASH, tagPattern);
    }

    public Sharded(List<S> shards, Pattern tagPattern) {
        this(shards, Hashing.MURMUR_HASH, tagPattern); // MD5 is really not good
        // as we works with
        // 64-bits not 128
    }

    /**
     * Constructor that depends on a shard provider.
     *
     * @param provider provider the shard provider
     * @param algo algo key hashing algorithm to be used
     * @param tagPattern tagPattern pattern to be used for key hashing
     */
    public Sharded(final AbstractDynamicShardsProvider<R, S> provider,
        final Hashing algo, final Pattern tagPattern) {
        this.useProvider = true;
        this.algo = algo;
        this.tagPattern = tagPattern;
        provider.register(this);
        initialize(provider.getShards());
    }

    public Sharded(List<S> shards, Hashing algo, Pattern tagPattern) {
        this.useProvider = false;
        this.algo = algo;
        this.tagPattern = tagPattern;
        initialize(shards);
    }

    private void initialize(List<S> shards) {
        // TODO: make sure all nodes and resources are not leaking
        if (nodes != null) {
            nodes.clear();
        }
        if (resources != null) {
            resources.clear();
        }

        nodes = new TreeMap<Long, S>();
        for (int i = 0; i != shards.size(); ++i) {
            final S shardInfo = shards.get(i);
            if (shardInfo.getName() == null) {
                for (int n = 0; n < 160 * shardInfo.getWeight(); n++) {
                    nodes.put(this.algo.hash("SHARD-" + i + "-NODE-" + n), shardInfo);
                }
            } else {
                for (int n = 0; n < 160 * shardInfo.getWeight(); n++) {
                    nodes.put(this.algo.hash(shardInfo.getName() + "*" + n), shardInfo);
                }
            }
            resources.put(shardInfo, shardInfo.createResource());
        }
    }

    public R getShard(byte[] key) {
        if(useProvider) {
            readLock.lock();
        }
        try {
            return resources.get(getShardInfo(key));
        } finally {
            if(useProvider) {
                readLock.unlock();
            }
        }
    }

    public R getShard(String key) {
        if(useProvider) {
            readLock.lock();
        }
        try {
            return resources.get(getShardInfo(key));
        } finally {
            if(useProvider) {
                readLock.unlock();
            }
        }
    }

    public S getShardInfo(byte[] key) {
        if(useProvider) {
            readLock.lock();
        }
        try {
            SortedMap<Long, S> tail = nodes.tailMap(algo.hash(key));
            if (tail.isEmpty()) {
                return nodes.get(nodes.firstKey());
            }
            return tail.get(tail.firstKey());
        } finally {
            if(useProvider) {
                readLock.unlock();
            }
        }
    }

    public S getShardInfo(String key) {
        return getShardInfo(SafeEncoder.encode(getKeyTag(key)));
    }

    /**
     * A key tag is a special pattern inside a key that, if preset, is the only part of the key
     * hashed in order to select the server for this key.
     *
     * @return The tag if it exists, or the original key
     * @see <a href="http://redis.io/topics/partitioning">partitioning</a>
     */
    public String getKeyTag(String key) {
        if (tagPattern != null) {
            Matcher m = tagPattern.matcher(key);
            if (m.find()) {
                return m.group(1);
            }
        }
        return key;
    }

    public Collection<S> getAllShardInfo() {
        if(useProvider) {
            readLock.lock();
        }
        try {
            return Collections.unmodifiableCollection(nodes.values());
        } finally {
            if(useProvider) {
                readLock.unlock();
            }
        }
    }

    public Collection<R> getAllShards() {
        if(useProvider) {
            readLock.lock();
        }
        try {
            return Collections.unmodifiableCollection(resources.values());
        } finally {
            if (useProvider) {
                readLock.unlock();
            }
        }
    }

    public void dynamicUpdate(final AbstractDynamicShardsProvider<R, S> provider) {
        if (useProvider) {
            writeLock.lock();
            try {
                initialize(provider.getShards());
            } finally {
                writeLock.unlock();
            }
        }
    }

}
