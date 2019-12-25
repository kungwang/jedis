package redis.clients.jedis.dynamicshards;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import redis.clients.jedis.util.ShardInfo;
import redis.clients.jedis.util.Sharded;

/**
 * Created by kungwang on 12/25/19.
 */
public abstract class AbstractDynamicShardsProvider<R, S extends ShardInfo<R>>
    implements DynamicShardsProvider<R, S> {

    private final ArrayList<Sharded<R, S>> shardeds;

    // shard lists and their locks
    private final List<S> shards;
    private final Lock readLock;
    private final Lock writeLock;
    private boolean changed = false;

    /**
     * Default constructor that initialize an empty list of shards / sharded
     */
    public AbstractDynamicShardsProvider() {
        this(null);
    }

    /**
     * Default constructor with initial shards list.
     * @param initialShards initial shards list
     */
    public AbstractDynamicShardsProvider(final List<S> initialShards) {
        super();
        if (null != initialShards && !initialShards.isEmpty()) {
            this.shards = new ArrayList<S>(initialShards.size());
            this.shards.addAll(initialShards);
        } else {
            this.shards = new ArrayList<S>(0);
        }
        // We're not expecting a lot of dynamic Sharded waiting for updates ...
        // So the initial size for those is set quite low
        this.shardeds = new ArrayList<Sharded<R,S>>(3);

        // Setup the locks
        ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
        readLock = rwLock.readLock();
        writeLock = rwLock.writeLock();
    }

    /**
     * Gets all the shards currently available and usable.
     * @return all the shards currently available and usable.
     */
    @Override
    public List<S> getShards() {
        readLock.lock();
        try {
            return shards;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * remove all shards
     */
    protected void clearShardsList() {
        writeLock.lock();
        try {
            this.shards.clear();
            this.changed = true;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * clear existing shards and add all shards from provided shard list
     * @param shardsList new list of shards
     */
    protected void setShardsList(List<S> shardsList) {
        writeLock.lock();
        try {
            this.shards.clear();
            this.shards.addAll(shardsList);
            this.changed = true;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * add list of shards to shard list
     * @param shards list of shards to add
     */
    @Override
    public void setShards(List<S> shards) {
        final int currentSize;

        readLock.lock();
        try {
            currentSize = this.shards.size();
            // Special use case :
            // The current shards list & the new one contains the same shards
            // ==> do nothing
            if (null != shards) {
                // TODO: list scanning can take time if we have a lot of shards
                if (currentSize == shards.size() && this.shards.containsAll(shards)) {
                    return;
                }
            }
        } finally {
            readLock.unlock();
        }

        // Special use case :
        // - current shards list not empty
        // - new shards list null or empty
        if (0 != currentSize && (null == shards || shards.isEmpty())) {
            clearShardsList();
        }

        // Default use case, the shards are not the same ...
        if (null != shards) {
            setShardsList(shards);
        }

        notifyShardeds();
    }

    /**
     * add a shard to shard list
     * @param newShard shard to add to list
     */
    @Override
    public void addShard(S newShard) {
        if(null == newShard) {
            return;
        }

        // Do nothing if the new shard is already present ...
        readLock.lock();
        try {
            // TODO: list scanning can take time if we have a lot of shards
            if (this.shards.contains(newShard)) {
                return;
            }
        } finally {
            readLock.unlock();
        }

        // Add the new shard ...
        writeLock.lock();
        try {
            this.shards.add(newShard);
            this.changed = true;
        } finally {
            writeLock.unlock();
        }

        notifyShardeds();
    }

    /**
     * remove existing shard from shard list
     * @param oldShard shard to remove
     */
    @Override
    public void removeShard(S oldShard) {
        if(null == oldShard) {
            return;
        }

        // Do nothing if the new shard is not present ...
        readLock.lock();
        try {
            if (!this.shards.contains(oldShard)) {
                return;
            }
        } finally {
            readLock.unlock();
        }

        // Remove the shard ...
        writeLock.lock();
        try {
            this.shards.remove(oldShard);
            this.changed = true;
        } finally {
            writeLock.unlock();
        }

        notifyShardeds();
    }

    /**
     * Register a Sharded to be notified when the shards are updated.
     * @param sharded sharded a Sharded to be notified when the shards are updated.
     */
    @Override
    public synchronized void register(Sharded<R, S> sharded) {
        if(null != sharded) {
            if(!shardeds.contains(sharded)) {
                shardeds.add(sharded);
            }
        }
    }

    @Override
    public synchronized void unregister(Sharded<R, S> sharded) {
        if(null != sharded) {
            shardeds.remove(sharded);
        }
    }

    @Override
    public synchronized void unregisterAll() {
        shardeds.clear();
    }

    /**
     * Notify the registered Shardeds that the shards have been updated.
     */
    @SuppressWarnings("unchecked")
    @Override
    public void notifyShardeds() {
        Object[] arrLocal;
        synchronized (this) {
            if (!changed) {
                return;
            } else {
                arrLocal = shardeds.toArray();
                changed = false;
            }

            for (int i = arrLocal.length-1; i>=0; i--) {
                ((Sharded<R, S>)arrLocal[i]).dynamicUpdate(this);
            }
        }
    }
}
