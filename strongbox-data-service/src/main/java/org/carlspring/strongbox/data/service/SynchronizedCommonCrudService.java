package org.carlspring.strongbox.data.service;

import org.carlspring.strongbox.data.domain.GenericEntity;

import javax.inject.Inject;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import ca.thoughtwire.lock.DistributedLockService;
import com.hazelcast.core.HazelcastInstance;
import com.orientechnologies.orient.core.id.ORID;
import org.springframework.beans.factory.DisposableBean;

public abstract class SynchronizedCommonCrudService<T extends GenericEntity>
        extends CommonCrudService<T>
        implements DisposableBean
{

    private DistributedLockService lockService;

    @Inject
    protected void setHazelcastInstance(HazelcastInstance hazelcastInstance)
    {
        lockService = DistributedLockService.newHazelcastLockService(hazelcastInstance);
    }

    @Override
    protected <S extends T> S cascadeEntitySave(T entity)
    {
        Lock lock = acquireWriteLock(entity);
        try
        {
            return super.cascadeEntitySave(entity);
        }
        finally
        {
            logger.info("### Releasing write lock [{}] [{}]", getReentrantReadWriteLockKey(entity), entity.getClass());
            lock.unlock();
            logger.info("### Released write lock [{}] [{}]", getReentrantReadWriteLockKey(entity), entity.getClass());
        }
    }

    @Override
    protected boolean identifyEntity(T entity)
    {
        if (super.identifyEntity(entity))
        {
            return true;
        }

        ORID objectId;

        Lock lock = acquireReadLock(entity);
        try
        {
            objectId = findId(entity);
        }
        finally
        {
            logger.info("### Releasing read lock [{}] [{}]", getReentrantReadWriteLockKey(entity), entity.getClass());
            lock.unlock();
            logger.info("### Released read lock [{}] [{}]", getReentrantReadWriteLockKey(entity), entity.getClass());
        }

        if (objectId == null)
        {
            return false;
        }

        entity.setObjectId(objectId.toString());

        return true;
    }

    @Override
    public void destroy()
    {
        lockService.shutdown();
    }

    private Lock acquireWriteLock(T entity)
    {
        String key = getReentrantReadWriteLockKey(entity);
        ReadWriteLock readWriteLock = lockService.getReentrantReadWriteLock(key);
        Lock lock = readWriteLock.writeLock();
        logger.info("### Acquiring write lock [{}] [{}]", key, entity.getClass());
        lock.lock();
        logger.info("### Acquired write lock [{}] [{}]", key, entity.getClass());
        return lock;
    }

    private Lock acquireReadLock(T entity)
    {
        String key = getReentrantReadWriteLockKey(entity);
        ReadWriteLock readWriteLock = lockService.getReentrantReadWriteLock(key);
        Lock lock = readWriteLock.readLock();
        logger.info("### Acquiring read lock [{}] [{}]", key, entity.getClass());
        lock.lock();
        logger.info("### Acquired read lock [{}] [{}]", key, entity.getClass());
        return lock;
    }

    protected abstract ORID findId(T entity);

    protected abstract String getReentrantReadWriteLockKey(T entity);
}
