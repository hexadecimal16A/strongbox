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
    void setHazelcastInstance(HazelcastInstance hazelcastInstance)
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
            lock.unlock();
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
            lock.unlock();
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
        ReadWriteLock readWriteLock = lockService.getReentrantReadWriteLock(getReentrantReadWriteLockKey(entity));
        Lock lock = readWriteLock.writeLock();
        lock.lock();
        return lock;
    }

    private Lock acquireReadLock(T entity)
    {
        ReadWriteLock readWriteLock = lockService.getReentrantReadWriteLock(getReentrantReadWriteLockKey(entity));
        Lock lock = readWriteLock.readLock();
        lock.lock();
        return lock;
    }

    protected abstract ORID findId(T entity);

    protected abstract String getReentrantReadWriteLockKey(T entity);
}
