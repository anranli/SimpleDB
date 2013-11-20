package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;


public class LockManager {
    ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, Boolean>> page_locks;
    ConcurrentHashMap<PageId, Permissions> page_lock_type;  

    public LockManager() {
        // some code goes here
        //boolean isn't needed, but make sure to delete shit from this table if it no longer exists instead of setting it false
        //using hashmap because of the quick lookup
        this.page_locks = new ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, Boolean>>();
        //this is the data structure that we'll use heavily to check if the exclusive lock is gone, if the page doesn't have a lock, and so on
        this.page_lock_type = new ConcurrentHashMap<PageId, Permissions>();
    }

    //only one thread at a time
    public synchronized void lock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        if (this.getPageLockType(pid) != null){
            //if shared lock, go ahead and grant access
            if (perm == Permissions.READ_ONLY) {
                //check if its read only and we don't want to add in the same tid
                if ((this.getPageLockType(pid) == Permissions.READ_ONLY) && !(this.page_locks.get(pid).containsKey(tid))) {
                    this.page_locks.get(pid).put(tid, true);
                }
                
                else if ((this.getPageLockType(pid) == Permissions.READ_WRITE) && !(this.page_locks.get(pid).containsKey(tid))) {
                    //if its write, then wait until no exclusive lock and then create a new page lock
                    if (this.getPageLockType(pid) != null){
                        try {
                            Thread.sleep(100);
                        }
                        catch (Exception e){
                            throw new TransactionAbortedException();
                        }
                    }
                    if (this.getPageLockType(pid) != null){
                        throw new TransactionAbortedException();
                    }
                    else {
                        this.grantNewPageLock(tid, pid, Permissions.READ_ONLY);
                    }
                }
            }
            else {
                //wait until either no locks exist for the page or if the only page left is the one we care about (upgrade) 

                if ((this.page_locks.get(pid).entrySet().size() == 1) && (this.page_locks.get(pid).containsKey(tid))) {
                    this.page_lock_type.remove(pid);
                    this.page_lock_type.put(pid, Permissions.READ_WRITE);
                }
                else if (this.page_locks.get(pid).containsKey(tid)) {
                    try{
                        Thread.sleep(100);
                    }
                    catch (Exception e){
                        throw new TransactionAbortedException();
                    }
                    //see if the lock is gone
                    if (this.page_locks.get(pid).entrySet().size() == 1){
                        this.page_lock_type.remove(pid);
                        this.page_lock_type.put(pid, Permissions.READ_WRITE); 
                    }
                    else {
                        throw new TransactionAbortedException();
                    }
                }
                else {
                    if (this.getPageLockType(pid) != null){
                        try {
                            Thread.sleep(100);
                        }
                        catch (Exception e){
                            throw new TransactionAbortedException();
                        }
                    }

                    if (this.getPageLockType(pid) == null){
                        this.grantNewPageLock(tid, pid, Permissions.READ_WRITE);
                    }
                    else {
                        throw new TransactionAbortedException();
                    }
                }
            }
        }
        else {
            this.grantNewPageLock(tid, pid, perm);
        }
    }

    public synchronized Permissions getPageLockType(PageId pid){
        if (page_lock_type.containsKey((PageId) pid)){
            return page_lock_type.get((PageId) pid);
        }
        else {
            return null;
        }
    }

    public synchronized void removeLock(TransactionId tid, PageId pid){
        this.page_locks.get(pid).remove(tid);
        if (this.page_locks.get(pid).entrySet().size() == 0){
            //nothing is locked
            this.page_lock_type.remove(pid);
            //remove the hash
            this.page_locks.remove(pid);
        }
    }

    public synchronized void grantNewPageLock(TransactionId tid, PageId pid, Permissions perm){
        ConcurrentHashMap<TransactionId, Boolean> page_lock = new ConcurrentHashMap<TransactionId, Boolean>();
        page_lock.put(tid, true);
        this.page_locks.put(pid, page_lock);
        this.page_lock_type.put(pid, perm);
    }

    public synchronized Boolean holdsLock(TransactionId tid, PageId pid){
        if (this.getPageLockType(pid) != null){
            if (this.page_locks.get(pid).containsKey(tid)){
                return true;
            }
            else{
                return false;
            }
        }
        else {
            return false;
        }
    }
}