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
            if (perm == Permissions.READ_ONLY) {
                if ((this.getPageLockType(pid) == Permissions.READ_ONLY) && !(this.page_locks.get(pid).containsKey(tid))) {
                    //check if its read only and we don't want to add in the same tid
                    this.page_locks.get(pid).put(tid, true);
                }
                else if ((this.getPageLockType(pid) == Permissions.READ_WRITE) && !(this.page_locks.get(pid).containsKey(tid))) {
                    //if page has write lock and the tid with lock isn't the same, then block until no exclusive lock and then create a new page lock
                    if (this.getPageLockType(pid) != null){ this.block(); }

                    //if the lock still isn't up, then throw an exception
                    if (this.getPageLockType(pid) == null){
                        this.grantNewPageLock(tid, pid, Permissions.READ_ONLY);
                    }
                    else { throw new TransactionAbortedException(); }
                }
            }
            else {
                if (this.page_locks.get(pid).containsKey(tid)) {
                    //if the locks on this page contains the key, then block until
                    if (this.page_locks.get(pid).entrySet().size() != 1){ 
                        this.block();
                    }

                    //upgrade condition
                    if ((this.page_locks.get(pid).entrySet().size() == 1) && (this.page_locks.get(pid).containsKey(tid))) {
                        //if there is only one lock on it and tid is the one we care about
                        this.page_lock_type.remove(pid);
                        this.page_lock_type.put(pid, Permissions.READ_WRITE); 
                    }
                    else if (this.getPageLockType(pid) == null){ 
                        //see if the lock is gone          
                        this.grantNewPageLock(tid, pid, Permissions.READ_WRITE);
                    }
                    else { throw new TransactionAbortedException(); }
                }
                else {
                    //since the tid isn't part of current locks, we have to block until all locks are gone
                    if (this.getPageLockType(pid) != null){ this.block(); }

                    if (this.getPageLockType(pid) == null){
                        this.grantNewPageLock(tid, pid, Permissions.READ_WRITE);
                    }
                    else { throw new TransactionAbortedException(); }
                }
            }
        }
        else {
            this.grantNewPageLock(tid, pid, perm);
        }
    }

    public synchronized Permissions getPageLockType(PageId pid){
        if (page_lock_type.containsKey((PageId) pid)){ return page_lock_type.get((PageId) pid); }
        else { return null; }
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
            if (this.page_locks.get(pid).containsKey(tid)){ return true; }
            else{ return false; }
        }
        else { return false; }
    }
    public synchronized void block() throws TransactionAbortedException {
        try { Thread.sleep(100); }
        catch (Exception e){ throw new TransactionAbortedException(); }
    }
}