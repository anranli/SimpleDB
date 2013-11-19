package simpledb;

import java.io.*;
import java.util.*;

ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, Boolean>> page_locks;
ConcurrentHashMap<PageId, Permissions> page_lock_type;  

public class LockManager {
    public LockManager() {
        // some code goes here
        //boolean isn't needed, but make sure to delete shit from this table if it no longer exists instead of setting it false
        //using hashmap because of the quick lookup
        this.page_locks = new ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, Boolean>>();
        //this is the data structure that we'll use heavily to check if the exclusive lock is gone, if the page doesn't have a lock, and so on
        this.page_lock_type = new ConcurrentHashMap<PageId, Permissions>();
    }

    //only one thread at a time
    public synchronized void lock(TransactionId tid, PageId pid, Permissions perm){
        if (this.getPageLockType(pid) != null){
            //the 1st of 3 cases where you can get an exclusive lock
            if ((this.page_locks.get(pid).entrySet().size() == 1) && (this.page_locks.get(pid).containsKey(tid))) {
                //locks don't go from write to read, so if it's already READ_WRITE, no need to change
                if (this.getPageLockType(pid) != Permissions.READ_WRITE){
                    //upgrade if possible
                    this.page_lock_type.put(pid, perm);
                } 
            }
            //if shared lock, go ahead and grant access
            else if (perm == Permissions.READ_ONLY) {
                if ((this.getPageLockType(pid) == Permissions.READ_ONLY) && !(this.page_locks.get(pid).containsKey(tid))) {
                    this.page_locks.get(pid).put(tid, true);
                }
                else if (this.getPageLockType(pid) == Permissions.READ_WRITE) {
                    while (this.page_lock_type(pid) != null){

                }
                ConcurrentHashMap<TransactionId, Boolean> page_lock = new ConcurrentHashMap<TransactionId, Boolean>();
                page_lock.put(tid, true);
                page_locks.put(pid, page_lock);
                page_lock_type.put(pid, Permissions.READ_ONLY);

            }
            //the 3rd of 3 cases where you can get an exclusive lock
            else {
                while (this.page_lock_type(pid) != null){

                }
                ConcurrentHashMap<TransactionId, Boolean> page_lock = new ConcurrentHashMap<TransactionId, Boolean>();
                page_lock.put(tid, true);
                page_locks.put(pid, page_lock);
                page_lock_type.put(pid, Permissions.READ_WRITE);
            }
        }
        else {
            //the 2nd of 3 cases where you can potentially get an exclusive lock
            ConcurrentHashMap<TransactionId, Boolean> page_lock = new ConcurrentHashMap<TransactionId, Boolean>();
            page_lock.put(tid, true);
            page_locks.put(pid, page_lock);
            page_lock_type.put(pid, perm);
        }
    }

    public synchronized Permissions getPageLockType(PageId pid){
        if page_lock_type.containsKey((PageId) pid){
            return page_lock_type.get((PageId) pid);
        }
        else {
            return null;
        }
    }

    public synchronized Boolean canAccessFile(TransactionId tid, PageId pid){
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