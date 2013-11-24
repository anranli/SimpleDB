package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;


public class LockManager {
    ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, Permissions>> page_locks;
    ConcurrentHashMap<PageId, Permissions> page_lock_type; 
    ConcurrentHashMap<TransactionId, ArrayList<PageId>> tid_locks; 
    ConcurrentHashMap<TransactionId, PageId> waiting_on_locks;

    public LockManager() {
        //using hashmap because of the quick lookup
        this.page_locks = new ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, Permissions>>();
        //this is the data structure that we'll use heavily to check if the exclusive lock is gone, if the page doesn't have a lock, and so on
        this.page_lock_type = new ConcurrentHashMap<PageId, Permissions>();
        //makes exercise 4 easy
        this.tid_locks = new ConcurrentHashMap<TransactionId, ArrayList<PageId>>();
        this.waiting_on_locks = new ConcurrentHashMap<TransactionId, PageId>();
    }

    //only one thread at a time
    public synchronized void lock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        if (!(this.tid_locks.containsKey(tid))){
            this.tid_locks.put(tid, new ArrayList<PageId>());
        }
        if (this.getPageLockType(pid) == null) {
            this.grantNewPageLock(tid, pid, perm);
        }
        else {
            if (perm == Permissions.READ_ONLY) {
                if ((this.getPageLockType(pid) == Permissions.READ_ONLY) && !(this.page_locks.get(pid).containsKey(tid))) {
                    //check if its read only and we don't want to add in the same tid
                    this.page_locks.get(pid).put(tid, perm);
                    this.tid_locks.get(tid).add(pid);
                }
                else if ((this.getPageLockType(pid) == Permissions.READ_WRITE) && !(this.page_locks.get(pid).containsKey(tid))) {
                    //if page has write lock and the tid with lock isn't the same, then block until no exclusive lock and then create a new page lock
                    if (this.getPageLockType(pid) != null){ 
                    	this.block(tid, pid); 
                    }

                    //if the lock still isn't up, then throw an exception
                    if (this.getPageLockType(pid) == null){
                        this.grantNewPageLock(tid, pid, Permissions.READ_ONLY);
                    }
                    else { 
                        (Database.getBufferPool()).abort(tid);
                        throw new TransactionAbortedException(); 
                    }
                }
            }
            //perm == Permissions.READ_WRITE
            else { 
                if (this.page_locks.get(pid).containsKey(tid)) {
                    //if the locks on this page contains the key, then block until
                    if (this.page_locks.get(pid).entrySet().size() != 1){ 
                        this.block(tid, pid);
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
                    else { 
                        System.out.println((TransactionId) this.page_locks.get(pid).keySet().toArray()[0]);
                        (Database.getBufferPool()).abort(tid);
                        throw new TransactionAbortedException(); 
                    }
                }
                else {
                    //since the tid isn't part of current locks, we have to block until all locks are gone
                    if (this.getPageLockType(pid) != null){ this.block(tid, pid); }

                    if (this.getPageLockType(pid) == null){
                        this.grantNewPageLock(tid, pid, Permissions.READ_WRITE);
                    }
                    else { 
                        System.out.println((TransactionId) this.page_locks.get(pid).keySet().toArray()[0]);
                        (Database.getBufferPool()).abort(tid);
                        throw new TransactionAbortedException(); 
                    }
                }
            }
        }
    }

    public Permissions getPageLockType(PageId pid){
        if (page_lock_type.containsKey((PageId) pid)){ return page_lock_type.get((PageId) pid); }
        else { return null; }
    }

    public void removeLock(TransactionId tid, PageId pid){
        this.page_locks.get(pid).remove(tid);
        this.tid_locks.get(tid).remove(pid);
        if (this.page_locks.get(pid).entrySet().size() == 0){
            //nothing is locked
            this.page_lock_type.remove(pid);
            //remove the hash
            this.page_locks.remove(pid);
        }
    }

    public  void grantNewPageLock(TransactionId tid, PageId pid, Permissions perm){
        ConcurrentHashMap<TransactionId, Permissions> page_lock = new ConcurrentHashMap<TransactionId, Permissions>();
        page_lock.put(tid, perm);
        this.page_locks.put(pid, page_lock);
        this.page_lock_type.put(pid, perm);
        this.tid_locks.get(tid).add(pid);
    }

    public  Boolean holdsLock(TransactionId tid, PageId pid){
        if (this.getPageLockType(pid) != null){
            if (this.page_locks.get(pid).containsKey(tid)){ return true; }
            else{ return false; }
        }
        else { return false; }
    }

    public  void block(TransactionId tid, PageId pid) throws TransactionAbortedException {
        //try { Thread.sleep(100); }
        try {
        	//mark as 
        	this.waiting_on_locks.put(tid, pid);
            while (this.getPageLockType(pid) != null){
            	if (deadLock(tid, pid)) {
            		System.out.println("deadlocked");
            		throw new TransactionAbortedException();
            	}
            	System.out.println(tid + " " + pid + " sleeping");
            	Thread.sleep(100);
            }
        }
        catch (Exception e){ throw new TransactionAbortedException(); }
    }
    
    private boolean deadLock(TransactionId tid, PageId pid) {
    	if (this.getPageLockType(pid) != null){ 
	    	HashSet<PageId> set = new HashSet<PageId>();
	    	
	    	//add own locked pages to set
	    	for (PageId ownPageId : this.tid_locks.get(tid)) {
	    		set.add(ownPageId);
	    	}
	    	System.out.println("1. set: " + set);
	    	
	    	LinkedList<PageId> q = new LinkedList<PageId>();
	    	q.add(pid);
	    	set.add(pid);
	    	System.out.println("2. q: " + q);
	    	System.out.println("2. set: " + set);
	    	
	    	while (!q.isEmpty()) {
	    		PageId tempPid = q.remove();
	    		
	    		// find tid that is holding tempPid removed from q and set as tempTid
	    		Enumeration<TransactionId> e = this.page_locks.get(tempPid).keys();
		    	while (e.hasMoreElements()) {
		    		TransactionId tempTid = e.nextElement();
		    		System.out.println("3. tempTid: " + tempTid);
		    		
		    		// only look at READ_WRITE
		    		if (this.page_locks.get(tempPid).get(tempTid)) {
		    			
		    			System.out.println(this.waiting_on_locks);
		    			// if it isn't waiting on anything, there is no deadlock
		    			if (this.waiting_on_locks.get(tempTid) == null) {
		    				return false;
		    			}
		    			
		    			PageId nextPid = this.waiting_on_locks.get(tempTid);
		    			System.out.println("4. nextPid: " + nextPid);
		    			
		    			// if waiting for something that's locked, deadlock
		    			if (set.contains(nextPid)){
		    				return true;
		    			}
		    			q.add(nextPid);
		    			set.add(nextPid);
		    		}
		    	}
	    		
	    	}
    	}
    	return false;
    }

    public ArrayList<PageId> getPages(TransactionId tid){
        if (this.tid_locks.containsKey(tid)){
            return this.tid_locks.get(tid);
        }
        else {
            return null;
        }
    }
}