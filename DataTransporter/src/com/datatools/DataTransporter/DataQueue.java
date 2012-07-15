package com.datatools.DataTransporter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;


public class DataQueue implements Data {

	private final int MAXIMUM_PENDING_OFFERS = Integer.MAX_VALUE;
	private final BlockingQueue<List> dataQueue = new ArrayBlockingQueue<List>(
			100);
	private boolean isStopped = false;

	private Semaphore semaphore = new Semaphore(MAXIMUM_PENDING_OFFERS);
	private Object rawData;
	private final String connectionName;

	public DataQueue(String connectionName) {
		this.connectionName = connectionName;
	}


	public Object getRawData() {
		return rawData;
	}

	// return true if the task was successfully placed on the queue, false
	// if the queue has been shut down.

	public void addRecord(List record) {
		synchronized (this) {
			if (isStopped)
				return /* false */;
			if (!semaphore.tryAcquire())
				throw new Error("too many threads");
		}
		try {
			try {
				dataQueue.put(record);

			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} finally {
			semaphore.release();
		}
	}

	// return the head task from the queue, or null if no task is available

	public List getRecord() {
		try {
			return dataQueue.take();
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	// return the head task from the queue, or null if no task is available

	public List recoverRecords() {
		try {
			
			ArrayList recordsToBeRecovered=new ArrayList();
			
			while(dataQueue.peek()!=null)
			{
				recordsToBeRecovered.add(dataQueue.poll());
			}
			
			return recordsToBeRecovered;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	// stop the queue, wait for producers to finish, then return the contents
	public Collection<List> shutDown() {
		synchronized (this) {
			isStopped = true;
		}
		semaphore.acquireUninterruptibly(MAXIMUM_PENDING_OFFERS);
		Set<List> returnCollection = new HashSet<List>();
		dataQueue.drainTo(returnCollection);
		return returnCollection;
	}


	public int count() {
		return dataQueue.size();
	}


	public void setRawData(Object rawData) {
		this.rawData = rawData;
	}

}