package com.fast.pool.ftp;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;


public abstract class Pool<T> {
	private final GenericObjectPool<T> internalPool;
	
	public Pool(GenericObjectPool.Config poolConfig, PoolableObjectFactory factory)  
	{
	    this.internalPool = new GenericObjectPool<T>(factory, poolConfig);  
	}
	
	public T getResource(){
		try{
			return this.internalPool.borrowObject();
		}catch(Exception e){
			e.printStackTrace();
			return null;
		}
	}
	
	public void returnResource(T resource){
	    try {
	    	this.internalPool.returnObject(resource);
	    }catch (Exception e) {
	    	e.printStackTrace();
	    }
	}
	
	public void destroy(){
	    try{
	    	this.internalPool.close();  
	    }catch (Exception e) {
	    	e.printStackTrace();
	    }
	}
	
	public int inPoolSize(){
		try{
			return this.internalPool.getNumIdle();
		}catch(Exception e){
			e.printStackTrace();
			return 0;
		}
	}
	
	public int borrowSize(){
		try{
			return this.internalPool.getNumActive();
		}catch(Exception e){
			e.printStackTrace();
			return 0;
		}
	}
}