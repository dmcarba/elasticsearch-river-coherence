package org.elasticsearch.plugin.river.coherence;

import java.util.Iterator;

import com.tangosol.net.NamedCache;
import com.tangosol.util.Filter;
import com.tangosol.util.MapEvent;
import com.tangosol.util.MapListener;
import com.tangosol.util.filter.LimitFilter;

public class CoherenceSynchronizer extends KeyValueStoreSynchronizer<Object, Object>
{
	private NamedCache cache;
	private Filter filter;
	
	public CoherenceSynchronizer(NamedCache cache, Filter filter, int batchSize)
	{
		super(batchSize);
		this.cache = cache;
		this.filter = filter;
	}
	
	@Override
	public Iterator<Object> initialKeySetIterator()
	{
		return new KeyIterator(filter);
	}

	@Override
	public void registerKeyListener(final KeyValueStoreSynchronizer<Object, Object>.KeyListener listener)
	{
		cache.addMapListener(new MapListener()
		{
			@Override
			public void entryUpdated(MapEvent event)
			{
				listener.onUpdate(event.getKey(), event.getNewValue());
			}
			
			@Override
			public void entryInserted(MapEvent event)
			{
				listener.onInsert(event.getKey(), event.getNewValue());
			}
			
			@Override
			public void entryDeleted(MapEvent event)
			{
				listener.onDelete(event.getKey());
			}
		}, filter, true);
	}
	
	private class KeyIterator implements Iterator<Object>
	{

		private LimitFilter filter;
		private Object[] currentSet;
		private int index;
		
		public KeyIterator(Filter filter)
		{
			this.filter = new LimitFilter(filter, batchSize);
			currentSet = cache.keySet(this.filter).toArray();
			index = 0;
		}
		
		@Override
		public boolean hasNext()
		{
			return currentSet.length > 0;
		}

		@Override
		public Object next()
		{
			Object result = currentSet[index++];
			if (index >= currentSet.length)
			{
				filter.nextPage();
				currentSet = cache.keySet(filter).toArray();
				index = 0;
			}
			return result;
		}

		@Override
		public void remove()
		{
			throw new UnsupportedOperationException();
		}
		
	}

	@Override
	protected Object retrieveValue(Object key)
	{
		return cache.get(key);
	}

}
