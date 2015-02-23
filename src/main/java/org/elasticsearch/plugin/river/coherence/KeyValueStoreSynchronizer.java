package org.elasticsearch.plugin.river.coherence;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public abstract class KeyValueStoreSynchronizer<K, V> implements Synchronizer<K, V>
{
	private static final int BATCH_SIZE = 100;

	protected final int batchSize;
	private int storedValues;
	private final KeyListener listener;
	private final Map<KeyOperation<K>, V> store;

	private final ReentrantLock lock;
	private final Condition notEmpty;

	public KeyValueStoreSynchronizer(int valuesCacheSize)
	{
		this.batchSize = valuesCacheSize;
		this.listener = new KeyListener();
		store = new LinkedHashMap<>();
		storedValues = 0;
		lock = new ReentrantLock(false);
		notEmpty = lock.newCondition();
	}

	public KeyValueStoreSynchronizer()
	{
		this(BATCH_SIZE);
	}

	public void start()
	{
		registerKeyListener(listener);
		Iterator<K> iterator = initialKeySetIterator();
		while (iterator.hasNext())
		{
			put(new KeyInsert<K>(iterator.next()));
		}		
	}

	public List<Map.Entry<KeyOperation<K>, V>> take() throws InterruptedException
	{
		lock.lock();
		try
		{
			while (store.size() == 0)
				notEmpty.await();
			List<Map.Entry<KeyOperation<K>, V>> result = new ArrayList<>();
			Iterator<Map.Entry<KeyOperation<K>, V>> it = store.entrySet().iterator();
			int i = 0;
			while (it.hasNext() && i < batchSize)
			{
				Map.Entry<KeyOperation<K>, V> entry = it.next();
				if (entry.getKey().getType() != OpType.DELETE && entry.getValue() == null)
				{
					entry.setValue(retrieveValue(entry.getKey().getKey()));
				}
				result.add(entry);
				i++;
				it.remove();
			}
			return result;
		}
		finally
		{
			lock.unlock();
		}
	}

	protected void put(KeyOperation<K> keyop, V value)
	{
		lock.lock();
		try
		{
			switch (keyop.getType())
			{
			case DELETE:
				store.put(keyop, value);
			default:
				if (value != null)
				{
					if (storedValues < batchSize)
					{
						storedValues++;
					}
					else
					{
						value = null;
					}
					store.put(keyop, value);
				}
				else if (!store.containsKey(keyop))
				{
					store.put(keyop, value);
				}
			}
			notEmpty.signal();
		}
		finally
		{
			lock.unlock();
		}
	}

	protected void put(KeyOperation<K> keyop)
	{
		put(keyop, null);
	}

	public abstract Iterator<K> initialKeySetIterator();

	public abstract void registerKeyListener(KeyListener listener);

	protected abstract V retrieveValue(K key);

	protected class KeyListener
	{
		public void onUpdate(K key, V value)
		{
			put(new KeyUpdate<K>(key), value);
		}

		public void onInsert(K key, V value)
		{
			put(new KeyInsert<K>(key), value);
		}

		public void onDelete(K key)
		{
			put(new KeyDelete<K>(key), null);
		}
	}

}
