package org.elasticsearch.plugin.river.coherence;

import java.util.List;
import java.util.Map;

public interface Synchronizer<K,V>
{
	public void start();
	public List<Map.Entry<KeyOperation<K>, V>> take() throws InterruptedException;
	
	public static abstract class KeyOperation<K>
	{
		@Override
		public int hashCode()
		{
			final int prime = 31;
			int result = 1;
			result = prime * result + ((key == null) ? 0 : key.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj)
		{
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			KeyOperation<?> other = (KeyOperation<?>) obj;
			if (key == null)
			{
				if (other.key != null)
					return false;
			}
			else if (!key.equals(other.key))
				return false;
			return true;
		}

		private K key;

		public KeyOperation(K key)
		{
			this.key = key;
		}

		public abstract OpType getType();

		public K getKey()
		{
			return key;
		}
	}
	
	enum OpType
	{
		DELETE, UPDATE, INSERT
	}

	static class KeyDelete<K> extends KeyOperation<K>
	{
		public KeyDelete(K key)
		{
			super(key);
		}

		@Override
		public OpType getType()
		{
			return OpType.DELETE;
		}
	}

	static class KeyInsert<K> extends KeyOperation<K>
	{
		public KeyInsert(K key)
		{
			super(key);
		}

		@Override
		public OpType getType()
		{
			return OpType.INSERT;
		}
	}

	static class KeyUpdate<K> extends KeyOperation<K>
	{
		public KeyUpdate(K key)
		{
			super(key);
		}

		@Override
		public OpType getType()
		{
			return OpType.UPDATE;
		}
	}

}
