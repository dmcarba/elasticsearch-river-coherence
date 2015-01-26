package org.elasticsearch.plugin.river.coherence;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest.OpType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.util.Filter;
import com.tangosol.util.MapEvent;
import com.tangosol.util.MapListener;
import com.tangosol.util.QueryHelper;
import com.tangosol.util.filter.LimitFilter;

public class CoherenceRiver extends AbstractRiverComponent implements River
{

	private static final String DEFAULT_LABEL = "default";
	private static final String DEFAULT_QUERY = "true";

	private final Client client;
	private String configPath;
	private String index;
	private String type = DEFAULT_LABEL;
	private String query = DEFAULT_QUERY;
	private final BulkProcessor bulkProcessor;

	private final ObjectMapper mapper = new ObjectMapper();
	private final ExecutorService service = Executors.newSingleThreadExecutor();

	private int bulkSize = 100;
	private int bulkThreadLimit = 10;
	private int bulkFlushInterval = 5;
	private NamedCache cache;
	private Callable<Void> indexerTask;

	private String cacheName = DEFAULT_LABEL;

	private Set<KeyOperation> keyOperationSet = Collections
			.newSetFromMap(new ConcurrentHashMap<KeyOperation, Boolean>());

	@Inject
	protected CoherenceRiver(RiverName riverName, RiverSettings settings, Client client)
	{
		super(riverName, settings);
		this.client = client;

		if (settings.settings().containsKey("coherence"))
		{
			@SuppressWarnings("unchecked")
			Map<String, Object> coherenceSettings = (Map<String, Object>) settings.settings().get(
					"coherence");
			configPath = XContentMapValues.nodeStringValue(coherenceSettings.get("configPath"),
					configPath);
			cacheName = XContentMapValues
					.nodeStringValue(coherenceSettings.get("cache"), cacheName);
			query = XContentMapValues.nodeStringValue(coherenceSettings.get("query"), query);
		}
		index = riverName.type();
		if (settings.settings().containsKey("index"))
		{
			@SuppressWarnings("unchecked")
			Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get(
					"index");
			index = XContentMapValues.nodeStringValue(indexSettings.get("index"), index);
			type = XContentMapValues.nodeStringValue(indexSettings.get("type"), type);
			// Throttling control parameters
			bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulkSize"), bulkSize);
			bulkThreadLimit = XContentMapValues.nodeIntegerValue(
					indexSettings.get("bulkThreadLimit"), bulkThreadLimit);
			bulkFlushInterval = XContentMapValues.nodeIntegerValue(
					indexSettings.get("bulkFlushInterval"), bulkFlushInterval);
		}
		bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener()
		{
			@Override
			public void beforeBulk(long executionId, BulkRequest request)
			{
				logger.debug("Before bulk of {} actions", request.numberOfActions());
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request, BulkResponse response)
			{
				logger.debug("After bulk of {} actions", request.numberOfActions());
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request, Throwable failure)
			{
				logger.error("Error executing bulk", failure);
			}
		}).setBulkActions(bulkSize).setConcurrentRequests(bulkThreadLimit)
				.setFlushInterval(TimeValue.timeValueSeconds(bulkFlushInterval)).build();

		indexerTask = new Callable<Void>()
		{
			@Override
			public Void call() throws Exception
			{

				List<KeyOperation> batch = new ArrayList<KeyOperation>();
				while (true)
				{
					while (keyOperationSet.size() > 0)
					{

						int i = 0;
						Iterator<KeyOperation> it = keyOperationSet.iterator();
						while (i < bulkSize && it.hasNext())
						{
							batch.add(it.next());
							it.remove();
							i++;
						}

						for (KeyOperation keyOp : batch)
						{
							try
							{
								switch (keyOp.getType())
								{
								case MapEvent.ENTRY_DELETED:
									bulkProcessor.add(new DeleteRequest(index, type, keyOp.getKey()
											.toString()));
									break;
								default:
									bulkProcessor.add(Requests
											.indexRequest(index)
											.type(type)
											.id(keyOp.getKey().toString())
											.source(mapper.writeValueAsString(cache.get(keyOp
													.getKey()))).opType(OpType.INDEX));
								}
							}
							catch (Exception ex)
							{
								logger.error("Error processing key {}", keyOp.getKey());
							}

						}
						batch.clear();
					}
					synchronized (this)
					{
						wait();
					}
				}
			}
		};
	}

	@SuppressWarnings("unchecked")
	@Override
	public void start()
	{
		if (!client.admin().indices().prepareExists(index).execute().actionGet().isExists())
		{
			CreateIndexRequestBuilder createIndexRequest = client.admin().indices()
					.prepareCreate(index);
			createIndexRequest.execute().actionGet();
		}

		if (configPath == null)
		{
			cache = CacheFactory.getCache(cacheName);
		}
		else
		{
			ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
			cache = CacheFactory.getCacheFactoryBuilder()
					.getConfigurableCacheFactory(configPath, classLoader)
					.ensureCache(cacheName, classLoader);
		}
		Filter filter = QueryHelper.createFilter(query);
		cache.addMapListener(new DefaultListener(), filter, true);
		LimitFilter lFilter = new LimitFilter(filter, bulkSize * 4);
		service.submit(indexerTask);
		Set<Object> keys = null;
		do
		{
			for (Object key : keys = cache.keySet(lFilter))
			{
				keyOperationSet.add(new KeyUpsert(key));
			}
			synchronized (indexerTask)
			{
				indexerTask.notify();
			}
			lFilter.nextPage();
		} while (keys.size() > 0);

	}

	@Override
	public void close()
	{
		try
		{
			CacheFactory.shutdown();
			service.shutdownNow();
		}
		catch (Exception ex)
		{
			logger.error("Error releasing cache factory resources", ex);
		}
	}

	private class DefaultListener implements MapListener
	{
		@Override
		public void entryInserted(MapEvent paramMapEvent)
		{
			storeOp(new KeyUpsert(paramMapEvent.getKey()));
		}

		private void storeOp(KeyOperation keyOp)
		{
			keyOperationSet.add(keyOp);
			synchronized (indexerTask)
			{
				indexerTask.notify();
			}
		}

		@Override
		public void entryUpdated(MapEvent paramMapEvent)
		{
			entryInserted(paramMapEvent);
		}

		@Override
		public void entryDeleted(MapEvent paramMapEvent)
		{
			storeOp(new KeyDeletion(paramMapEvent.getKey()));
		}

	}

	private static abstract class KeyOperation
	{
		Object key;

		public KeyOperation(Object key)
		{
			this.key = key;
		}

		public Object getKey()
		{
			return key;
		}

		public abstract int getType();

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
			KeyOperation other = (KeyOperation) obj;
			if (key == null)
			{
				if (other.key != null)
					return false;
			}
			else if (!key.equals(other.key))
				return false;
			return true;
		}
	}

	private static class KeyUpsert extends KeyOperation
	{
		public KeyUpsert(Object key)
		{
			super(key);
		}

		@Override
		public int getType()
		{
			return MapEvent.ENTRY_INSERTED;
		}
	}

	private static class KeyDeletion extends KeyOperation
	{
		public KeyDeletion(Object key)
		{
			super(key);
		}

		@Override
		public int getType()
		{
			return MapEvent.ENTRY_DELETED;
		}
	}

}
