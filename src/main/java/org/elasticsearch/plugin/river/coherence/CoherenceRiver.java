package org.elasticsearch.plugin.river.coherence;

import java.util.Map;

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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.net.cache.ContinuousQueryCache;
import com.tangosol.util.MapEvent;
import com.tangosol.util.MapListener;
import com.tangosol.util.QueryHelper;

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
	private int bulkSize = 500;
	private int bulkThreadLimit = 10;
	private int bulkFlushInterval = 5;

	@SuppressWarnings("unused")
	private ContinuousQueryCache cqc;
	private String cacheName = DEFAULT_LABEL;

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
	}

	@Override
	public void start()
	{
		if (!client.admin().indices().prepareExists(index).execute().actionGet().isExists())
		{
			CreateIndexRequestBuilder createIndexRequest = client.admin().indices()
					.prepareCreate(index);
			createIndexRequest.execute().actionGet();
		}
		NamedCache cache;
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
		cqc = new ContinuousQueryCache(cache, QueryHelper.createFilter(query),
				new DefaultListener());
	}

	@Override
	public void close()
	{
		try
		{
			CacheFactory.shutdown();
		}
		catch (Exception ex)
		{
			logger.error("Error releasing cache factory resources", ex);
		}
	}

	private class DefaultListener implements MapListener
	{
		private ObjectMapper mapper = new ObjectMapper();

		@Override
		public void entryInserted(MapEvent paramMapEvent)
		{
			try
			{
				bulkProcessor.add(Requests.indexRequest(index).type(type)
						.id(paramMapEvent.getKey().toString())
						.source(mapper.writeValueAsString(paramMapEvent.getNewValue()))
						.opType(OpType.INDEX));
			}
			catch (JsonProcessingException ex)
			{
				logger.error("Error serializing object", ex, paramMapEvent.getNewValue());
			}
		}

		@Override
		public void entryUpdated(MapEvent paramMapEvent)
		{
			// Upsert
			entryInserted(paramMapEvent);
		}

		@Override
		public void entryDeleted(MapEvent paramMapEvent)
		{
			bulkProcessor.add(new DeleteRequest(index, type, paramMapEvent.getKey().toString()));
		}
	}

}
