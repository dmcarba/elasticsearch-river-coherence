package org.elasticsearch.plugin.river.coherence;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.river.RiverIndexName;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;

@ClusterScope(scope = Scope.SUITE, numDataNodes = 2)
public class CoherenceRiverTest extends ElasticsearchIntegrationTest
{

	private static ObjectMapper mapper = new ObjectMapper();

	@Override
	protected Settings nodeSettings(int nodeOrdinal)
	{
		return ImmutableSettings.builder().put("plugins.load_classpath_plugins", "true").build();
	}

	@After
	private void cleanCache()
	{
		CacheFactory.shutdown();
	}

	@Test
	public void testIndexingCoherenceConfigIndexDefault() throws Exception
	{
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		NamedCache cache = CacheFactory.getCacheFactoryBuilder()
				.getConfigurableCacheFactory("test-cache-config.xml", classLoader)
				.ensureCache("TEST_CACHE", classLoader);
		// Add to cache
		for (int i = 1; i < 100; i++)
		{
			cache.put(-i, getTestBean(1));
		}
		// River
		XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
		builder.field("type", "coherence");
		builder.startObject("coherence");
		builder.field("configPath", "test-cache-config.xml");
		builder.field("cache", "TEST_CACHE");
		builder.endObject();
		builder.endObject();
		logger.info("Adding river \n{}", builder.string());
		client().prepareIndex(RiverIndexName.Conf.DEFAULT_INDEX_NAME, "coherence_river", "_meta")
				.setSource(builder).get();
		// Add to cache
		for (int i = 0; i < 1000; i++)
		{
			cache.put(i, getTestBean(1));
		}

		checkCount("coherence", 1099);
		checkDocument("coherence", "default", "100", 1);
		// Add to cache
		for (int i = 1000; i < 2000; i++)
		{
			cache.put(i, getTestBean(1));
		}
		checkCount("coherence", 2099);

		// Add to cache previous keys
		for (int i = 0; i < 1000; i++)
		{
			cache.put(i, getTestBean(2));
		}
		checkCount("coherence", 2099);
		checkDocument("coherence", "default", "100", 2);
		// Remove previous keys
		for (int i = 0; i < 1000; i++)
		{
			cache.remove(i);
		}
		checkCount("coherence", 1099);
	}

	@Test
	public void testIndexingCoherenceDefaultIndexConfig() throws Exception
	{
		NamedCache cache = CacheFactory.getCache("TEST_CACHE");
		// Add to cache
		cache.put(-1, getTestBean(1));
		// River
		XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
		builder.field("type", "coherence");
		builder.startObject("coherence");
		builder.field("cache", "TEST_CACHE");
		builder.endObject();
		builder.startObject("index");
		builder.field("index", "test_index");
		builder.field("type", "test_type");
		builder.field("bulkSize", "20");
		builder.field("bulkThreadLimit", "1");
		builder.field("bulkFlushInterval", "10");
		builder.endObject();
		builder.endObject();
		logger.info("Adding river \n{}", builder.string());
		client().prepareIndex(RiverIndexName.Conf.DEFAULT_INDEX_NAME, "coherence_river_2", "_meta")
				.setSource(builder).get();
		// Add to cache
		for (int i = 0; i < 100; i++)
		{
			cache.put(i, getTestBean(i));
		}
		checkCount("test_index", 101);
		for (int i = 0; i < 100; i++)
		{
			checkDocument("test_index", "test_type", String.valueOf(i), i);
		}
	}

	@Test
	public void testIndexingCoherenceFilter() throws Exception
	{
		NamedCache cache = CacheFactory.getCache("TEST_CACHE");
		// Add to cache
		cache.put(-1, getTestBean(1));
		// River
		XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
		builder.field("type", "coherence");
		builder.startObject("coherence");
		builder.field("cache", "TEST_CACHE");
		builder.field("query", "key() between 500 and 800");
		builder.endObject();
		builder.endObject();

		logger.info("Adding river \n{}", builder.string());
		client().prepareIndex(RiverIndexName.Conf.DEFAULT_INDEX_NAME, "coherence_river_3", "_meta")
				.setSource(builder).get();
		// Add to cache
		for (int i = 0; i < 1000; i++)
		{
			cache.put(i, getTestBean(1));
		}
		checkCount("coherence", 301);
	}

	private void checkCount(final String index, final int expectedCount)
			throws InterruptedException
	{
		ensureGreen(index);
		assertThat(awaitBusy(new Predicate<Object>()
		{
			@Override
			public boolean apply(Object o)
			{
				client().admin().indices().prepareRefresh(index).get();
				return client().prepareSearch(index).setSearchType(SearchType.COUNT).get()
						.getHits().getTotalHits() == expectedCount;
			}
		}, 20, TimeUnit.SECONDS), CoreMatchers.equalTo(true));
	}

	private void checkDocument(String index, String type, String id, int value) throws Exception
	{
		GetResponse response = client().prepareGet(index, type, id).execute().actionGet();
		TestBean test = mapper.readValue(response.getSourceAsString(), TestBean.class);
		assertEquals(value, test.getIntValue());
		assertEquals(10000000000L, test.getLongValue());
		assertEquals("testBean", test.getStringValue());
		assertEquals("element1", test.getArrayValue()[0]);
	}

	private TestBean getTestBean(int i)
	{
		TestBean testBean = new TestBean();
		testBean.setIntValue(i);
		testBean.setLongValue(10000000000L);
		testBean.setStringValue("testBean");
		testBean.setArrayValue(new String[]
		{ "element1", "element2" });
		Map<String, String> map = new HashMap<String, String>();
		map.put("key1", "value1");
		map.put("key2", "value2");
		testBean.setMapValue(map);
		return testBean;
	}

	@SuppressWarnings(
	{ "serial", "unused" })
	private static class TestBean implements Serializable
	{
		private int intValue;
		private long longValue;
		private String stringValue;
		private String[] arrayValue;
		private Map<String, String> mapValue;

		public void setIntValue(int intValue)
		{
			this.intValue = intValue;
		}

		public void setLongValue(long longValue)
		{
			this.longValue = longValue;
		}

		public void setStringValue(String stringValue)
		{
			this.stringValue = stringValue;
		}

		public void setArrayValue(String[] arrayValue)
		{
			this.arrayValue = arrayValue;
		}

		public void setMapValue(Map<String, String> mapValue)
		{
			this.mapValue = mapValue;
		}

		public int getIntValue()
		{
			return intValue;
		}

		public long getLongValue()
		{
			return longValue;
		}

		public String getStringValue()
		{
			return stringValue;
		}

		public String[] getArrayValue()
		{
			return arrayValue;
		}

		public Map<String, String> getMapValue()
		{
			return mapValue;
		}
	}

}
