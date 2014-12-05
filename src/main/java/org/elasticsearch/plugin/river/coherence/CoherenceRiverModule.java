package org.elasticsearch.plugin.river.coherence;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

public class CoherenceRiverModule extends AbstractModule
{

	@Override
	protected void configure()
	{
		bind(River.class).to(CoherenceRiver.class).asEagerSingleton();
	}

}
