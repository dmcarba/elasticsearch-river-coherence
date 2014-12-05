package org.elasticsearch.plugin.river.coherence;

import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;

public class CoherenceRiverPlugin extends AbstractPlugin
{

	@Override
	public String name()
	{
		return "river-coherence";
	}

	@Override
	public String description()
	{
		return "Coherence river plugin";
	}

	public void onModule(RiversModule module)
	{
		module.registerRiver("coherence", CoherenceRiverModule.class);
	}

}
