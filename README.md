Coherence River Plugin for ElasticSearch
========================================


This plugin fetches data from a Coherence cache and stores it in ElasticSearch.

To create the river you need first to install the plugin and the add the coherence jar dependencies including the configuration files and pof jars to the plugin directory

The river perform the initial import of the cache data and then maintain the index updated using a continuous query. The id used is the key of each key/value pair.