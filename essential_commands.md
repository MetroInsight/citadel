

# Virtuoso indexing name.
DB.DBA.RDF_OBJ_FT_RULE_ADD (null, '<http://metroinsight.io/citadel#name>', 'All');
DB.DBA.RDF_OBJ_FT_RULE_DEL (null, '<http://metroinsight.io/citadel#name>', 'All');

DB.DBA.RDF_OBJ_FT_RULE_ADD (null, 'http://metroinsight.io/citadel#name', 'All');
DB.DBA.RDF_OBJ_FT_RULE_DEL (null, 'http://metroinsight.io/citadel#name', 'All');

DB.DBA.RDF_OBJ_FT_RULE_ADD (null, null, '<http://metroinsight.io/citadel#name>');
DB.DBA.RDF_OBJ_FT_RULE_DEL (null, null, 'All');
DB.DBA.RDF_OBJ_FT_RULE_ADD (null, null, null)"


DB.DBA.VT_INC_INDEX_DB_DBA_RDF_OBJ ();
DB.DBA.VT_BATCH_UPDATE ('DB.DBA.RDF_OBJ', 'OFF', null);
