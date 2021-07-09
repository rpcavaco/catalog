
# ttp://milao.cm-porto.net/arcgis/rest/services/IGVP/IG_DMGGVP/MapServer


SQL_SEL_AGSNODES = "SELECT node, internalurl FROM md.agserver_node"

SQL_PESQUISA_GERAL = """with base as (
	select 'SITE' AS tipo, node,  site_id munisig, '' arcgisserver, '' display_name, '' dbobj
		, unaccent(string_agg(DISTINCT array_to_string(lexemes, ' '), ' ')) processado
	from md.munisig_site a,
	LATERAL ts_debug('portuguese', 
			  regexp_replace(replace(a.label, '-', ' '),'([a-z])([A-Z])', '\1 \2','g') )
	where array_length(lexemes, 1) > 0
	group by node, site_id
	UNION
	select 'AGS_FOLDER' AS tipo, node, '' munisig, folder arcgisserver, '' display_name, '' dbobj
		, unaccent(string_agg(DISTINCT array_to_string(lexemes, ' '), ' ')) processado
	from (
		select distinct node, folder
		from md.munisig_layer
		where char_length(folder) > 0
	) a,
	LATERAL ts_debug('portuguese', 
			  regexp_replace(replace(a.folder, '-', ' '),'([a-z])([A-Z])', '\1 \2','g') )
	where array_length(lexemes, 1) > 0
	group by node, folder
	UNION
	select 'AGS_SERVICE' AS tipo, node, 
		string_agg(DISTINCT site_id, ', ') munisig,
		folder || '/' ||  service arcgisserver, '' display_name, '' dbobj
		, unaccent(string_agg(DISTINCT array_to_string(lexemes, ' '), ' ')) processado
		--unaccent(array_to_string(lexemes, ' ')) processado
	from (
		select distinct node, site_id, folder, service
		from md.munisig_layer
	) a,
	LATERAL ts_debug('portuguese', 
			  regexp_replace(replace(a.service, '-', ' '),'([a-z])([A-Z])', '\1 \2','g') )
	where array_length(lexemes, 1) > 0
	group by node, folder, service
	UNION
	select 'LAYER' AS tipo, node, 
		string_agg(DISTINCT site_id, ', ') munisig,
		string_agg(DISTINCT format('%s/%s/MapServer/%s', folder, service, service_lyrid), ', ') arcgisserver, display_name, split_part(dbobj, '.', 2) dbobj
		, unaccent(string_agg(DISTINCT array_to_string(lexemes, ' '), ' ')) processado
	from md.munisig_layer a,
	LATERAL ts_debug('portuguese', 
			  regexp_replace(replace(a.display_name, '-', ' '),'([a-z])([A-Z])', '\1 \2','g') )
	where array_length(lexemes, 1) > 0
	group by node, display_name, split_part(dbobj, '.', 2)
)
select *
from base 
where array_length(md.array_intersect((select (ts_debug($1)).lexemes), string_to_array(processado, ' ')), 1) = array_length((select (ts_debug($2)).lexemes), 1)"""

