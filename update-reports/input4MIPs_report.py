import os
import json
import argparse
import urllib.request


def get_solr_query_url(): 
    search_url = 'https://esgf-node.llnl.gov/esg-search/search/' \
                 '?limit=0&format=application%2Fsolr%2Bjson' 

    with urllib.request.urlopen(search_url) as url:
        js = json.loads(url.read().decode('UTF-8'))
    shards = js['responseHeader']['params']['shards'] 

    solr_url = 'https://esgf-node.llnl.gov/solr/datasets/select' \
               '?q=*:*&wt=json&facet=true&fq=type:Dataset' \
               '&fq=replica:false&shards={shards}&{{query}}' 

    return solr_url.format(shards=shards)

def get_doi(activity_id, mip_era, target_mip_list, institution_id, source_id, cafile=None):
    json_url = 'https://cera-www.dkrz.de/WDCC/ui/' \
               'cerasearch/cerarest/exportcmip6?input={query}&wt=json'

    query = '.'.join([activity_id, mip_era, target_mip_list, institution_id, source_id])

    if cafile is not None:
        with urllib.request.urlopen(json_url.format(query=query), cafile=cafile) as url:
            js = json.loads(url.read().decode('UTF-8'))
    else:
        with urllib.request.urlopen(json_url.format(query=query)) as url:
            js = json.loads(url.read().decode('UTF-8'))

    return dict(id=js['identifier']['id'],title=js['titles'][0])

def get_input4mips_stats(output_dir, cafile):
    activity_id = 'input4MIPs'      
    field1 = 'mip_era'
    field2 = 'target_mip_list'                                                                          
    field3 = 'institution_id'                                                                                 
    field4 = 'source_id'
    target_mip_list_pivot = ','.join([field1,field2,field3,field4,'target_mip_list'])   
    dataset_category_pivot = ','.join([field1,field2,field3,field4,'dataset_category'])                
    source_version_pivot = ','.join([field1,field2,field3,field4,'source_version'])                                                         

    solr_url = get_solr_query_url()

    def query_solr(_pivot): 
        query = 'rows=0&fq=activity_id:{activity_id}' \
                '&facet.pivot={pivot}'                              

        query_url = solr_url.format(query=query.format(activity_id=activity_id,pivot=_pivot))
        with urllib.request.urlopen(query_url) as url:
            js = json.loads(url.read().decode('UTF-8'))

        facets = js['facet_counts']['facet_pivot'][_pivot]
        facet_dict = {'field':'activity_id','value':activity_id,'pivot':facets}
    
        def build_dict(_dict):
            new_dict = {} 
            for d in _dict['pivot']: 
                if 'pivot' in d: 
                    new_dict.update({d['value']:build_dict(d)}) 
                else: 
                    if d['field'] in new_dict:
                        if not isinstance(new_dict[d['field']], list):
                            new_dict[d['field']] = [new_dict[d['field']]]
                        new_dict[d['field']].append(d['value'])
                    else:
                        new_dict.update({d['field']:d['value']}) 
            return new_dict
        
        return build_dict(facet_dict)

    def get_dataset_status():                     
        _pivot = ','.join(['instance_id','dataset_status'])
        query = 'rows=0&fq=activity_id:{activity_id}' \
                '&facet.pivot={pivot}'                              

        query_url = solr_url.format(query=query.format(activity_id=activity_id,pivot=_pivot))
        with urllib.request.urlopen(query_url) as url:
            js = json.loads(url.read().decode('UTF-8'))

        facets = js['facet_counts']['facet_pivot'][_pivot]
        facet_dict = {'field':'activity_id','value':activity_id,'pivot':facets}
    
        id_status_dict = {}
        for f in facets:
            if 'pivot' in f:
                id_status_dict[f['value']] = f['pivot'][0]['value']
            else:
                id_status_dict[f['value']] = 'None'

        return id_status_dict

    target_mip_list_dict = query_solr(target_mip_list_pivot)
    dataset_category_dict = query_solr(dataset_category_pivot)
    source_version_dict = query_solr(source_version_pivot)

    dataset_status_dict = get_dataset_status()

    # organize instance_id's by mip_era, target_mip, institution_id, and source_id
    id_status = {}
    for instance_id, status in dataset_status_dict.items():
        id_list = instance_id.split('.')
        key = '.'.join(id_list[:5])
        if key not in id_status:
            id_status[key] = {}
        id_status[key][instance_id] = status

    data_dict = {}
    for mip_era,v1 in target_mip_list_dict.items():
        for target_mip,v2 in v1.items():
            for inst_id,v3 in v2.items():
                for src_id,v4 in v3.items():
                    did = '.'.join([activity_id, mip_era, target_mip, inst_id, src_id])
                    if did in id_status:
                        doi_dict = get_doi(activity_id, mip_era, target_mip, inst_id, src_id, cafile)
                        doi = doi_dict['id']
                        title = doi_dict['title']
                        target_mip_list = v4['target_mip_list']
                        dataset_category = dataset_category_dict[mip_era][target_mip][inst_id][src_id]['dataset_category']
                        source_version = source_version_dict[mip_era][target_mip][inst_id][src_id]['source_version']
                        id_dict = id_status[did]
                        data_dict[did] = dict(institutionId=inst_id,
                                            sourceId=src_id,
                                            mipTable=target_mip_list,
                                            datatype=dataset_category,
                                            version=source_version,
                                            id=id_dict,
                                            doi=doi,
                                            title=title
                                            )

    with open('input4MIPs_report.json', 'w') as outfile:
        json.dump(dict(data=data_dict), outfile, indent=4)

    filepath = os.path.join(output_dir, 'input4MIPs_report.json')
    with open(filepath, 'w') as outfile:
        json.dump(dict(data=data_dict), outfile, indent=4)


def main():

	parser = argparse.ArgumentParser(description="Create a JSON table for the report of input4MIPs in ESGF")
	parser.add_argument("--output", "-o", dest="output", type=str, default=os.path.curdir, help="Output directory (default is current directory)")
	parser.add_argument("--cafile", dest="cafile", type=str, default=None, help="File path of a CA certificates file for HTTPS requests")
	args = parser.parse_args()

	if not os.path.isdir(args.output):
		print("{} is not a directory. Exiting.".format(args.output))
		return
	
	get_input4mips_stats(args.output, args.cafile)


if __name__ == '__main__':
	main()