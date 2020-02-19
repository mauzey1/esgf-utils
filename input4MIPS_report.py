import json
import argparse
import requests

def get_solr_query_url(): 
    search_url = 'https://esgf-node.llnl.gov/esg-search/search/' \
                 '?limit=0&format=application%2Fsolr%2Bjson' 

    req = requests.get(search_url) 
    js = json.loads(req.text) 
    shards = js['responseHeader']['params']['shards'] 

    solr_url = 'https://esgf-node.llnl.gov/solr/datasets/select' \
               '?q=*:*&wt=json&facet=true&fq=type:Dataset' \
               '&fq=replica:false&shards={shards}&{{query}}' 

    return solr_url.format(shards=shards)

def get_doi(activity_id, mip_era, target_mip_list, institution_id, source_id):
    json_url = 'https://cera-www.dkrz.de/WDCC/ui/' \
               'cerasearch/cerarest/exportcmip6?input={query}&wt=json'

    query = '.'.join([activity_id, mip_era, target_mip_list, institution_id, source_id])

    req = requests.get(json_url.format(query=query), verify='/etc/ssl/certs/ca-bundle.crt') 
    js = json.loads(req.text)

    return dict(id=js['identifier']['id'],title=js['titles'][0])

def get_input4mips_stats():
    activity_id = 'input4MIPs'      
    field1 = 'mip_era'
    field2 = 'target_mip'                                                                          
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
        req = requests.get(query_url) 
        js = json.loads(req.text)

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
        req = requests.get(query_url) 
        js = json.loads(req.text)

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
                    print(did)
                    doi_dict = get_doi(activity_id, mip_era, target_mip, inst_id, src_id)
                    doi = doi_dict['id']
                    title = doi_dict['title']
                    target_mip_list = v4['target_mip_list']
                    dataset_category = dataset_category_dict[mip_era][target_mip][inst_id][src_id]['dataset_category']
                    source_version = source_version_dict[mip_era][target_mip][inst_id][src_id]['source_version']
                    id_dict = id_status[did]
                    data_dict[did] = dict(institution_id=inst_id,
                                          source_id=src_id,
                                          mip_table=target_mip_list,
                                          data_type=dataset_category,
                                          version=source_version,
                                          id=id_dict,
                                          doi=doi,
                                          title=title
                                         )

    with open('input4MIPS_report.json', 'w') as outfile:
        json.dump(dict(data=data_dict), outfile, indent=4)

if __name__ == '__main__':
    get_input4mips_stats()