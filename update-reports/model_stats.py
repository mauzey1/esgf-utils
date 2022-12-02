from __future__ import print_function
import requests
import os
import json
import argparse


def get_solr_query_url():
    search_url = 'https://esgf-node.llnl.gov/esg-search/search/' \
                 '?limit=0&format=application%2Fsolr%2Bjson'

    req = requests.get(search_url)
    js = json.loads(req.text)
    shards = js['responseHeader']['params']['shards']

    solr_url = 'https://esgf-node.llnl.gov/solr/datasets/select' \
               '?q=*:*&wt=json&facet=true&fq=type:Dataset' \
               '&fq=replica:false&fq=latest:true&shards={shards}&{{query}}'
    
    return solr_url.format(shards=shards)


def get_stats(project, facet1, facet2, facet3, facet4):
	solr_url = get_solr_query_url()

	query = 'rows=0&fq=project:{project}' \
			'&facet.field={facet1}&facet.field={facet2}' \
			'&facet.field={facet3}&facet.field={facet4}' \
			'&facet.pivot={{!stats=piv}}{facet1},{facet2},{facet3},{facet4}'
	query_url = solr_url.format(query=query.format(project=project, 
												   facet1=facet1, 
												   facet2=facet2, 
												   facet3=facet3, 
												   facet4=facet4))
	req = requests.get(query_url)
	js = json.loads(req.text)

	return js


def main():

	parser = argparse.ArgumentParser(description="Create HTML tables for the data holdings of ESGF")
	parser.add_argument("--project", "-p", dest="project", type=str, default="CMIP6", help="MIP project name (default is CMIP6)")
	parser.add_argument("--output", "-o", dest="output", type=str, default=os.path.curdir, help="Output directory (default is current directory)")
	args = parser.parse_args()

	if not os.path.isdir(args.output):
		print("{} is not a directory. Exiting.".format(args.output))
		return

	js = get_stats(args.project, "table_id", "variable_id", "experiment_id", "source_id")
	
	path = os.path.join(args.output, args.project+'_model_stats.json')
	with open(path, 'w') as outfile:
		json.dump(js, outfile, indent=4)


if __name__ == '__main__':
	main()
