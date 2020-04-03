import requests
import os
import json
import datetime
import argparse
import collections
import numpy
import matplotlib.pyplot as plt
from dateutil.relativedelta import relativedelta


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


def get_dataset_time_data(project, start_date, end_date, activity_id=None, cumulative=False):

	date_format = '%Y-%m-%dT%H:%M:%SZ'
	start_str = start_date.strftime(date_format)
	end_str = end_date.strftime(date_format)

	solr_url = get_solr_query_url()

	if activity_id is None:
		query = 'rows=0&fq=project:{project}' \
				'&facet.range=_timestamp' \
				'&facet.range.start={start_date}' \
				'&facet.range.end={end_date}' \
				'&facet.range.gap=%2B1DAY'
		query_url = solr_url.format(query=query.format(project=project, 
													start_date=start_str, 
													end_date=start_str))
	else:
		query = 'rows=0&fq=project:{project}' \
				'&fq=activity_id:{activity_id}' \
				'&facet.range=_timestamp' \
				'&facet.range.start={start_date}' \
				'&facet.range.end={end_date}' \
				'&facet.range.gap=%2B1DAY'
		query_url = solr_url.format(query=query.format(project=project, 
													activity_id=activity_id, 
													start_date=start_str, 
													end_date=start_str))
	req = requests.get(query_url)
	js = json.loads(req.text)

	ts_counts = js['facet_counts']['facet_ranges']['_timestamp']
	ts = ts_counts['counts'][0::2]
	counts = ts_counts['counts'][1::2]

	datetimes = [datetime.datetime.strptime(t,date_format) for t in ts]

	if cumulative:
		counts = numpy.cumsum(counts)

	return (datetimes, counts)


def gen_plot(project, start_date, end_date, activity_id=None, cumulative=False, output_dir=None):

	start_str = start_date.strftime("%Y%m%d")
	end_str = end_date.strftime("%Y%m%d")
	
	# store data in CSV files
	print("start = %s, end = %s"%(start_str, end_str))
	datetimes, counts = get_dataset_time_data(project=project, 
											  start_date=start_date, 
											  end_date=end_date, 
											  activity_id=activity_id, 
											  cumulative=cumulative)

	# plot data


def main():

	parser = argparse.ArgumentParser(description="Create HTML tables for the data holdings of ESGF")
	parser.add_argument("--project", "-p", dest="project", type=str, default="CMIP6", help="MIP project name (default is CMIP6)")
	parser.add_argument("--activity_id", "-ai", dest="activity_id", type=str, default=None, help="MIP activity id (default is None)")
	parser.add_argument("--start_date", "-sd", dest="start_date", type=str, default=None, help="Start date in YYYY-MM-DD format (default is None)")
	parser.add_argument("--end_date", "-ed", dest="end_date", type=str, default=None, help="End date in YYYY-MM-DD format (default is None)")
	parser.add_argument("--output", "-o", dest="output", type=str, default=os.path.curdir, help="Output directory (default is current directory)")
	parser.add_argument("--cumulative", "-cs", dest="cumulative", action='store_true', help="Use cumulative (default is False)")
	parser.set_defaults(cumulative=False)
	args = parser.parse_args()

	if args.start_date is None:
		print("You must enter a start date.")
		return
	else:
		try:
			start_date = datetime.datetime.strptime(args.start_date, '%Y-%m-%d')
		except ValueError:
			raise ValueError("Incorrect start date format, should be YYYY-MM-DD")
			return

	if args.end_date is None:
		print("You must enter an end date.")
		return
	else:
		try:
			end_date = datetime.datetime.strptime(args.end_date, '%Y-%m-%d')
		except ValueError:
			raise ValueError("Incorrect end date format, should be YYYY-MM-DD")
			return

	if not os.path.isdir(args.output):
		print("{} is not a directory. Exiting.".format(args.output))
		return
	
	gen_plot(args.project, start_date, end_date, args.activity_id, args.cumulative, args.output)


if __name__ == '__main__':
	main()
