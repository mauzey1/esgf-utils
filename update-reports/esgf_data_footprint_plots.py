import requests
import os
import csv
import json
import datetime
import argparse
import collections
import numpy
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker


def get_solr_query_url():
    search_url = 'https://esgf-node.llnl.gov/esg-search/search/' \
                 '?limit=0&format=application%2Fsolr%2Bjson'

    req = requests.get(search_url)
    js = json.loads(req.text)
    shards = js['responseHeader']['params']['shards']

    solr_url = 'https://esgf-node.llnl.gov/solr/files/query' \
               '?q=*:*&wt=json' \
               '&fq=replica:false&fq=latest:true&shards={shards}&{{query}}'
    
    return solr_url.format(shards=shards)


def get_data_footprint_time_data(project, start_date, end_date, activity_id=None, experiment_id=None, cumulative=False):

	date_format = '%Y-%m-%dT%H:%M:%SZ'
	start_str = start_date.strftime(date_format)
	end_str = end_date.strftime(date_format)

	solr_url = get_solr_query_url()

	query = 'rows=0&fq=project:{project}' \
			'&json.facet.daily_footprint.type=range' \
			'&json.facet.daily_footprint.field=_timestamp' \
			'&json.facet.daily_footprint.start=\"{start_date}\"' \
			'&json.facet.daily_footprint.end=\"{end_date}\"' \
			'&json.facet.daily_footprint.gap=\"%2B1DAY\"' \
			'&json.facet.daily_footprint.facet={{data_footprint:\"sum(size)\"}}'
	if activity_id:
		query += '&fq=activity_id:{activity_id}'
	if experiment_id:
		query += '&fq=experiment_id:{experiment_id}'
	query_url = solr_url.format(query=query.format(project=project, 
												   start_date=start_str, 
												   end_date=end_str, 
												   activity_id=activity_id, 
												   experiment_id=experiment_id))

	req = requests.get(query_url)
	js = json.loads(req.text)

	print(js.keys())
	print(js['facets'].keys())
	daily_footprint = js['facets']['daily_footprint']['buckets']
	timestamp = []
	data_footprint = []
	for df in daily_footprint:
		timestamp.append(df['val'])
		if 'data_footprint' in df:
			data_footprint.append(df['data_footprint'])
		else:
			data_footprint.append(0)

	datetimes = [datetime.datetime.strptime(t,date_format) for t in timestamp]

	if cumulative:
		data_footprint = numpy.cumsum(data_footprint)

	return (datetimes, data_footprint)


def gen_plot(project, start_date, end_date, ymin=None, ymax=None, activity_id=None, experiment_id=None, cumulative=False, output_dir=None):

	start_str = start_date.strftime("%Y%m%d")
	end_str = end_date.strftime("%Y%m%d")
	
	# store data in CSV files
	print("Getting ESGF data from {} to {}".format(start_str,end_str))
	datetimes, data_footprint = get_data_footprint_time_data(project=project, 
											  		 		 start_date=start_date, 
											  		 		 end_date=end_date, 
															 activity_id=activity_id, 
															 experiment_id=experiment_id, 
															 cumulative=cumulative)

	if cumulative:
		filename = "esgf_datasets_publication_cumulative_data_footprint_{}".format(project)
	else:
		filename = "esgf_datasets_publication_data_footprint_{}".format(project)
	if activity_id:
		filename += "_{}".format(activity_id)
	if experiment_id:
		filename += "_{}".format(experiment_id)
	filename += "_{}-{}".format(start_str, end_str)

	filename = os.path.join(output_dir, filename)

	csv_filename = filename+".csv"
	print("Writing data to {}".format(csv_filename))
	with open(csv_filename, 'w') as csv_file:
		fieldnames = ['date', 'data_footprint']
		writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

		writer.writeheader()
		for dt,df in zip(datetimes, data_footprint):
			writer.writerow({'date': dt, 'data_footprint': df})

	# plot data
	plot_filename = filename+".png"
	print("Saving plot to {}".format(plot_filename))

	fig, ax = plt.subplots(figsize=(10,5))

	# convert footprint to terabytes
	data_footprint = [df/(2**40) for df in data_footprint]

	@ticker.FuncFormatter
	def major_formatter(x, pos):
		return "%.2f TB" % x

	ax.plot(datetimes, data_footprint)

	ylim_min = ymin if ymin else numpy.min(data_footprint)
	ylim_max = ymax if ymax else numpy.max(data_footprint)
	ax.set(xlim=(start_date, end_date), ylim=(ylim_min, ylim_max))

	title = "{} ".format(project)
	if activity_id:
		title += "{} ".format(activity_id)
	if experiment_id:
		title += "{} ".format(experiment_id)
	if cumulative:
		title += "cumulative data footprint on ESGF"
	else:
		title += "data footprint on ESGF"

	ax.set(xlabel='date', ylabel='data footprint', title=title)
	ax.yaxis.set_major_formatter(major_formatter)
	ax.grid()

	fig.savefig(plot_filename)


def main():

	parser = argparse.ArgumentParser(description="Gather data footprint per day from ESGF")
	parser.add_argument("--project", "-p", dest="project", type=str, default="CMIP6", help="MIP project name (default is CMIP6)")
	parser.add_argument("--activity_id", "-ai", dest="activity_id", type=str, default=None, help="MIP activity id (default is None)")
	parser.add_argument("--experiment_id", "-ei", dest="experiment_id", type=str, default=None, help="MIP experiment id (default is None)")
	parser.add_argument("--start_date", "-sd", dest="start_date", type=str, default=None, help="Start date in YYYY-MM-DD format (default is None)")
	parser.add_argument("--end_date", "-ed", dest="end_date", type=str, default=None, help="End date in YYYY-MM-DD format (default is None)")
	parser.add_argument("--output", "-o", dest="output", type=str, default=os.path.curdir, help="Output directory (default is current directory)")
	parser.add_argument("--ymax", dest="ymax", type=int, default=None, help="Maximum of y-axis for data footprint plot (default is None)")
	parser.add_argument("--ymin", dest="ymin", type=int, default=None, help="Minimum of y-axis for data footprint plot (default is None)")
	parser.add_argument("--cumulative", dest="cumulative", action='store_true', help="Get cumulative data footprint of datasets over time (default is False)")
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
	
	gen_plot(args.project, start_date, end_date, args.ymin, args.ymax, args.activity_id, args.experiment_id, args.cumulative, args.output)


if __name__ == '__main__':
	main()
