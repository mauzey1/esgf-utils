from __future__ import print_function
import requests
import os
import json
import glob
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

    pivot_key = js['facet_counts']['facet_pivot'].keys()[0]
    start_pivot = js['facet_counts']['facet_pivot'][pivot_key]

    def __pivot(piv):
        pd = {}
        for p in piv:
            if "pivot" in p:
                pd[p["value"]] = __pivot(p["pivot"])
            else:
                pd[p["value"]] = p["count"]
        return pd

    return __pivot(start_pivot)

# Count the number of entries at the innermost level of the nested dictionary 
# (i.e., sum of all models listed under all table/variable/experiment entries).
def count_models_per_exp(dataset_counts):
    table_dict = {}
    for table_id, variables in dataset_counts.iteritems():
        var_dict = {}
        for var_id, experiments in variables.iteritems():
            model_counts = {}
            for exp_id, models in experiments.iteritems():
                model_counts[exp_id] = len(models)
            var_dict[var_id] = model_counts
        table_dict[table_id] = var_dict

    return table_dict


# Count the number of entries at the 2nd level of the nested dictionary 
# that host at least 1 experiment with 5 or more models contributing.  
# (i.e., how many of the ~2000 table/variable entries indicate that for at least 1 experiment, 
# at least 5 models were able/willing to provide the requested output?)
def count_vars_with_5modelexps(dataset_counts):
    variable_counts = {}
    for table_id, variables in dataset_counts.iteritems():
        var_count = 0
        for var_id, experiments in variables.iteritems():
            exp_has_5_models = False
            for exp_id, models in experiments.iteritems():
                if len(models) >= 5:
                    exp_has_5_models = True
            if exp_has_5_models:
                var_count += 1
        if var_count > 0:
            variable_counts[table_id] = var_count

    return variable_counts


# how many variables would fail to meet a threshold of 3 models
def count_vars_with_lessthan3models(dataset_counts):
    variable_counts = {}
    for table_id, variables in dataset_counts.iteritems():
        var_count = 0
        for var_id, experiments in variables.iteritems():
            unique_models = set()
            for exp_id, models in experiments.iteritems():
                unique_models.update(list(models.keys()))
            if len(unique_models) < 3:
                var_count += 1
        if var_count > 0:
            variable_counts[table_id] = var_count

    return variable_counts


# how many variables were never reported 
# (even by a single model for a single experiment)
def count_vars_not_reported(dataset_counts, project_tables):
    variable_counts = {}
    for table_id, table_data in project_tables.iteritems():
        table_vars = table_data['variable_entry'].keys()

        if table_id in dataset_counts:
            var_ids = dataset_counts[table_id].keys()
            var_count = sum([1 if x not in var_ids else 0 for x in table_vars])
        else:
            var_count = len(table_vars)

        if var_count > 0:
            variable_counts[table_id] = var_count

    return variable_counts


def main():

    parser = argparse.ArgumentParser(description="Generate dataset statistics for a project in ESGF")
    parser.add_argument("--project", "-p", dest="project", type=str, default="CMIP6", help="MIP project name (default is CMIP6)")
    parser.add_argument("--count-missing-vars", dest="count_missing_vars", action='store_true', help="Count variables in table not found in index")
    parser.add_argument("--output", "-o", dest="output", type=str, default=os.path.curdir, help="Output directory (default is current directory)")
    parser.add_argument("--tables", "-t", dest="cmor_tables", type=str, default="Tables", help="CMOR tables directory (default is \"Tables\")")
    args = parser.parse_args()

    if not os.path.isdir(args.output):
        print("{} is not a directory. Exiting.".format(args.output))
        return

    if args.count_missing_vars and not os.path.isdir(args.cmor_tables):
        print("{} is not a directory. Exiting.".format(args.cmor_tables))
        return

    dataset_counts = get_stats(args.project, "table_id", "variable_id", "experiment_id", "source_id")
    model_counts = count_models_per_exp(dataset_counts)
    variable_counts = count_vars_with_5modelexps(dataset_counts)
    vars_with_lessthan3models_counts = count_vars_with_lessthan3models(dataset_counts)

    stats_dict = { "number of datasets per table-variable-experiment-model": dataset_counts,
                   "number of models per table-variable-experiment": model_counts,
                   "number of variables per table with at least 1 experiment with =>5 models": variable_counts,
                   "number of variables per table with less than 3 models": vars_with_lessthan3models_counts
                 }

    if args.count_missing_vars:
        table_paths = glob.glob(os.path.join(args.cmor_tables, args.project+"_*.json"))
        
        project_tables = {}
        for path in table_paths:
            table_name = os.path.basename(path).replace("CMIP6_","").replace(".json","") 
            if table_name not in ["CV", "grids", "formula_terms", "coordinate", "input_example"]:
                with open(path) as f:
                    data = json.load(f)
                    project_tables[table_name] = data
        
        vars_not_reported_counts = count_vars_not_reported(dataset_counts, project_tables)

        stats_dict["number of variables per table not reported"] = vars_not_reported_counts

    path = os.path.join(args.output, args.project+'_model_stats.json')
    with open(path, 'w') as outfile:
        json.dump(stats_dict, outfile, indent=4)


if __name__ == '__main__':
    main()
