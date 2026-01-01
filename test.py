import time
import config
import pm4py
import argparse
import df_manipulation
import embedding

parser = argparse.ArgumentParser(description='Process Discovery using DOROTHY')

print(config.DATA_DIR)
print(config.PROJECT_ROOT)

parser.add_argument('-event_log', type=str, help="Event log name")
args = parser.parse_args()
dataset = args.event_log

eventlog_df = pm4py.read_xes(config.DATA_DIR + "\\"+ dataset + ".xes")

df_group = eventlog_df.groupby("case:concept:name", sort=False)

filtered_df = df_manipulation.filterlog(df_group, "concept:name", "org:resource")

df_group = filtered_df.groupby("case:concept:name", sort=False)
traces_df = df_manipulation.get_traces(df_group)

df_group = traces_df.groupby("traces", sort=False)
variant_traces_df = df_manipulation.get_variant_traces(traces_df.groupby("traces", sort=False))

variant_traces_embeddings = embedding.get_variants_embeddings(variant_traces_df["traces"].to_list())