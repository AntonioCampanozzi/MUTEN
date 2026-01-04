import time
import config
import pm4py
import argparse
import df_manipulation
import embedding
from timestamp_embedder import TimeEmbedder

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
time_deltas = df_manipulation.get_time_deltas(df_group)

max_sequence_length = int(time_deltas.groupby("case:concept:name").size().max())
print("Max sequence length: ", max_sequence_length)

print(df_group['time:timestamp'].head(1))