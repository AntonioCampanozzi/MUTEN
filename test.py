import time
import pandas as pd
import config
import pm4py
import argparse
import df_manipulation
import embedding
import numpy as np

parser = argparse.ArgumentParser(description='Process Discovery using DOROTHY')

parser.add_argument('-event_log', type=str, help="Event log name")
args = parser.parse_args()
dataset = args.event_log

eventlog_df = pm4py.read_xes(config.DATA_DIR + "\\"+ dataset + ".xes")

df_group = eventlog_df.groupby("case:concept:name", sort=False)

filtered_df = df_manipulation.filterlog(df_group, "concept:name", "org:resource")

df_group = filtered_df.groupby("case:concept:name", sort=False)

time_deltas_df = df_manipulation.get_time_deltas(df_group)

time_deltas_df['time:timestamp'] = embedding.log_scale_time_deltas(time_deltas_df['time:timestamp'].values)

sentence_df = df_manipulation.get_traces(time_deltas_df.groupby("case:concept:name", sort=False))

variant_df=df_manipulation.get_variant_traces(sentence_df.groupby("traces", sort=False))

print(variant_df.head(10))

'''BERT_embedings = embedding.get_sentence_embeddings(variant_df['traces'].tolist()[0:5])
print(BERT_embedings[0].shape)

print(BERT_embedings)

print(variant_df.head(10))

coeff= embedding.get_time_embeddings(time_deltas['time:timestamp'].tolist()[0:5])

final_embeddings = embedding.concat_embeddings(BERT_embedings, coeff)

print(final_embeddings.shape)

print(final_embeddings)'''