# MUTEN: MUltiview Trace ENcodings

This repository is a fork of the code associated with the work  
[*DOROTHY*](https://ieeexplore.ieee.org/document/11220731), which addresses the problem of extracting excessively complex process models from non-Pareto event logs—a phenomenon known as **spaghetti-like process models**.

This type of event log is characterized by very high variance, and the extracted Petri nets tend to exhibit structures that are difficult to interpret. As a result, they fail to meet their primary objective: providing a concise and intuitive representation of a large set of process instances.

## Background

**MUTEN** explores one of the future research directions outlined in *DOROTHY*, namely the integration of **additional views** into the extraction of representative traces.

In traditional process discovery, the focus is generally limited to the **control-flow perspective**, while other available information is often ignored. This is because process models have historically been designed to describe only the sequence of executed activities.

## Research Objective

The research goal of **MUTEN** is to analyze how the extraction of process models changes when also incorporating:

- **Resource-related information**
- **Temporal characteristics of traces**

By enriching the discovery process with these additional dimensions, MUTEN aims to improve the expressiveness and interpretability of the resulting process models.

# How to use
Process Discovery using **MUTEN**
- event_log: event log name
- miner: ILP miner (ilp), Inductive Miner (im) or Split Miner (sm)

```
python -m main.py -event_log production -miner im
```
