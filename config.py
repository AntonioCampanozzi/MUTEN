import os

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

DATA_DIR = os.path.join(PROJECT_ROOT, "event_log")
INITIAL_SENTENCE = "activity <concept:name> executed by <org:resource>,\n"
SENTENCE = " followed by activity <concept:name> executed by <org:resource>,\n"
FINAL_SENTENCE = "final activity <concept:name> executed by <org:resource>."