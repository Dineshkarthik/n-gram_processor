"""Pyspark Script to get count of words in a distance from selector."""
from pyspark import SparkContext, SparkConf
from toolz.curried import sliding_window
import os
import yaml

conf = SparkConf().setAppName("ngramsCollector").setMaster("local")
sc = SparkContext(conf=conf)

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
f = open(os.path.join(THIS_DIR, 'config.yaml'))
config = yaml.safe_load(f)
f.close()


def collect_words(filename, selected_index, selector_index, selector,
                  distance):
    """Using ngram get required set of words.

    Check given text file for words present in a specigic order at specific
    distance from a word in n-gram.

    Parameters
    ----------
    filename: string
        Name of the text file to be processed.
    selected_index: int
        Index in ngram where the required word will be present.
    selector_index: int
        Index in ngram where the selector will be present.
    distance: int
        Distance between selector and the word ot be selected

    Returns
    -------
    list
        contains tuples with selected word and frequency.
    """
    textfile = sc.textFile(filename)
    tokens = textfile.map(lambda x: x.lower()).map(str.split)
    counters = tokens.flatMap(sliding_window(distance)).filter(
        lambda x: x[selector_index] == selector).map(
            lambda x: x[selected_index]).map(lambda x: (x, 1)).reduceByKey(
                lambda x, y: x + y).sortBy(lambda x: -x[1]).collect()
    return counters


if config["order"] == 'right':
    selector_index, selected_index = 0, config["distance"] - 1
else:
    selector_index, selected_index = config["distance"] - 1, 0

for root, dirs, files in os.walk(config['path']):
    for file in files:
        filename = os.path.join(root, file)
        print("Processing: " + filename)
        result = collect_words(filename, selected_index, selector_index,
                               config["selector"], config["distance"])
        if len(result) > 0:
            out_ = filename.replace(config['path'], '')
            outfile = os.path.splitext(os.path.join(config['outpath'], out_))[
                0] + ".txt"
            if not os.path.exists(os.path.dirname(outfile)):
                os.makedirs(os.path.dirname(outfile))
            with open(outfile, "w+") as file:
                file.write(str(result))
