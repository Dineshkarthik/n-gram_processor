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


def get_frequency(filename, selected_index, selector_index, selector, distance,
                  case_sensitivity):
    """Using ngram get required words and frequency.

    Check given text file for words present in a specific order at specific
    distance from a word in n-gram.

    Parameters
    ----------
    filename: string
        Name of the text file to be processed.
    selected_index: int
        Index in ngram where the required word will be present.
    selector_index: int
        Index in ngram where the selector will be present.
    selector: string
        Word from which the distance is calculated.
    distance: int
        Distance between selector and the word ot be selected.
    case_sensitivity: string
        To determine whether the texts are case_sensitive or not

    Returns
    -------
    list
        contains tuples with selected word and frequency.
    """
    textfile = sc.textFile(filename)
    tokens = textfile.map(
        str.split) if case_sensitivity == 'yes' else textfile.map(
            lambda x: x.lower()).map(str.split)
    counters = tokens.flatMap(sliding_window(distance)).filter(
        lambda x: x[selector_index] == selector).map(
            lambda x: x[selected_index]).map(lambda x: (x, 1)).reduceByKey(
                lambda x, y: x + y).sortBy(lambda x: -x[1]).collect()
    return counters


def get_words(filename, selector, distance, selector_index, case_sensitivity):
    """Using ngram get required set of words.

    Check given text file for words present in a specific order at specific
    distance from a word in n-gram.

    Parameters
    ----------
    filename: string
        Name of the text file to be processed.
    selector: string
        Word from which the distance is calculated.
    distance: int
        Distance between selector and the word ot be selected.
    selected_index: int
        Index in ngram where the required word will be present.
    case_sensitivity: string
        To determine whether the texts are case_sensitive or not.

    Returns
    -------
    list
        contains seelcted set of words(part of sentence).
    """
    textfile = sc.textFile(filename)
    tokens = textfile.map(
        str.split) if case_sensitivity == 'yes' else textfile.map(
            lambda x: x.lower()).map(str.split)
    words = tokens.flatMap(sliding_window(distance)).filter(
        lambda x: x[selector_index] == selector).map(
            lambda x: ' '.join(x)).collect()
    return words


if config["order"] == 'right':
    selector_index, selected_index = 0, config["distance"] - 1
else:
    selector_index, selected_index = config["distance"] - 1, 0

for root, dirs, files in os.walk(config['path']):
    for file in files:
        filename = os.path.join(root, file)
        print("Processing: " + filename)
        if config["function"] == 'frequency':
            result = get_frequency(filename, selected_index, selector_index,
                                   config["selector"], config["distance"],
                                   config["case_sensitive"])
        else:
            result = get_words(filename, config["selector"],
                               config["distance"], selector_index,
                               config["case_sensitive"])
        if len(result) > 0:
            out_ = filename.replace(config['path'], '')
            outfile = os.path.splitext(os.path.join(config['outpath'], out_))[
                0] + ".txt"
            if not os.path.exists(os.path.dirname(outfile)):
                os.makedirs(os.path.dirname(outfile))
            with open(outfile, "w+") as file:
                file.write(str(result))
