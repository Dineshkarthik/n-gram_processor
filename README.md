# **n-gram_processor**

Using n-gram get set of words and their frequency of occurrence  in given directory / sub-directory/ text file, which are present in a specific order at specific distance from a word.

### Installation

You need Apache Spark, Python 3.* , dependency packages - toolz, PyYaml   installed globally:

Install Spark using the [instructions](https://www.santoshsrinivas.com/installing-apache-spark-on-ubuntu-16-04/).

If Pysark is not using Python 3, Just set the environment variable:

    export PYSPARK_PYTHON=python3

Now Clone and install requirements:
```sh
$ git clone https://github.com/Dineshkarthik/n-gram_processor.git
$ cd n-gram_processor
$ pip install -r requirements.txt
$ spark-submit ngrams_collector.py
```
## Configuration ##

    path: '/path/to/your/input/'
    outpath: '/path/to/your/output/'
    function: 'frequency'
    order: 'right'
    distance: 3
    selector: 'python'

 - path - path to your input directory
 - outpath - where you wanted your output files
 - function - `frequency` or `words`
	 - frequency - returns frequency of words in particular distance.
		  ex: [('programming', 3), ('functions', 2)]
	 - words - returns set of words present in specified distance.
		 ex: ['python programming language', 'python function programming']
 - order - `left` or `right` 
	 - left - from left to right
	 - right - from right to left
 - distance - position of the word to be seleceted
 - selector - word from which the distance need to be calculated. 
