#!/usr/bin/python

'''
File: bigpy_sketch.py
Created: September 17, 2015
Authors: Paul Kowalski <paulkowa@buffalo.edu>
         Dhanasekar Karuppasamy <dhanasek@buffalo.edu>
Copyright (c) 2015-2016 Paul Kowalski, Dhanasekar Karuppasamy

Distributed under the MIT License.
See accompanying file LICENSE_MIT.txt.
This file is part of BiGPy.
'''
from __future__ import print_function
import logging
import pprint
import os, sys
from os.path import dirname
from optparse import OptionParser, Option, OptionValueError
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from utils import timeit
sys.path.append(dirname(os.getcwd()[0:-3] + "/gpfs/projects/jzola/paulkowa/BiGPY/include/mmh3-2.0/build/lib.linux-x86_64-2.7/"))
import mmh3
from metrics import dump_data

pp = pprint.PrettyPrinter(indent=4)
SPARK_APP_NAME = "BiGPyElasticSketch"

def gen_kmers(input, options):
  '''
  Generate list of kmers for input sequence of length k
  '''
  kmers = []
  for i in xrange(len(input) - (options.kmer - 1)):
    kmers.append((str(input[i:options.kmer + i]), i))
  #pp.pprint(kmers)
  return kmers
  # input[0:options.kmer]

def map_sketch(input, options):
  '''
  Returns a list of sketches (x,r, pos, d)
  x = hashed sketch
  r = ID of original sequence
  pos = index of kmer starting position in original sequence
  d = count of kmers extracted
  '''
  id_seq = input.split('\t')
  # pp.pprint(type(id_seq[0]))
  sketches = [(mmh3.hash64(i[0])[0], (int(id_seq[0]), i[1], len(id_seq[1]) - options.kmer + 1)) for i in gen_kmers(id_seq[1], options)]
  # if (mmh3.hash64(i)[0] % options.mod == 0)
  return sketches


# Combines input pairs of sequences with matching hashes
def combine_pairs(input, options):
    output = []
    # Iterate to each n sequence ID
    for n in range(0, len(input) - 1, 3):
        # Iterate through each sequence ID after the nth ID
        for i in range(n + 3, len(input) -1, 3):
            if i != len(input) - 2 and input[n] != input[i]:
                if input[n] < input[i]:
                    output.append(((input[n], input[i], min(input[n + 2], input[i + 2])), (input[n + 1], input[i + 1])))
                else:
                    output.append(((input[i], input[n], min(input[n + 2], input[i + 2])), (input[i + 1], input[n + 1])))
    return output

def sketch(options, spark_context, master):

    '''
    Read input file into rdd
    Generate RDD of sketches
    Filler sketch RDD to only contain those of % == 0
    '''

    # Read from file and generate RDD of all sketches from original sequences
    # (Hash, (id, pos, count))
    sketchRDD = spark_context.textFile(options.input).flatMap(lambda s: map_sketch(s, options))
    sketchRDD.persist()

    for i in range(0, options.iter):
        #Steps in order
        # Filter (mod - i) sketches from sketchRDD => (Hash, (id, pos, count))
        # Reduce sampled sketches => (Hash, (id1, pos1, count1, id2, pos2, count2, ...))
        # Filter the reduced subset to remove singletons
        # Map subset to (id(j), id(k), count(min(j,k)), (pos(j), pos(k))) for pairs that share a sketch
        # Reduce => ((id(j), id(k), count(min(j,k)), (List of all positions of shared kmers))
        # Map, set value to containment value => (id(j), id(k), C(min(j,k)), count)
        modRDD = sketchRDD.filter(lambda s: s[0] % (options.mod - i) == 0).reduceByKey(lambda a, b: a + b).filter(lambda v: len(v[1]) > 3).flatMap(lambda v: combine_pairs(v[1], options)).reduceByKey(lambda a, b: a + b).map(lambda a: (a[0], (int(float((len(a[1]) / 2)) / float(a[0][2]) * 100))))
        if i == 0:
            # Set totalRDD to modRDD for first iteration
            totalRDD = modRDD
            totalRDD.persist()
        else:
            # Create the union of totalRDD and modRDD for each following iteration
            totalRDD = totalRDD.union(modRDD)

    # Materialize totalRDD and print sketches that share edges
    # Not entirely sure why but if Reduce and filter are called in seperate lines from take, this operation does not work.
    pp.pprint(totalRDD.reduceByKey(lambda a, b: a + b).filter(lambda a: a[1] > options.jmin).take(20))

    #Remove spark:// and port in the end of the master url
    pp.pprint(master)
    master = master.split(":")[1][2:]
    dump_data("http://" + master + ":4040/api/v1",options.input)

    '''
    Old code
    Use at own risk
    '''
    # Sample mod m from list of sketches
    # (Hash, (id, pos, count))
    # modRDD = sketchRDD.filter(lambda s: s[0] % (options.mod - i) == 0)

    # Reduce sampled sketches to (Hash, (id1, pos1, count1, id2, pos2, count2, ...))
    # Filter the reduced subset to remove singletons
    # Map subset to (id(j), id(k), count(min(j,k)), (pos(j), pos(k))) for pairs that share a sketch
    # Reduce to ((id(j), id(k), count(min(j,k)), (List of all positions of shared kmers))
    #redRDD = modRDD.reduceByKey(lambda a, b: a + b).filter(lambda v: len(v[1]) > 3).flatMap(lambda v: combine_pairs(v[1], options)).reduceByKey(lambda a, b: a + b)

    # Count number of occurances that a given pair shares kmers
    # (id(j), id(k), count(min(j,k)), count)
    #countRDD = redRDD.map(lambda a: (a[0], (int(float((len(a[1]) / 2)) / float(a[0][2]) * 100))))
    
    #totalRDD.reduceByKey(lambda a, b: a + b).filter(lambda a: a[1] < options.jmin)

    # Print first 5 items in fsaRDD
    #pp.pprint("sketchRDD sketchs")
    #pp.pprint(sketchRDD.take(20))
    #pp.pprint("modRDD after Filter")
    #pp.pprint(modRDD.take(20))
    #pp.pprint("redRDD after Reduce")
    #pp.pprint(redRDD.take(20))
    #pp.pprint("countRDD after Reduce")
    #pp.pprint(countRDD.take(20))
    #pp.pprint(totalRDD.take(20))
    # Reduce output values after all iterations    
    
    # Remove spark:// and port in the end of the master url
    #pp.pprint(master)
    #master = master.split(":")[1][2:]
    #dump_data("http://" + master + ":4040/api/v1",options.input)

    '''
    End old code
    '''

# Add node input for partitioning data
def setup():
    '''
    Handle command line arguments.
    '''
    parser = OptionParser()
    parser.add_option("-i", "--input", \
        action="store", \
        type="string", \
        dest="input", \
        default=None, \
        help="File to read input sequence list from.\
               This is the O/P file from Elastic Prepare Phase", \
        metavar="{FILE|DIR}")
    parser.add_option("-o", "--output", \
        action="store", \
        type="string", \
        dest="output", \
        default=None, \
        help="Sketch phase output file a name without extension",\
        metavar="FILE")
    parser.add_option("-k", "--kmer", \
        action="store", \
        type="int", \
        dest="kmer", \
        default=-1, \
        help="Length of the KMERs")
    parser.add_option("--mod", \
        action="store", \
        type="int", \
        dest="mod", \
        default=2, \
        help="Value used for sampling kmers to be compared")
    parser.add_option("-j", "--jmin", \
        action="store", \
        type="int", \
        dest="jmin", \
        default=10, \
        help="Minimal number of kmer matches required for a sequence to be compared")
    parser.add_option("--iter", \
        action="store", \
        type="int", \
        dest="iter", \
        default=1, \
        help="Number of iterations")
    parser.add_option("-m", "--master", \
        action="store", \
        type="string", \
        dest="spark_master", \
        default="local[*]", \
        help="Spark Master node. Defauls to \"local[*]\"")

    (options, args) = parser.parse_args()

    # Print error messages if required options are not provided
    if not options.input:
        raise OptionValueError("\n\tInput file required" \
            "\n\tUse -h or --help for options")
    if not options.output:
        raise OptionValueError("\n\tOutput file required" \
            "\n\tUse -h or --help for options")
    if options.output.find('.') != -1 or options.output[-1:] == "/":
        raise OptionValueError("\n\tOutput filename required." \
            "\n\tPlease include a filename without extension (ie. bigpy)" \
            "\n\tUse -h or --help for options")
    if options.kmer == -1:
        parser.error("\n\tKMER length required." \
            "\n\tUse -h or --help for options.")
    return options

def setOutput(options):
    if options.output.find('/') != -1:
        index = options.output.rfind('/')
        path = options.output[0:index + 1]
        output = options.output[index + 1:]
        os.chdir(path)
        return output
    return options.output

@timeit
def main():
    '''
    Get the File names for I/O and run the sketch phase.
    '''
    sys.path.append(dirname(os.getcwd()[0:-3] + "include/mmh3"))
    options = setup()
    #setOutput(options)
    spark_context = SparkContext(appName=SPARK_APP_NAME, \
                              master=options.spark_master)
    sketch(options, spark_context, options.spark_master)

if __name__ == "__main__":
    main()
