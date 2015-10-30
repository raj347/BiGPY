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
from optparse import OptionParser
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from utils import timeit
sys.path.append(dirname(os.getcwd()[0:-3] + "include/mmh3-2.0/"))
import mmh3
pp = pprint.PrettyPrinter(indent=4)
pp.pprint(sys.path)
SPARK_APP_NAME = "BiGPyElasticSketch"

def int64_to_uint64(i):
    return ctypes.c_uint64(i).value

def gen_kmers(input, options):
  '''
  Generate list of kmers for input sequence of length k
  '''
  kmers = []
  for i in xrange(len(input) - (options.kmer - 1)):
    kmers.append(str(input[i:options.kmer + i]))
  # pp.pprint(kmers)
  return kmers
  # input[0:options.kmer]

def map_sketch(input, options):
  '''
  Returns a list of sketches (x,r,d)
  x = hashed sketch
  r = ID of original sequence
  d = count of kmers extracted
  '''
  id_seq = input.split('\t')
  # pp.pprint(type(id_seq[0]))
  sketches = [(mmh3.hash64(i)[0], int(id_seq[0]), len(id_seq[1]) - options.kmer + 1) for i in gen_kmers(id_seq[1], options)]
  # if (mmh3.hash64(i)[0] % options.mod == 0)
  return sketches

def sketch(options, spark_context):
    '''
    Read input file into rdd
    Generate RDD of sketches
    Filler sketch RDD to only contain those of % == 0
    '''
    fsaRDD = spark_context.textFile(options.input)
    sketchRDD = fsaRDD.flatMap(lambda s: map_sketch(s, options))
    modRDD = sketchRDD.filter(lambda s: s[0] % options.mod == 0)
    # pp.pprint(os.getcwd()[0:-3] + "include/")
    
    # Print first 5 items in fsaRDD
    pp.pprint("fsaRDD FIRST SEQUENCE")
    pp.pprint(fsaRDD.take(1))
    pp.pprint("sketchRDD sketchs")
    pp.pprint(sketchRDD.take(11))
    pp.pprint("modRDD after Filter")
    pp.pprint(modRDD.take(6))
    

    '''
    pp.pprint(mmh3.hash64('ABAAAAA'))
    pp.pprint((mmh3.hash64('ABAAAAA')[1]))
    pp.pprint(numpy.uint64(mmh3.hash64('ABAAAAA')[1]))
    pp.pprint(mmh3.hash64('ABAAAAA'))
    pp.pprint(int64_to_uint64(mmh3.hash64('ABAAAAA')[0]))
    pp.pprint(int64_to_uint64(mmh3.hash64('ABAAAAA')[1]))
    pp.pprint(mmh3.hash64('ABAAABA'))
    pp.pprint(int64_to_uint64(mmh3.hash64('ABAAABA')[0]))
    pp.pprint(-1 * mmh3.hash64('ABAAABA')[1])
    '''
    # pp.pprint(sketchRDD.take(1))

    # infile = spark_context.wholeTextFiles(options.input)
    # pp.pprint(infile.take(5))
    # rdd = spark_context.parallelize(infile.collect())
    # rdd.saveAsSequenceFile('testSEQ')

    # fsaRDD = spark_context.sequenceFile('testSEQ')
    # pp.pprint(fsaRDD.take(1))    

    # fsaRDD.saveAsSequenceFile(options.input)
    # sketchRDD = fsaRDD.flatMap()



    # kmerRDD = fastaRDD.flatMap(lambda v : create_kmer_tuple(v, options.kmer_length))
    
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
        default="N/A", \
        help="Sketch phase output file a name without extension",\
        metavar="FILE")
    parser.add_option("-k", "--kmer", \
        action="store", \
        type="int", \
        dest="kmer", \
        default=-1, \
        help="Lenght of the KMERs")
    parser.add_option("--mod", \
        action="store", \
        type="int", \
        dest="mod", \
        default=2, \
        help="Lenght of the KMERs")
    parser.add_option("-p", \
        action="store", \
        type="int", \
        dest="nodes", \
        default=3, \
        help="Lenght of the KMERs")
    parser.add_option("-m", "--master", \
        action="store", \
        type="string", \
        dest="spark_master", \
        default="local[*]", \
        help="Spark Master node. Defauls to \"local[*]\"")

    (options, args) = parser.parse_args()

    # Print error messages if required options are not provided
    if not options.input:
        parser.error("Input file required.\n"+
                      "Use -h or --help for options.\n")
    if options.output.find('/') != -1 or options.output.find('.') != -1:
        parser.error("Output filename required.\n" +\
                     "Please include a filename without extension.\n" +\
                     "Use -h or --help for options\n")
    if options.kmer == -1:
        parser.error("KMER length required.\n"+
                      "Use -h or --help for options.\n")
    return options


@timeit
def main():
    '''
    Get the File names for I/O and run the sketch phase.
    '''
    sys.path.append(dirname(os.getcwd()[0:-3] + "include/mmh3"))
    options = setup()
    spark_context = SparkContext(appName=SPARK_APP_NAME, \
                              master=options.spark_master)
    sketch(options, spark_context)

if __name__ == "__main__":
    main()
