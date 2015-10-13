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

import logging
import mmh3
import pprint
from optparse import OptionParser
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from utils import timeit

pp = pprint.PrettyPrinter(indent=4)

SPARK_APP_NAME = "BitPyElasticSketch"

def kmerize(seq, kmer_length):
    '''
    Creates kmers for a given string @seq with @k as n.
    TODO :
        * Change the implementation to use generators
    '''
    kmers = [ ]
    for i in xrange(len(seq) - (kmer_length-1)):
        kmers.append((i, seq[i:i+kmer_length]))
    return kmers

def create_kmer_tuple(astring, kmer_length):
   '''
   For a string @astring, create a tuple (seq_id, kmers_index , hash)
   '''
   seq_id, seq = astring.split("\t")
   kmer_tuple = []
   for k in kmerize(seq, kmer_length):
       # k is a tuple of (index, kmer_string)
       kmer_tuple.append((seq_id, mmh3.hash64(k[1])[0], k[0]))
   return kmer_tuple

def sketch(options):
    '''
    Sketch logic
    '''
    spark_context = SparkContext(appName=SPARK_APP_NAME, \
                              master=options.spark_master)
    fastaRDD = spark_context.textFile(options.input)
    kmerRDD = fastaRDD.flatMap(lambda v : create_kmer_tuple(v, options.kmer_length))
    pp.pprint(kmerRDD.collect())

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
    parser.add_option("-k", "--kmerlength", \
        action="store", \
        type="int", \
        dest="kmer_length", \
        default=None, \
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
    if not int(options.kmer_length):
        parser.error("KMER length required.\n"+
                      "Use -h or --help for options.\n")
    return options


@timeit
def main():
    '''
    Get the File names for I/O and run the sketch phase.
    '''
    options = setup()
    sketch(options)

if __name__ == "__main__":
    main()
