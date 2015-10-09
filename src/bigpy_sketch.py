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
import pprint
from optparse import OptionParser
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from utils import timeit

pp = pprint.PrettyPrinter(indent=4)

class BigPyElasticSketch:
    '''
    Implements the Elastic Sketch phase.
    TODO :
       Implement logging
    '''

    SPARK_APP_NAME = "BitPyElasticSketch"

    def __init__(self, options):
        '''
        Initialize sketch phase.
        '''
        self.input = options.input
        self.output = options.output
        self.kmer_length = int(options.kmer_length)
        self.spark_master = options.spark_master
        self.spark_context = None

    def kmerize(self, seq):
        '''
        Creates kmers for a given string @seq with @k as n.
        TODO : 
            * Change the implementation to use generators
        '''
        kmers = [ ]
        for i in xrange(len(seq) - (self.kmer_length-1)):
            kmers.append(seq[i:i+self.kmer_length])
        return kmers

    def init_spark(self):
        '''
        Create Spark context.
        '''
        try:
            self.spark_context = SparkContext(appName="PySparkElastic", \
                                          master=self.spark_master)
        except:
            raise "Could not initialize Spark context."

    def sketch(self):
        '''
        Sketch logic
        '''
        self.init_spark()



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
        type="string", \
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
    sketch_phase = BigPyElasticSketch(options)
    sketch_phase.sketch() 

if __name__ == "__main__":
    main()
