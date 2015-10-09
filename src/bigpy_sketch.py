#!/usr/bin/python

'''
File: bigpy-prepare.py
Created: September 17, 2015
Authors: Paul Kowalski <paulkowa@buffalo.edu>
         Dhanasekar Karuppasamy <dhanasek@buffalo.edu>
Copyright (c) 2015-2016 Paul Kowalski, Dhanasekar Karuppasamy

Distributed under the MIT License.
See accompanying file LICENSE_MIT.txt.
This file is part of BiGPy.
'''

from optparse import OptionParser
import pprint

pp = pprint.PrettyPrinter(indent=4)

def setup():
    '''
    Handle command line arguments.
    '''
    parser = OptionParser()
    parser.add_option("-i", "--input", \
        action="store", \
        type="string", \
        dest="input", \
        default="N/A", \
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

    (options, args) = parser.parse_args()

    # Print error messages if required options are not provided
    if options.input == "N/A":
        parser.error("Input file required.\n"+
                      "Use -h or --help for options.\n")
    if options.output.find('/') != -1 or options.output.find('.') != -1:
        parser.error("Output filename required.\n" +\
                     "Please include a filename without extension.\n" +\
                     "Use -h or --help for options\n")
    return options


def run(options):
    '''
    Sketch Phase implementation
    '''

def main():
    '''
    Get the File names for I/O and run the sketch phase.
    '''
    options = setup()
    run(options)

if __name__ == "__main__":
    main()
