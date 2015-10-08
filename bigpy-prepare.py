#!/usr/bin/python

# $Id$
# File: bigpy-prepare.cpp
# Created: September 17, 2015
# Authors: Paul Kowalski <paulkowa@buffalo.edu>
#         Dhanasekar Karuppasamy <dhanasek@buffalo.edu>
# Copyright (c) 2015-2016 Paul Kowalski, Dhanasekar Karuppasamy

# Distributed under the MIT License.
# See accompanying file LICENSE_MIT.txt.
# This file is part of BiGPy.



from Bio import SeqIO
from optparse import OptionParser
from collections import namedtuple
import timeit
import fileinput
import re
import binascii
import os


# Handle command line arg options
def setUp():
    parser = OptionParser()
    parser.add_option("-i", "--input", action="store", type="string", dest = "input", default = "none", help = "{FILE|DIR} to read input sequence list from", metavar = "{FILE|DIR}")
    parser.add_option("-o", "--output", action="store", type="string", dest = "output", default = "bigpy", help = "{FILE|DIR} to print results to", metavar = "{FILE|DIR}")
    parser.add_option("-l", "--length", action="store", type="int", dest = "length", default = 100, help = "Filter sequences shorter than SIZE", metavar = "SIZE")
    parser.add_option("-d", "--dna", action="store", dest = "type", default = True, help = "DNA/RNA or amino acid sequences supported\nTrue = DNA\nFalse = amino acid", metavar = "{True|False}")
    parser.add_option("-c", "--clean", action="store", dest = "clean", default = True, help = "Remove sequences that contain invalid characters", metavar = "{True|False}")
    (options, args) = parser.parse_args()

    if options.input == "none":
        parser.error("Input file or directory required\nUse -h or --help for options")
    if options.output.find("/") != -1:
        parser.error("Currently BiGPY only supports printout to same directory\nPlease only provide a filename without extension (ie. myfile)")
    return options

# Run and print all information
def run(options):
    fileList = []
    print "scanning " + options.input + " for input files..."
    files = checkDir(options, fileList)
    print "found " + str(files) + " input file(s)\nextracting sequences..."
    start_time = timeit.default_timer()
    statStruct = parse(options, fileList)
    elapsed = timeit.default_timer() - start_time
    print "valid " + statStruct.validSeqs + " out of " + statStruct.countSeqs + " sequences\nwriting output files...\n" + \
        "shortest sequence: " + statStruct.shortSeq + "\nlongest sequence: " + statStruct.longSeq + "\naverage sequence: " + statStruct.avgSeq + \
        "\nmedian sequence: " + statStruct.medSeq + "\ntime: " + str(elapsed) + " seconds\ndone!"


# Check input directory for all fasta / fsa files
def checkDir(options, fileList):
    inputFiles = 0
    if options.input[-1:] == '/':
        for file in os.listdir(options.input):
            if file.endswith(".fsa") or file.endswith(".fasta"):
                fileList.append(str(file))
                inputFiles += 1
    else:
        fileList.append(options.input)
        inputFiles += 1
    return inputFiles


# Parse the input file and print to output file cleaned sequences
def parse(options, fileList):
    # Open output files
    seqFile = open(options.output + "Seqs.seq", 'w+')
    cleanFile = open(options.output + "Clean.txt", "w+")
    mapFile = open(options.output + "Map.fsa", "w+")
    removedSeqs = open(options.output + "Removed.fsa", "w+")

    # Create statistic storage
    statStruct = namedtuple('statStruct', ['countSeqs', 'validSeqs', 'shortSeq', 'longSeq', 'avgSeq', 'medSeq'])
    count, valid, shortest, longest, avg, median = (0,) * 6

    # Iterate through input and write to output files
    for f in fileList:
        if options.input[-1:] == '/':
            inFile = open(options.input + f, 'rU')

        else:
            inFile = open(f, 'rU')

        for record in SeqIO.parse(inFile, "fasta"):
            # Record longest and shorest sequences
            if count == 0:
                shortest = len(record.seq)
                longest = len(record.seq)
                avg = len(record.seq)
            elif len(record.seq) < shortest:
                shortest = len(record.seq)
            elif len(record.seq) > longest:
                longest = len(record.seq)
            avg = average(len(record.seq), count + 1, avg)

            # Process sequence                
            if options.clean:
                if (len(record.seq) > options.length and checkAlphabet(str(record.seq), options)):
                    seqFile.write(str(bin(count)) + '\t' + text_to_bi(str(record.seq)) + '\n')
                    cleanFile.write(str(bin(count)) + '\t' + str(record.seq) + '\n')
                    mapFile.write(str(count) + '\t' + str(record.description) + '\n')
                    valid += 1
                else:
                    removedSeqs.write(str(record.description) + '\n' + str(record.seq) + '\n')
                
            else:
                if (len(record.seq) > options.length):
                    seqFile.write(str(bin(count)) + '\t' + text_to_bi(str(record.seq)) + '\n')
                    cleanFile.write(str(bin(count)) + '\t' + str(record.seq) + '\n')
                    mapFile.write(str(count) + '\t' + str(record.description) + '\n')
                else:
                    removedSeqs.write(str(record.description) + '\n' + str(record.seq) + '\n')
                valid += 1
            count += 1

        inFile.close()

    seqFile.close()
    cleanFile.close()
    mapFile.close()
    removedSeqs.close()

    return statStruct(str(count), str(valid), str(shortest), str(longest), str(avg), str(median))

# Compute rolling average
def average(length, count, avg):
    if count == 1:
        return avg
    else:
        return (avg + ((length - avg) / (count + 1)))

# Checks if sequence contains only valid characters
def checkAlphabet(seq, options):
    # Check if valid DNA sequence
    if options.type:
        if re.match("^[ACTG]*$", seq):
            return True

    # Check if valid protein sequence
    else:
        if re.match("^[ACDEFGHIKLMNPQRSTVWY]*$", seq):
            return True
    return False

# Convert string to binary
def text_to_bi(text, encoding='utf-8', errors='surrogatepass'):
    bits = bin(int(binascii.hexlify(text.encode(encoding, errors)), 16))[2:]
    return bits.zfill(8 * ((len(bits) + 7) // 8))

# Convert binary to string
def text_from_bi(bits, encoding='utf-8', errors='surrogatepass'):
    n = int(bits, 2)
    return int2bytes(n).decode(encoding, errors)

# Break binary sequence into bytes for conversion to ACSII
def int2bytes(i):
    hex_string = '%x' % i
    n = len(hex_string)
    return binascii.unhexlify(hex_string.zfill(n + (n & 1)))

# Main
def main():
    run(setUp())

if __name__ == "__main__":
    main()
