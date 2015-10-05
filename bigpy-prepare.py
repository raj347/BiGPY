'''
$Id$
File: bigpy-prepare.cpp
Created: September 17, 2015

Authors: Paul Kowalski <paulkowa@buffalo.edu>
         Dhanasekar Karuppasamy <dhanasek@buffalo.edu>
Copyright (c) 2015-2016 Paul Kowalski, Dhanasekar Karuppasamy
Distributed under the MIT License.
See accompanying file LICENSE_MIT.txt.

This file is part of BiGPy.
 '''
#!/usr/bin/python

from Bio import SeqIO
from optparse import OptionParser
import fileinput
import re
import binascii

#def info(options):


# Handle command line arg options
def setUp():
    parser = OptionParser()
    
    parser.add_option("-i", "--input", dest = "input", help = "FILE to read input sequence list from", metavar = "FILE")
    parser.add_option("-o", "--output", dest = "output", default = "bigpy", help = "FILE to print results to", metavar = "FILE")
    parser.add_option("-l", "--length", dest = "length", default = 100, help = "Filter sequences shorter than SIZE", metavar = "SIZE")
    parser.add_option("-d", "--dna", dest = "type", default = True, help = "DNA/RNA or amino acid sequences supported\nTrue = DNA\nFalse = amino acid", metavar = "{True|False}")
    parser.add_option("-c", "--clean", dest = "clean", default = True, help = "Remove sequences that contain invalid characters", metavar = "{True|False}")
    
    (options, args) = parser.parse_args()
    return options


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


# Parse the input file and print to output file cleaned sequences
def parse(options):
    inFile = open(options.input, 'rU')
    seqFile = open(options.output + "Seqs.seq", 'w+')
    cleanFile = open(options.output + "Clean.txt", "w+")
    mapFile = open(options.output + "Map.fsa", "w+")
    removedSeqs = open(options.output + "Removed.fsa", "w+")

    # Iterate through input and write to output files
    for record in SeqIO.parse(inFile, "fasta"):
        if options.clean:
            if (len(record.seq) > options.length and checkAlphabet(str(record.seq), options)):
                seqFile.write(text_to_bi(str(record.seq)) + '\n')
                cleanFile.write(str(record.seq) + '\n')
                mapFile.write(str(record.id) + '\n' + str(record.seq) + '\n')
            else:
                removedSeqs.write(str(record.id) + '\n' + str(record.seq) + '\n')
                
        else:
            if (len(record.seq) > options.length):
                seqFile.write(text_to_bi(str(record.seq)) + '\n')
                cleanFile.write(str(record.seq) + '\n')
                mapFile.write(str(record.id) + '\n' + str(record.seq) + '\n')

    inFile.close()
    seqFile.close()


# Main
def main():

    options = setUp()
    info(options)
    parse(options)

if __name__ == "__main__":
    main()
