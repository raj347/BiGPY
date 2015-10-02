'''
File: seq_prepare.py
Created:September 17, 2015

Author: Paul Kowalski paulkowa@buffalo.edu
'''

from Bio import SeqIO
from optparse import OptionParser
import fileinput
import re

# Handle command line arg options
def setUp():
    parser = OptionParser()
    
    parser.add_option("-i", "--input", dest = "input", help = "FILE to read input sequence list from", metavar = "FILE")
    parser.add_option("-o", "--output", dest = "output", default = "clean_sequences.fsa", help = "FILE to print cleaned sequence list to", metavar = "FILE")
    parser.add_option("-l", "--length", dest = "length", default = 100, help = "Filter sequences shorter than SIZE", metavar = "SIZE")
    parser.add_option("-d", "--dna", dest = "type", default = True, help = "DNA/RNA or amino acid sequences supported\nTrue = DNA\nFalse = amino acid", metavar = "{True|False}")
    parser.add_option("-c", "--clean", dest = "clean", default = True, help = "Remove sequences that contain invalid characters", metavar = "{True|False}")
    
    (options, args) = parser.parse_args()
    return options

# Checks if sequence contains only valid characters
def checkAlphabet(seq, options):
    # Check if valid DNA sequence
    if options.type :
        if re.match("^[ACTG]*$", seq):
            return True
    # Check if valid protein sequence
    else:
        if re.match("^[ACDEFGHIKLMNPQRSTVWY]*$", seq):
            return True
    return False

# Parse the input file and print to output file cleaned sequences
def parse(options):
    outFile = open(options.output, 'w+')
    inFile = open(options.input, 'rU')
    inSeqIter = SeqIO.parse(inFile, "fasta")

    if options.clean :
        outSeqIter = (record for record in inSeqIter if len(record.seq) > options.length and checkAlphabet(str(record.seq), options))
    else:
        outSeqIter = (record for record in inSeqIter if len(record.seq) > options.length)

    # Currently wraps characters in output at length 60, haven't found the wrapping option in BioSeqIO
    # Would like no wrapping on sequences
    SeqIO.write(outSeqIter, outFile, "fasta")
    inFile.close()
    outFile.close()

# Main
def main():

    options = setUp()
    parse(options)

if __name__ == "__main__":
    main()
