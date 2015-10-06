BiGPy - Biological Similarity Graphs with PySpark.

If no output filename is provided the following will be the output files:

bigpyRemoved.fsa - Lists all sequences, with headers, from the input file were filtered during the processing stage.

bigpyClean.txt - Lists all sequences, without headers, kept after filtering. A single line stores one text sequence and its ID.

bigpyMap.fsa - Lists all sequences kept after filtering with a sequence ID. ID is the line in the bigpySeqs.seq and bigpyClean.txt files in which the sequence is listed.

bigpySeqs.seq - Lists all in binary with ID to be used for parallel step. A single line stores one sequence.
