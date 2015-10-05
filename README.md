BiGPy - Biological Similarity Graphs with PySpark.

If no output filename is provided the following will be the output files:

bigpyRemoved.fsa - Lists all sequences, with headers, from the input file were filtered during the processing stage.

bigpyClean.txt - Lists all sequences, without headers, kept after filtering. A single line stores one text sequence.

bigpyMap.fsa - Lists all sequences kept after filtering with a sequence ID. ID is the original header from the input file.

bigpySeqs.seq - Lists all in binary to be used for parallel step. A single line stores one sequence.