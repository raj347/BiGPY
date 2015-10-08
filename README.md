BiGPy - Biological Similarity Graphs with PySpark.

bigpy-prepare is used to prepare input data to be used later for bigpy-sketch. It takes an input file or directory and processes the fasta files within to create several output files.

.brm - Lists all sequences headers from the input file(s) that were filtered during the processing stage.

.btxt - Lists all sequences, without headers, kept after filtering. A single line stores one text sequence and its ID.

.bmap - Lists all sequence headers kept after filtering with a sequence ID. ID is the line in the .bseq and .btxt files in which the sequence is listed.

.bseq - Lists all kept sequences in binary with ID to be used for parallel step.
