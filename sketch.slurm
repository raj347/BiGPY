#!/bin/bash

### Make sure that nodes are exclusive for you
### ntasks-per-node is irrelevant
#SBATCH --partition=debug
#SBATCH --exclusive
#SBATCH --ntasks-per-node=4

### Customize this section for your job
#SBATCH --mem=64000
#SBATCH --nodes=1

#SBATCH --job-name="ILikePie"
#SBATCH --output=%j.stdout
#SBATCH --error=%j.stderr

# MAKE SURE THAT SPARK_DIR_LOG AND SPARK_WORKER_DIR
# ARE SET IN YOUR BASHRC

# Add extra modules here

# Set your command and arguments
PROG="/user/dhanasek/BiGPY/src/bigpy_sketch.py"
ARGS="-i /user/dhanasek/BiGPY/src/test.btxt -o testSketch -k 16"

####### DO NOT EDIT THIS PART
module load python/anaconda
module load boost/1.57.0
module load java/1.8.0_45
module load hadoop/2.6.0
module load spark/1.4.1-hadoop

# GET LIST OF NODES
NODES=(`srun hostname | sort | uniq`)

NUM_NODES=${#NODES[@]}
LAST=$((NUM_NODES - 1))

# FIRST NODE IS MASTER
ssh ${NODES[0]} "cd $SPARK_HOME; ./sbin/start-master.sh"
MASTER="spark://${NODES[0]}:7077"

# ALL OTHER NODES ARE WORKERS
mkdir -p $SLURM_SUBMIT_DIR/$SLURM_JOB_ID
for i in `seq 1 $LAST`; do
  ssh ${NODES[$i]} "cd $SPARK_HOME; nohup ./bin/spark-class org.apache.spark.deploy.worker.Worker $MASTER &> $SLURM_SUBMIT_DIR/$SLURM_JOB_ID/nohup-${NODES[$i]}.out" &
done


# SUBMIT PYSPARK JOB
$SPARK_HOME/bin/spark-submit --master $MASTER $PROG $ARGS


# CLEAN SPARK JOB
ssh ${NODES[0]} "cd $SPARK_HOME; ./sbin/stop-master.sh"

for i in `seq 1 $LAST`; do
  ssh ${NODES[$i]} "killall java"
done
