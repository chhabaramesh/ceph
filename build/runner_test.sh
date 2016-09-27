#DEVICE=/dev/zs6

#/dev/md127 , /dev/sdj
DEV=$1 
# zipf, zipf1.1, uniform_with_rw, uniform_xcopy, uniform_xcopy_with_reads tag
RUN_TYPE=$2
# Storage size in gbs, 800gb, 200 gb etc
STORAGE_SIZE=$3
# mount of storage available to app in gbs 560gb, 130gb etc
APP_STORAGE_SIZE=$4
# Run time in secs
RUN_TIME=$5
TAG=$6

NUM_THREADS=32

if [ $# -lt 5 ] ; then
	echo "Usage ./runner_test.sh /dev/md127 zipf1.1 800 200 3600"
	exit;
fi


export WS_XCOPY_EMU=0

if [ $RUN_TYPE == "zipf1.1" ] ; then
	echo "Zipf 1.1 run"
	tag=${TAG}"_zipf1.1"
	cmd="./runner_fio.sh $1 0 256 $tag 0 $RUN_TIME zipf:1.1 $STORAGE_SIZE" 
elif [ $RUN_TYPE == "uniform_with_rw" ]; then
	echo "uniform with rw run"
	tag=${TAG}"_uni_rw"
	export WS_GC_THREADS=96
	cmd="./runner_fio.sh $1 0 256 $tag 0 $RUN_TIME random $STORAGE_SIZE"

elif [ $RUN_TYPE == "uniform_with_reads" ]; then

	echo "Not tested yet"
	exit

elif [ $RUN_TYPE == "uniform_xcopy" ]; then
	export WS_XCOPY_EMU=1
	export WS_GC_THREADS=4
	tag=${TAG}"_uni_xcp"
#	NUM_THREADS=32
	cmd="./runner_fio.sh $1 0 $NUM_THREADS $tag 0 $RUN_TIME random $STORAGE_SIZE"
fi

export WS_STORAGE_SIZE=$APP_STORAGE_SIZE;

echo "Running fio command $cmd"

$cmd

exit; 

./runner_fio.sh 
