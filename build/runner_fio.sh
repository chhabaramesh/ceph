killall top iostat

#DEVICE=/dev/zs6

DEVICE=$1
RUN_TYPE=$2

FIO_JOBS=$3
TAG=$4
READ_RATIO=$5
RUN_TIME=$6
DISTRIBUTION=$7
STORAGE_SIZE=${8}g

DEPTH=1
#STORAGE_SIZE=125g
#FIO_TIME=1800

rw="write"
if [ $RUN_TYPE -eq 100 ] ; then
    rw="randread"
elif [ $RUN_TYPE -eq 0 ] ; then
    rw="randwrite"
elif [ $RUN_TYPE -eq 1 ] ; then
    rw="write"
elif [ $RUN_TYPE -eq 2 ] ; then
    rw="read"
elif [ $RUN_TYPE -eq 3 ] ; then
    rw="randrw"
fi

dst1=`date +G%d%H%M%S`
DEV=`echo $1 | cut -d/  -f3`
dst="$DEV"_"$rw"_"thds$FIO_JOBS"_"time$RUN_TIME"_"$dst1"_"$TAG"
mkdir $dst
echo "Device is" $DEVICE

FIO_OUTPUT=$dst/out.fio.txt

killall top
killall iostat

top -b | grep -C 5 fio  >> $dst/out.top.txt&
iostat -kyxt 3 >> $dst/out.iostat.txt&

# start fio
#cmd="./fio --filename=$DEVICE --thread  --output=$FIO_OUTPUT --direct=1 --rw=$rw -refill_buffers --randrepeat=0 --random_distribution=$DISTRIBUTION --ioengine=psync --bs=4k --rwmixread=$READ_RATIO -iodepth=$DEPTH --group_reporting --name=4ktest --thread --numjobs=$FIO_JOBS --runtime=$RUN_TIME --filesize=$STORAGE_SIZE --status-interval=10 --time_based"

cmd="fio --filename=$DEVICE --thread  --output=$FIO_OUTPUT --direct=1 --rw=$rw -refill_buffers --norandommap  --randrepeat=0 --random_distribution=$DISTRIBUTION --ioengine=psync --bs=4k --rwmixread=$READ_RATIO -iodepth=$DEPTH --group_reporting --name=4ktest --thread --numjobs=$FIO_JOBS --runtime=$RUN_TIME --status-interval=10 --time_based"
echo $cmd
#$cmd | tee $dst/stats.txt
fio run.fio


killall top iostat
