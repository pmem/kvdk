#!/bin/bash
max=1
min=10000000000

for m in 1 4 8 16 32 64 96
do
i=$(grep -w "write ops 0" string_vs256_read_thread"$m" | awk '{print $3}')
h=${i%?}
        if [ $max -lt $h ];then
                max=$h
        fi

        if [ $min -gt $h ];then
                min=$h
        fi
done
if [  $max -gt 20 -a $max -lt 40 ];then
		echo "sucessful"
else 	echo "fail"
fi
