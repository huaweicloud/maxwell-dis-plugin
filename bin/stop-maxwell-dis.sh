#!/bin/bash -l

pdir=$(cd `dirname $0`;cd ../;pwd)
cd "${pdir}"

MAIN_CLASS="com.zendesk.maxwell.Maxwell"

stop_pid=""
process_num=`ps -ef | grep "${MAIN_CLASS}" | grep -v grep | wc -l`
if [ ${process_num} -eq 0 ]; then
    /bin/echo "Cloud not find Maxwell process."
    exit 1
elif [ ${process_num} -gt 1 ]; then
    /bin/echo "Find multi Maxwell process."
    num=0
    /bin/echo -e "Num\tPID \tProcess"
    for i in `ps -ef | grep "${MAIN_CLASS}" | grep -v grep | awk '{print $2}'`
    do
        pid_arr[$num]=${i}
        /bin/echo -e "${num}\t${i}\t`ps -eo pid,cmd| grep ${i} | grep -v grep | awk '{print $(NF-4), $(NF-3), $(NF-2), $(NF-1), $NF}'`"
        num=`expr ${num} + 1`;
    done

    read -p "Please enter the Num to stop the corresponding process: " CONFIRM_NUM

    if [ -z "${CONFIRM_NUM}" ]; then
        /bin/echo "Exit without selecting Num."
        exit
    fi
    pid=${pid_arr[$CONFIRM_NUM]}
else
    pid=`ps -ef | grep "${MAIN_CLASS}" | grep -v grep | awk '{print $2}'`
fi

if [ -z "${pid}" ]; then
    /bin/echo "Failed to stop process: cloud not get the pid."
    exit
fi

kill ${pid}

/bin/echo -n "Stopping Maxwell [${pid}]."
calcSeconds=`date +"%s"`
while true;
do
    sleep 0.5
    process=`ps -eo pid,cmd| grep ${pid} | grep -v grep`
    if [ $? -eq 0 ]; then
        elapsed=$(expr `date +"%s"` - ${calcSeconds})
        if [ ${elapsed} -gt 20 ]; then
            /bin/echo -n " force stop"
            kill -9 ${pid}
        else
            /bin/echo -n "."
        fi
    else
        if [ -e "${pdir}/maxwell.pid" ]; then
            sed -i "/${pid}/d" "${pdir}/maxwell.pid"
        fi

        if [ ${process_num} -eq 1 ]; then
            find "${pdir}/lib/" -name "sqlite-*sqlitejdbc*" | xargs -n1 rm -f
        fi
        /bin/echo " Successfully."
        exit 0
    fi
done