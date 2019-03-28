#!/bin/bash -l

pdir=$(cd `dirname $0`;cd ../;pwd)
cd "${pdir}"

MAIN_CLASS="com.zendesk.maxwell.Maxwell"

process=`ps -ef | grep "${MAIN_CLASS}" | grep -v grep`
if [ $? -ne 0 ]; then
    echo "Cloud not find Maxwell process."
    exit 1
fi

pid=`echo ${process} | awk -F" " '{print $2}'`
kill ${pid}

/bin/echo -n "Stopping Maxwell [${pid}]."
calcSeconds=`date +"%s"`
while true;
do
    sleep 0.5
    process=`ps -ef | grep "${MAIN_CLASS}" | grep -v grep`
    if [ $? -eq 0 ]; then
        elapsed=$(expr `date +"%s"` - ${calcSeconds})
        if [ ${elapsed} -gt 15 ]; then
            /bin/echo -n " force stop"
            kill -9 ${pid}
        else
            /bin/echo -n "."
        fi
    else
        rm -f "${pdir}/maxwell.pid"
        echo " Successfully."
        exit 0
    fi
done