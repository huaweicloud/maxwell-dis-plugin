#!/bin/bash -l

while getopts 'f:l:' opt; do
    case $opt in
        f)
            config_value="$OPTARG";;
        l)
            logger_value="$OPTARG";;
        ?)
            echo "Usage: `basename $0` -f config_file -l logger_name"
            exit
    esac
done

pdir=$(cd "`dirname $0`";cd ../;pwd)

CONFIG_PATH=${pdir}/conf/config.properties
if [ -n "${config_value}" ]; then
    CONFIG_PATH=`echo ${config_value} |  cut -d" " -f1 | xargs -n1 realpath`
fi

export MAXWELL_LOGGER_NAME="maxwell-dis"
if [ -n "${logger_value}" ]; then
    export MAXWELL_LOGGER_NAME=${logger_value}
fi

MAIN_CLASS_ARGS="--config ${CONFIG_PATH} --logger ${MAXWELL_LOGGER_NAME}"

cd "${pdir}"

MAIN_CLASS="com.zendesk.maxwell.Maxwell"

process=`ps -ef | grep "${MAIN_CLASS}" | grep "${CONFIG_PATH}" | grep -v grep`
if [ $? -eq 0 ]; then
    pid=`echo ${process} | awk '{print $2}'`
    echo "Maxwell [${pid}] is running, please stop it first."
    exit 1
fi
    
JAVACMD="java"
JAVA_START_HEAP="256m"
JAVA_MAX_HEAP="512m"

JAVA_PATH=`which ${JAVACMD}`
if [ "${JAVA_PATH}" == "" ]; then
    echo "No java found, please install JRE1.8+"
    exit 1
fi

JAVA_VERSION=`java -version 2>&1 |awk 'NR==1{ gsub(/"/,""); print $3 }'`
if [[ "${JAVA_VERSION}" =~ "1.7" || "${JAVA_VERSION}" =~ "1.6" || "${JAVA_VERSION}" =~ "1.5" ]]; then
    echo "Java version ${JAVA_VERSION} is too low, please upgrade to 1.8+"
    exit 1
fi

LIB_DIR="${pdir}/lib/*"
CONFIG_DIR="."
#CLASSPATH="$LIB_DIR":$(find "$LIB_DIR" -type f -name \*.jar | paste -s -d : -):"$CLASSPATH":"$CONFIG_DIR"
OOME_ARGS="-XX:OnOutOfMemoryError=\"/bin/kill -9 %p\""
JVM_ARGS="-Xms${JAVA_START_HEAP} -Xmx${JAVA_MAX_HEAP} -Djava.io.tmpdir=${pdir}/lib -Dfile.encoding=UTF-8 -Dlog4j.shutdownCallbackRegistry=com.djdch.log4j.StaticShutdownCallbackRegistry -Dlog4j.configurationFile=log4j2.xml $JVM_ARGS"

exec $JAVACMD $JVM_ARGS $JVM_DBG_OPTS "$OOME_ARGS" \
  -cp "${LIB_DIR}" \
  $MAIN_CLASS ${MAIN_CLASS_ARGS} > /dev/null 2>&1 &
  
sleep 1
if [ $? -eq 0 ]; then
    process=`ps -ef | grep "${MAIN_CLASS}" | grep -v grep`
    if [ $? -ne 0 ]; then
        echo -e "Failed to start maxwell, cloud not find Maxwell process id, please check logs/maxwell-dis.log for detail information.\n"
        if [ -e "${pdir}/logs/maxwell-dis.log" ]; then
            tail -n +`grep -n "ERROR" "${pdir}/logs/maxwell-dis.log" | tail -1 | awk -F":" '{print $1}'` "${pdir}/logs/maxwell-dis.log" | tail - 100
        fi
        exit 1
    fi
    pid=`echo ${process} | awk -F" " '{print $2}'`
    echo "${pid}" >> "${pdir}/maxwell.pid"
    echo "Success to start Maxwell [${pid}]."
else
    echo "Failed to start Maxwell."
fi
