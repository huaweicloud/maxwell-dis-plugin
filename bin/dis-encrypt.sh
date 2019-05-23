#!/bin/bash -l

read -s -t 20 -p "Please enter password:" PASS

if [ $? -ne 0 ]; then
    echo ""
    echo "Failed to read password, exit."
    exit 1
fi

echo ""

if [ -z "${PASS}" ]; then
    echo "Get empty password."
    exit 1
fi

pdir=$(cd `dirname $0`;cd ../;pwd)
cd ${pdir}

result=`java -cp "lib/*" com.zendesk.maxwell.util.EncryptTool "${PASS}"`

if [ $? -ne 0 ]; then
    echo "Failed to encrypt."
    exit 1
fi

echo "Encrypt result: ${result}"