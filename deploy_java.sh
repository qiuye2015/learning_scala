#!/bin/bash
#
# update java on remote node
# ./java_deploy.sh $user_name
#
#

CUR_DIR=$(cd `dirname $0`; pwd)

if [ $# -lt 1 ]; then
    echo "FATAL para not enough"
    exit 1
fi
USER_NAME=$1
DEPLOY_DIR=/data/$USER_NAME/deploy/java

#
# deploy java
#
for IP in $(cat $CUR_DIR/host_ip.txt)
do
    # init
    ssh $USER_NAME@$IP "mkdir -p $DEPLOY_DIR"

    # copy java
    scp -r $CUR_DIR/jdk-8u191-linux-x64.tar.gz $USER_NAME@$IP:$DEPLOY_DIR
    
    # decompression
    ssh $USER_NAME@$IP "cd $DEPLOY_DIR; tar -zxvf jdk-8u191-linux-x64.tar.gz"

    # add rwx qualification
    ssh $USER_NAME@$IP "chmod -R 777 $DEPLOY_DIR/jdk1.8.0_191"

    # modify environment variables
    ssh $USER_NAME@$IP "echo '' >> /home/$USER_NAME/.bash_profile"
    ssh $USER_NAME@$IP "echo '# java varibles' >> /home/$USER_NAME/.bash_profile"
    ssh $USER_NAME@$IP "echo 'export JAVA_HOME=$DEPLOY_DIR/jdk1.8.0_191' >> /home/$USER_NAME/.bash_profile"
    ssh $USER_NAME@$IP "echo 'export JRE_HOME=\$JAVA_HOME/jre' >> /home/$USER_NAME/.bash_profile"
    ssh $USER_NAME@$IP "echo 'export CLASSPATH=\$JAVA_HOME/lib:\$CLASSPATH' >> /home/$USER_NAME/.bash_profile"
    ssh $USER_NAME@$IP "echo 'export PATH=\$JAVA_HOME/bin:\$JRE_HOME/bin:\$PATH' >> /home/$USER_NAME/.bash_profile"
    
    # check status
    ssh $USER_NAME@$IP "source /home/$USER_NAME/.bash_profile; java -version"
done








