#!/bin/sh


# rds_skipper.sh: RDS MySQL replication skipper.
# Since pt-slave-restart couldn't skip replication errors I had to write this script.
# The script accepts error number and error text for skipping. 
# It can skip multiple error numbers and an error text.

# Version: 1.0
# By: Kedar Vaijanapurkar
# Website: https://kedar.nitty-witty.com/blog


# ------
# To Do
# ------
# Error handling
# Improve logging
# Option to specify error number OR error text 
# Multiple error texts
# Improve iteration (RDS skip itself takes longer
# Provide Stats


ENDPT="rds-endpoint.kedar.nitty-witty.com" # your RDS endpoint
MYSQL='mysql -h${ENDPT}'
ERRNO_TO_SKIP="1062"  # Add the comma separated error numbers to skip here
MATCH_TEXT="Default database: 'mysql'" # Match error text
ITERATION=1 # Number of seconds to sleep between the iteration
KDEBUG=0 # use KDEBUG=1 skip_rds_repl.sh to print debug messages

while true;
do
  SBM=`$MYSQL -e"show slave status\G" | grep -i seconds_behind_master | awk '{print $2}'`
  LASTERRNUM=`$MYSQL -e"show slave status\G" | grep -i Last_Errno | awk '{print $2}'`
  SKIP_ERR_TXT==`$MYSQL -e"show slave status\G" | grep -i Last_SQL_Error`
  echo ""
  echo "seconds_behind_master : ${SBM}"
  [ "$KDEBUG" == 1 ] && echo $SKIP_ERR_TXT
  [ "$KDEBUG" == 1 ] && echo $MATCH_TEXT
  if [[ ${SBM} == 'NULL' ]]
  then
    if [[ ",$ERRNO_TO_SKIP," == *",$LASTERRNUM,"* ]] && [[ $SKIP_ERR_TXT == *"$MATCH_TEXT"* ]]
    then
      echo "Replication is down, and error number and error text matches. Skipping error ${LASTERRNUM}"
      $MYSQL -e"CALL mysql.rds_skip_repl_error;"
    else
      echo "Replication is down, but error number ${LASTERRNUM} and ${MATCH_TEXT} does not match the list. Exiting script."
      exit 1;
    fi
  else
    echo "Replication is up"
  fi
  sleep $ITERATION;
done
