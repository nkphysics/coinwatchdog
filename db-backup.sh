#!/bin/bash


DATETIME=$(date "+%FT%T")
DEST="/PATH/TO/LOCATION/OF/DATABASE"
BACKUPS="/PATH/TO/BACKUP/LOCATION/"

# Backup and the database
cp $DEST/coinwatchdog.db $DEST/coinwatchdog_"$DATETIME".db
gzip $DEST/coinwatchdog_"$DATETIME".db
mv $DEST/coinwatchdog_"$DATETIME".db.gz $BACKUPS

# Prune old backups older than 7 days
find "$BACKUPS" -name "coinwatchdog*.gz" -mtime +7 -exec rm {} \; -print
