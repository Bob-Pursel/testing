#!/bin/bash

#This is a script to load files into Vertica and then Export to Parquet format
#The following arguments are required:
#                logNum == The log_num value of the file to process
                 fileNum == The file_config.file_num of the file to process
#                
#Maintenance Log
#
#   Date           Who               Changes
#  ----------    --------------     ------------------------------------
#  10/2017       Pursel             Initial
#
#
#

logNum = $1
fileNum = $2

#get file details from Vertica file_config table

vals=($(vsql -Ddwp1 -udbadmin -pdbadmin -se "(SELECT file_name, format, table_name, parquet_table_name, requires_quote_subs, path_name from fileConfig where file_num = :fileNum"))
fileName = ${vals[0]};
format = ${vals[1]};
tableName = ${vals[2]};
parquetTableName = ${vals[3]};
requiresQuoteSubs = ${vals[4]};
pathName = ${vals[5]};


