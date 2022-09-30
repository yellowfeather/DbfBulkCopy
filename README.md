# DbfBulkCopy

[![Build status](https://ci.appveyor.com/api/projects/status/1se8yd6ra2m71nlk?svg=true)](https://ci.appveyor.com/project/chrisrichards/dbfbulkcopy)

Command line application to bulk copy from DBF files to MS SqlServer

Usage:

```
DbfBulkCopy 1.1.0
Copyright Chris Richards

  --server                Required. The database server
  --database              Required. The name of the database
  --dbf                   Required. Path to the DBF file to import
  --table                 Required. The name of the database table to import into
  --UseSSPI               (Default: true) Use Integrated Authentication to connect to the database server 
  --bulkcopytimeout       (Default: 30) The connection timeout used in the bulk copy operation
  --truncate              (Default: false) Whether to truncate the table before copying
  --skipdeletedrecords    (Default: false) Whether to skip deleted records
  --help                  Display this help screen.
  --version               Display version information.
  --userid                The UserID used to connect to the database server
  --password              The password used to connect to the database server
  --createtable           Create destination table. Drop it first if it exists 
```
