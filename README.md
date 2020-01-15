# MSSQL-Diff-Table-values

# SQL compare data from two tables

Diff values vrom table.
For the moment diffs are done on same sql instance, and on different databases.
And diff tables should have same name.

The results are returned just for columns where was found any diff. 
Comparing with other techique like example EXCEPT/INTERSECT what are returning all columns, even if there are no diffs

@ExcludeColumns - there will be delimited columns what should be excluded from Diff
@JoinFKTables - by default is enabled, and will do a diff and on "child table" level 
            - let say if table contain just references (as an example), then id reference are used for joining to actual data, 
              and IDs are excluded from Diff
              
              
@UseCollation - enable if objesct are on diff collations

@WhereClause - filter to limit exclude data from comarations. Filter should start with AND (need to dbl check :) )
             - if @JoinFKTables is enabled, then add alias "mt." on columns
             
             
ToDo:

1. add ability to compare table A and table B where name are not the same
2. add ability to compare data from another sql instance (if is linked)
