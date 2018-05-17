# BulkLoadToHBase

Map Reduce program to bulk load data from text/csv file to HBase .
Input is TextFileInputFormat
Output is HFileOutputFormat

After the job completion Hfiles will be loaded to HBase

Note:Number of reducers are decided by number of regions for the output table.
     
