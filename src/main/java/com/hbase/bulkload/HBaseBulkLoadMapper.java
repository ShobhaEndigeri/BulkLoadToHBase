package com.hbase.bulkload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HBaseBulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
	private String hbaseTable;	
	private String dataSeperator;
	private String columnFamily;
	private ImmutableBytesWritable hbaseTableName;

	public void setup(Context context) {
		Configuration configuration = context.getConfiguration();		
		hbaseTable = configuration.get("hbase.table.name");		
		dataSeperator = configuration.get("data.seperator");		
		columnFamily = configuration.get("COLUMN_FAMILY_1");			
		hbaseTableName = new ImmutableBytesWritable(Bytes.toBytes(hbaseTable));	
	}
	
	public void map(LongWritable key, Text value, Context context) {
		try {		
			String valStr = value.toString();
			String rowKey = valStr.substring(0,valStr.indexOf(dataSeperator));
			String rowVal = valStr.substring(valStr.indexOf(dataSeperator)+1,valStr.length());
			
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(columnFamily), Bytes.toBytes("val"), Bytes.toBytes(rowVal));			
			context.write(hbaseTableName, put);	
		} catch(Exception exception) {			
			exception.printStackTrace();			
		}
	}
}
