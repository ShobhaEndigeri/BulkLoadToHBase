package com.hbase.bulkload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

public class HBaseBulkLoad {	
	/**
	 * doBulkLoad.
	 *
	 * @param pathToHFile path to hfile
	 * @param tableName 
	 */
	public static final String MAX_FILES_PER_REGION_PER_FAMILY = "hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily";

	public static void doBulkLoad(String pathToHFile, String tableName) {

		try {		
			Configuration configuration = null;
			Connection connection = null;
			configuration = new Configuration();			
			configuration = HBaseConfiguration.create();
			connection = ConnectionFactory.createConnection(configuration);
			configuration.set("mapreduce.child.java.opts", "-Xmx1g");	
			configuration.setInt(MAX_FILES_PER_REGION_PER_FAMILY, 1024);
			
			HBaseConfiguration.addHbaseResources(configuration);	
			TableName tn = TableName.valueOf(tableName);
			RegionLocator locator = connection.getRegionLocator(tn);
			LoadIncrementalHFiles loadFfiles = new LoadIncrementalHFiles(configuration);	
			HTable hTable = new HTable(configuration, tableName);	
			loadFfiles.doBulkLoad(new Path(pathToHFile), connection.getAdmin(), hTable, locator);	

			System.out.println("Bulk Load Completed..");		
		} catch(Exception exception) {			
			exception.printStackTrace();			
		}		
	}
}
