package master2016;

import java.io.IOException;
import java.io.InterruptedIOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseTable {		
	
	// Column names
	private static byte[] COL_HASHTAG1 = Bytes.toBytes("HASHTAG1");
	private static byte[] COL_HASHTAG2 = Bytes.toBytes("HASHTAG2");
	private static byte[] COL_HASHTAG3 = Bytes.toBytes("HASHTAG3");

	private static byte[] COL_FREQ1 = Bytes.toBytes("FREQ1");
	private static byte[] COL_FREQ2 = Bytes.toBytes("FREQ2");
	private static byte[] COL_FREQ3 = Bytes.toBytes("FREQ3");
	

	public static void createTable(String tableName, String familyName) throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		
		byte[] TABLE = Bytes.toBytes(tableName);
		byte[] CF = Bytes.toBytes(familyName);
		
		// Create table and column
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);
		
		// Check if the table does already exists. If true, eliminate it.
		if(admin.tableExists(TableName.valueOf(TABLE))) {
			admin.disableTable(TableName.valueOf(TABLE));
			admin.deleteTable(TableName.valueOf(TABLE));
		}

		HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE));
		
		/*
		 * Physically, all column family members are stored together on the filesystem. 
		 * 
		 * Try to make do with one column family if you can in your schemas. 
		 * Only introduce a second and third column family in the case where data access is usually column scoped; 
		 * i.e. you query one column family or the other but usually not both at the one time.
		 * 
		 * 
		 * 6.3.2.2. Attributes
		 * Although verbose attribute names (e.g., "myVeryImportantAttribute") are easier to read, prefer shorter attribute names (e.g., "via") to store in HBase.
		 */

		// * 6.3.2.1. Column Families
		// * Try to keep the ColumnFamily names as small as possible, preferably one character (e.g. "d" for data/default).		

		HColumnDescriptor family = new HColumnDescriptor(CF);
		// TODO: correct number of max versions allowed.
		// Definition: limit the number of version of each column
		family.setMaxVersions(10);  // Default is 3. 
		table.addFamily(family);			

		admin.createTable(table);
		admin.close();
	}
	
	public static HTable open(String tableName) throws IOException{
		// Open table
		Configuration conf = HBaseConfiguration.create();
		HConnection conn = HConnectionManager.createConnection(conf);		
		return new HTable(TableName.valueOf(Bytes.toBytes(tableName)), conn);
	}
	
	
	public static void addRow(HTable table, String familyName, byte[] key, String hashtag1, String freqHashtag1, String hashtag2, String freqHashtag2, String hashtag3, String freqHashtag3){
		byte[] CF = Bytes.toBytes(familyName);
		
		Put put = new Put(key);
		
		put.add(CF, COL_HASHTAG1, Bytes.toBytes(hashtag1));
		put.add(CF, COL_FREQ1, Bytes.toBytes(freqHashtag1));
		
		// TODO: second and third hashtags can be null?
		put.add(CF, COL_HASHTAG2, Bytes.toBytes(hashtag2));
		put.add(CF, COL_FREQ2, Bytes.toBytes(freqHashtag2));
		
		put.add(CF, COL_HASHTAG3, Bytes.toBytes(hashtag3));
		put.add(CF, COL_FREQ3, Bytes.toBytes(freqHashtag3));
		
		try {
			table.put(put);
		} catch (RetriesExhaustedWithDetailsException | InterruptedIOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}


}
