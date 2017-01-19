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

/** HBaseTable: logic and operations of a Hbase table.
 * 
 * This class contains operations such as create, open and add a row to a table.
 * 
 */
public class HbaseTable {		
	
	// Column names for hashtags and frequencies.
	private static byte[] COL_HASHTAG1 = Bytes.toBytes("HASHTAG1");
	private static byte[] COL_HASHTAG2 = Bytes.toBytes("HASHTAG2");
	private static byte[] COL_HASHTAG3 = Bytes.toBytes("HASHTAG3");
	private static byte[] COL_FREQ1 = Bytes.toBytes("FREQ1");
	private static byte[] COL_FREQ2 = Bytes.toBytes("FREQ2");
	private static byte[] COL_FREQ3 = Bytes.toBytes("FREQ3");
	
	/* Create a table, if it does already exist, it is previously dropped.
	 * 
	 * @param tableName, name of the table.
	 * @param familyName, name of the family of columns
	 */
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
		HColumnDescriptor family = new HColumnDescriptor(CF);
		
		table.addFamily(family);
		admin.createTable(table);
		admin.close();
	}
	
	/* Open a Hbase table.
	 * 
	 * @param tableName, name of the table
	 * @return HTable cursor.
	 * 
	 */
	public static HTable open(String tableName) throws IOException{
		// Open table
		Configuration conf = HBaseConfiguration.create();
		HConnection conn = HConnectionManager.createConnection(conf);		
		return new HTable(TableName.valueOf(Bytes.toBytes(tableName)), conn);
	}
	
	/* Add a row to a table.
	 * 
	 * @param table, cursor to Hbase table.
	 * @param familyName, name of the column's family.
	 * @param key, key of the table.
	 * @param hashtag1, first hashtag
	 * @param freqHashtag1, frequency of the first hashtag
	 * @param hashtag2, second hashtag
	 * @param freqHashtag2, frequency of the second hashtag
	 * @param hashtag3, third hashtag
	 * @param freqHashtag3, frequency of the third hashtag
	 */
	public static void addRow(HTable table, String familyName, byte[] key, String hashtag1, 
			String freqHashtag1, String hashtag2, String freqHashtag2, String hashtag3, String freqHashtag3){
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
