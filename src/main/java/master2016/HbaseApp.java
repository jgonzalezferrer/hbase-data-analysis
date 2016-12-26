package master2016;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseApp {

	/*
	 * Mode of running:
	 * 1) run first query
	 * 2) run second query
	 * 3) run third query
	 * 4) create the table twitterStats and load data files.
	 */ 
	private static String mode;

	/*
	 * String with the format IP:PORT	
	 */
	private static String zkHost;

	/*
	 * Timestamp in milliseconds to be used as start timestamp.
	 */
	private static String startTS;

	/*
	 * Timestamp in milliseconds to be used as end timestamp.
	 */
	private static String endTS;

	/*
	 * Size of the ranking for the top-N
	 */
	private static String N; 

	/*
	 * A csv list of languages.
	 */
	private static String languages;

	/*
	 * Path to the folder containing the files with the trending topics-	
	 */
	private static String dataFolder;

	/*
	 * Path to the folder where the files with the query results are stored.
	 */
	private static String outputFolder; 
	public static void main( String[] args )  {

		// The number of arguments must be exactly 8.
		if (args.length != 8){
			System.err.println("Error in number of parameters");
			return;
		}
		
		mode = args[0];
		zkHost = args[1];
		startTS = args[2];
		endTS = args[3];
		N = args[4];
		languages = args[5];
		dataFolder = args[6];
		outputFolder = args[7];
		
		if(mode.equals("4")){
			// Create table and column
			Configuration conf = HBaseConfiguration.create();
			try {
				HBaseAdmin admin = new HBaseAdmin(conf);
				
				byte[] TABLE = Bytes.toBytes("TABLE_NAME");
				byte[] CF = Bytes.toBytes("DATA_NAME");
				
				// TODO: what happens if the table does already exist?
				HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE));
				
				HColumnDescriptor family = new HColumnDescriptor(CF);
				// TODO: correct number of max versions allowed.
				// Definition: limit the number of version of each column
				family.setMaxVersions(10);  // Default is 3. 
				
				table.addFamily(family);
				admin.createTable(table);
				
				admin.close();
				
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}
}
