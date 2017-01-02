package master2016;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.Table;
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
	public static void main( String[] args ) throws IOException  {

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
		String [] languagesList = languages.split(",");
		dataFolder = args[6];
		outputFolder = args[7];

		String tableName = "TABLE_NAME";
		String familyName = "d";




		if(mode.equals("4")){
			
			// Get all files with .out extension.
			File dir = new File(dataFolder);
			File [] files = dir.listFiles(new FilenameFilter() {
			    @Override
			    public boolean accept(File dir, String name) {
			        return name.endsWith(".out");
			    }
			});

			HbaseTable.createTable(tableName, familyName);
			HTable table = HbaseTable.open(tableName);	

			for(int i=0; i<files.length; i++){

				File filePath = files[i];
				BufferedReader reader = new BufferedReader(new FileReader(filePath));

				// TODO: format of each sentence? (in the example there are spaces).
				String line;
				while((line = reader.readLine()) != null){

					String[] lines = line.split(",");

					Long timeStamp = new Long(lines[0]);
					String lang = lines[1];
					String hashtag1 = lines[2];
					String freqHashtag1 = lines[3];
					String hashtag2 = lines[4];
					String freqHashtag2 = lines[5];
					String hashtag3 = lines[6];
					String freqHashtag3 = lines[7];


					/* How to select the key?
					 * 
					 * 1) Rows in HBase are sorted lexicographically by row key. This design optimizes for scans, 
					 * allowing you to store related rows, or rows that will be read together, near each other    .
					 *                     
					 * 2) If you are not using a filter against rowkey column in your query, your rowkey design may be wrong. 
					 * The row key should be designed to contain the information you need to find specific subsets of data.
					 */


					// Generate row key.
					byte[] key = KeyGenerator.generateKey(timeStamp, lang);				
					HbaseTable.addRow(table, familyName, key, hashtag1, freqHashtag1, hashtag2, freqHashtag2, hashtag3, freqHashtag3);

					// DEBUG
					Get get = new Get(key);
					Result rs = table.get(get);
					byte[] valueResult=rs.getValue(Bytes.toBytes(familyName), Bytes.toBytes("HASHTAG1"));
					System.out.println("Result = " + Bytes.toString(valueResult));

				}			

				reader.close();

			}
			table.close();

		}

		else if(mode.equals("1") || mode.equals("2") || mode.equals("3")){
			byte[] startKey = KeyGenerator.generateStartKey(new Long(startTS));
			byte[] endKey = KeyGenerator.generateEndKey(new Long(endTS));

			Scan scan = new Scan(startKey, endKey);
			HTable table = HbaseTable.open(tableName);

			if(mode.equals("3")){				
				ResultScanner rs = table.getScanner(scan);
				debug(rs);
			}
			else{
				for(int i=0; i<languagesList.length; i++){
					String lang = languagesList[i];

					// Filter by language.
					RegexStringComparator endsWithLang = new RegexStringComparator(lang+"$");
					RowFilter langFilter = new RowFilter(CompareOp.EQUAL, endsWithLang);
					scan.setFilter(langFilter);

					ResultScanner rs = table.getScanner(scan);
					debug(rs);
					
				}
			}
			table.close();

		}
	}
	
	// Method for debugging
	private static void debug(ResultScanner rs) throws IOException{
		Result res = rs.next();
		while (res!=null && !res.isEmpty()){
			
			byte[] valueResult = res.getValue(Bytes.toBytes("d"), Bytes.toBytes("HASHTAG1"));
			System.out.println(Bytes.toLong(res.getRow())+Bytes.toString(valueResult));
			res = rs.next();
		} 
		
	}
}
