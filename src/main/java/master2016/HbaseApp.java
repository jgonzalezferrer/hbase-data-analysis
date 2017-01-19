package master2016;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
	private static int N; 

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

		// If we do not use at least 3 parameters.
		if (args.length != 7 && args.length != 6 && args.length != 3){
			System.err.println("Error in number of parameters."
					+ " You need to use 7 parameters for mode 1 and 2"
					+ ", or, you need to use 6 paramaters for mode 3"
					+ ", or, you need to use 3 parameters for mode 4");
			return;
		}

		mode = args[0];
		zkHost = args[1];

		String tableName = "twitterStats";
		String familyName = "d";

		if(mode.equals("4")){

			dataFolder = args[2];

			// Get all files with .out extension.
			File dir = new File(dataFolder);
			File [] outFiles = dir.listFiles(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					return name.endsWith(".out");
				}
			});

			HbaseTable.createTable(tableName, familyName);
			HTable table = HbaseTable.open(tableName);	

			for(int i=0; i<outFiles.length; i++){
				BufferedReader reader = new BufferedReader(new FileReader(outFiles[i]));

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

				}			

				reader.close();

			}
			table.close();

		}
		else if(mode.equals("1") || mode.equals("2") || mode.equals("3")){

			startTS = args[2];
			endTS = args[3];
			N = Integer.parseInt(args[4]);

			// Used in modes 1 and 2.
			String [] languagesList = null;

			if(mode.equals("3")){// We do not have languages in the argument.
				outputFolder = args[5];
			}
			else{
				languages = args[5];
				languagesList = languages.split(",");
				outputFolder = args[6];
			}

			byte[] startKey = KeyGenerator.generateStartKey(new Long(startTS));
			byte[] endKey = KeyGenerator.generateEndKey(new Long(endTS));

			Scan scan = new Scan(startKey, endKey);
			HTable table = HbaseTable.open(tableName);

			if(mode.equals("3")){				
				ResultScanner rs = table.getScanner(scan);
				List<Hashtag> sortedRank = rankResults(rs);
				printQueryResults(sortedRank, "3");
				
			}
			
			else{
				for(int i=0; i<languagesList.length; i++){
					String lang = languagesList[i];

					// Filter by language.
					RegexStringComparator endsWithLang = new RegexStringComparator(lang+"$");
					RowFilter langFilter = new RowFilter(CompareOp.EQUAL, endsWithLang);
					scan.setFilter(langFilter);

					ResultScanner rs = table.getScanner(scan);
					List<Hashtag> sortedRank = rankResults(rs);
					
					String n = mode.equals("1")?"1":"2";
					printQueryResults(sortedRank, n);
				}
			}
			table.close();

		}
	}

	private static List<Hashtag> rankResults(ResultScanner rs) throws IOException{


		Map<String, Hashtag> rank = new HashMap<String, Hashtag>();

		Result res = rs.next();
		while (res!=null && !res.isEmpty()){
			
			String row=Bytes.toString(res.getRow());
			String lang = row.substring(row.length()-2);
			
			for(int i=1; i<=3; i++){
				byte[] hashtag = res.getValue(Bytes.toBytes("d"), Bytes.toBytes("HASHTAG"+i));
				byte[] value = res.getValue(Bytes.toBytes("d"), Bytes.toBytes("FREQ"+i));
				String hashtagString = Bytes.toString(hashtag);
				int valueInt = Integer.parseInt(Bytes.toString(value));

				if(!rank.containsKey(hashtagString+":"+lang)){

					rank.put(hashtagString+":"+lang, new Hashtag(hashtagString, lang, valueInt));
				}
				else{
					rank.get(hashtagString+":"+lang).incrementCount(valueInt);
				}


			}
			res = rs.next();

		}

		List<Hashtag> sortedRank = new ArrayList<Hashtag>(rank.values());
		Collections.sort(sortedRank, Collections.reverseOrder());

		return sortedRank.subList(0, N);
		
	}
	
	private static void printQueryResults(List<Hashtag> sortedRank, String n) throws FileNotFoundException{
		int i = 1;
		FileOutputStream file = new FileOutputStream(new File(outputFolder+"/"+"01_query"+n+".out"), true);
		PrintWriter writer = new PrintWriter(file);
		for(Hashtag hash: sortedRank){			
			String msg = hash.getLang()+","+i+","+hash.getText()+","+startTS+","+endTS+"\n";			
			writer.append(msg);
			writer.flush();
			i++;
		}
		writer.close();
	}
	
}
