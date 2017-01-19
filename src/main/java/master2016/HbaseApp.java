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

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;


/** TwitterApp: storing and querying trending topics from Twitter 
 * 
 * 
 * Java application that stores trending topics from Twitter into HBase 
 * and provides users with a set of queries for data analysis.	
 * 
 * @author: Antonio Javier González Ferrer
 * @author: Aitor Palacios Cuesta
 *  
 */
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

		// The number of parameters must be either 7, 6 or 3.
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

		// Load table into HBase
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

			// Create table, if it does already exist, it will be dropped.
			HbaseTable.createTable(tableName, familyName);
			HTable table = HbaseTable.open(tableName);	

			
			for(int i=0; i<outFiles.length; i++){//For each of the .out files.
				BufferedReader reader = new BufferedReader(new FileReader(outFiles[i]));

				String line;
				while((line = reader.readLine()) != null){

					// The format is: "timestamp,lang,hashtag1,freq1,hash2tag,freq2,hash3tag,freq3
					String[] lines = line.split(",");

					Long timeStamp = new Long(lines[0]);
					String lang = lines[1];
					String hashtag1 = lines[2];
					String freqHashtag1 = lines[3];
					String hashtag2 = lines[4];
					String freqHashtag2 = lines[5];
					String hashtag3 = lines[6];
					String freqHashtag3 = lines[7];


					/* Generate row key.
					 * We have chosen timestamp and lang to optimize the desigh for scans, 
					 * allowing to store related rows, or rows that will be read together, near each other.
					 */
					byte[] key = KeyGenerator.generateKey(timeStamp, lang);				
					HbaseTable.addRow(table, familyName, key, hashtag1, 
							freqHashtag1, hashtag2, freqHashtag2, hashtag3, freqHashtag3);
				}
				reader.close();
			}
			table.close();
		}
		// Query modes.
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

			// Defining the start and end of the keys (window between timestamps).
			byte[] startKey = KeyGenerator.generateStartKey(new Long(startTS));
			byte[] endKey = KeyGenerator.generateEndKey(new Long(endTS));

			Scan scan = new Scan(startKey, endKey);
			HTable table = HbaseTable.open(tableName);

			if(mode.equals("3")){// Do not consider the languages, just scan between timestamps.
				ResultScanner rs = table.getScanner(scan);
				List<Hashtag> sortedRank = rankResults(rs);
				printQueryResults(sortedRank, "3");
			}
			
			else{// We have to filter by language.
				for(int i=0; i<languagesList.length; i++){
					String lang = languagesList[i];

					// Filter the language.by row key, the lang field is at the end of the key.
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

	/*
	 * Compute the ranking of the hashtags.
	 * 
	 * @param rs, result scanner from HBase scan.
	 * @return first N hashtags
	 */
	private static List<Hashtag> rankResults(ResultScanner rs) throws IOException{

		Map<String, Hashtag> rank = new HashMap<String, Hashtag>();

		Result res = rs.next();
		while (res!=null && !res.isEmpty()){
			
			String row=Bytes.toString(res.getRow());
			// The lang is located in the last two characters of the key.
			String lang = row.substring(row.length()-2);
			
			for(int i=1; i<=3; i++){// We have 3 hashtags per tweet
				byte[] hashtag = res.getValue(Bytes.toBytes("d"), Bytes.toBytes("HASHTAG"+i));
				byte[] value = res.getValue(Bytes.toBytes("d"), Bytes.toBytes("FREQ"+i));
				String hashtagString = Bytes.toString(hashtag);
				int valueInt = Integer.parseInt(Bytes.toString(value));

				// We store the hashtag in a map in order to compute the total rank.
				// We need to define both hashtag and lang as key, since it is not specified if the same word
				// can appear in different languages (e.g. "Titanic" might be in both "es" and "en").
				if(!rank.containsKey(hashtagString+":"+lang)){
					rank.put(hashtagString+":"+lang, new Hashtag(hashtagString, lang, valueInt));
				}
				else{
					rank.get(hashtagString+":"+lang).incrementCount(valueInt);
				}
			}
			res = rs.next();
		}

		// We sort the final rank, first by value then alphabetically.
		List<Hashtag> sortedRank = new ArrayList<Hashtag>(rank.values());
		Collections.sort(sortedRank, Collections.reverseOrder());

		// Return just the first N elements.
		return sortedRank.subList(0, N);
		
	}
	
	/*
	 * Append into the file the query result.
	 * 
	 * @param sortedRank, sorted rank with the first N elements.
	 * @param n, query id.
	 */
	private static void printQueryResults(List<Hashtag> sortedRank, String n) throws FileNotFoundException{
		// cont to store the rank position. 
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