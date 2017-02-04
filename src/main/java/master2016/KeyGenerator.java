package master2016;

import org.apache.hadoop.hbase.util.Bytes;

/** KeyGenerator: class for generating Hbase keys.
 * 
 */
public class KeyGenerator {

	/* Generate a row key based on first timestamp and second language.
	 * 
	 * @param timeStamp, timestamp of the tweet
	 * @param lang, language of the tweet
	 * @return key in bytes.
	 * 
	 */
	public static byte[] generateKey(Long timeStamp, String lang){
		/* TODO: discuss the number of bytes.
		 * Structure of the key:
		 *  X bytes for timeStamp.
		 *  Y bytes for lang.
		 */
		int X = 8;
		int Y = 2;
		byte[] key = new byte[X+Y];	
		System.arraycopy(Bytes.toBytes(timeStamp), 0, key, 0, X);
		System.arraycopy(Bytes.toBytes(lang), 0, key, X, Y);
		return key;
	}

	/* Generate the start of the key given a timestamp
	 * 
	 * @param timeStamp, timestamp of the tweet
	 * @return start key in bytes
	 * 
	 */
	public static byte[] generateStartKey(Long timeStamp) {
		int X = 8;
		int Y = 2;
		byte[] key = new byte[X+Y];
		
		System.arraycopy(Bytes.toBytes(timeStamp), 0, key, 0, X);
		for (int i = X; i < X+Y; i++){
			key[i] = (byte)-255;
		}
		return key;
	}
	
	/* Generate the end of the key given a timestamp
	 * 
	 * @param timeStamp, timestamp of the tweet
	 * @return end key in bytes
	 * 
	 */
	public static byte[] generateEndKey(Long timeStamp) {
		int X = 8;
		int Y = 2;
		byte[] key = new byte[X+Y];
		System.arraycopy(Bytes.toBytes(timeStamp), 0, key, 0, X);
		for (int i = X; i < X+Y; i++){
			key[i] = (byte)255;
		}
		return key;
	}
}
