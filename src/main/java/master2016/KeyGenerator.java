package master2016;

import org.apache.hadoop.hbase.util.Bytes;

public class KeyGenerator {
	
	public static byte[] generateKey(Long timeStamp, String lang){
		/* TODO: discuss the number of bytes.
		 * Structure of the key:
		 *  X bytes for timeStamp.
		 *  Y bytes for lang.
		 */
		int X = 8;
		int Y = 2;
		byte[] key = new byte[X+Y];
		/*
		 * The java.lang.System.arraycopy() method copies 
		 * an array from the specified source array, beginning at the specified position, 
		 * to the specified position of the destination array.
		 * 
		 * public static void arraycopy(Object src, int srcPos, Object dest, int destPos, int length)
		 * 
		 * src -- This is the source array.
		 * srcPos -- This is the starting position in the source array.
		 * dest -- This is the destination array.
		 * destPos -- This is the starting position in the destination data.
		 * length -- This is the number of array elements to be copied.
		 */
		System.arraycopy(Bytes.toBytes(timeStamp), 0, key, 0, X);
		System.arraycopy(Bytes.toBytes(lang), 0, key, X, Y);
		return key;
	}

}
