package master2016;

/** Hashtag: storing the text, language and number of occurrences of a given hashtag.
 * 
 * @param text, text of the hashtag
 * @param lang, language of the hashtag
 * @param count, number of occurrences of the hashtag
 *  
 */
public class Hashtag implements Comparable<Hashtag>{

	private String text;
	private String lang;
	private int count;
	

	public Hashtag (String text, String lang, int count){
		this.text=text;
		this.lang=lang;
		this.count=count;
	}

	public String getText(){
		return this.text;
	}

	public String getLang(){
		return this.lang;
	}
	
	public int getCount(){
		return this.count;
	}
	
	/* Incremets the number of occurrences of the hashtag
	 * 
	 * @param addCount, number of count to update.
	 * 
	 */
	public void incrementCount(int addCount){
		this.count+=addCount;
	}

	/* compareTo overrided to define a new total ordering.
	 * 
	 * It first order by value, then it order alphabetically.
	 */
	@Override
	public int compareTo(Hashtag o) {
		// TODO Auto-generated method stub
		int x1 = this.count;
		int x2 = o.getCount();

		if(x1 != x2){
			return x1-x2;
		}
		else{
			String s1 = this.text;
			String s2 = o.getText();
			return s2.compareTo(s1);   	
		}
	}
}
