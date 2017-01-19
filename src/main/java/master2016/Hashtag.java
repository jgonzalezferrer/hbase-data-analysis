package master2016;

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
	
	public void incrementCount(int addCount){
		this.count+=addCount;
	}

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
