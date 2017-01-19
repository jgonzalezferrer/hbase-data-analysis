package master2016;

public class Hashtag {

	private String text;
	private int count;
	
	public Hashtag (String text, int count){
		this.text=text;
		this.count=count;
	}
	
	public String getText(){
		return this.text;
	}
	
	public int getCount(){
		return this.count;
	}
	

}
