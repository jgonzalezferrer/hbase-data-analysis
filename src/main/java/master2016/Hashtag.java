package master2016;

public class Hashtag implements Comparable<Hashtag>{

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
			return s1.compareTo(s2);   	
		}
	}
}
