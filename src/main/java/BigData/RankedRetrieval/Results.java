package BigData.RankedRetrieval;

public class Results implements Comparable<Results> {
	private double score;
    private String url;

    //constructor
    public Results(String url, double score) {
        this.score = score;
        this.url = url;
    }

    //get the url
    public String getUrl() {
        return url;
    }
    
    //get the score
    public double getScore() {
        return score;
    }

    //compare Result objects
	@Override
	public int compareTo(Results o) {
		// TODO Auto-generated method stub
		return this.getScore() <= o.getScore() ? 1 : -1;
	}
}
