package pubsubapp.vo;

public class Query {
	private String fisrtQuery;
	private String secondQuery;

	public String getFisrtQuery() {
		return fisrtQuery;
	}

	public void setFisrtQuery(String fisrtQuery) {
		this.fisrtQuery = fisrtQuery;
	}

	public String getSecondQuery() {
		return secondQuery;
	}

	public void setSecondQuery(String secondQuery) {
		this.secondQuery = secondQuery;
	}

	@Override
	public String toString() {
		return "Query [fisrtQuery=" + fisrtQuery + ", secondQuery=" + secondQuery + "]";
	}

}
