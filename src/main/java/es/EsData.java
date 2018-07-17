package es;

public class EsData{
    private String data;
	private String id;
	public String getData() {
		return data;
	}
	public void setData(String data) {
		this.data = data;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}

	public EsData(){}

	public EsData(String id,String data) {
		this.data = data;
		this.id = id;
	}
}