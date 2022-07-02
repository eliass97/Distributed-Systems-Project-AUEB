//A class that keeps all the data about a POI
public class POI implements java.io.Serializable{
	private int id;
	private String name;
	private double latitude;
	private double longtitude;
	private String category;
	
	//Constructor
	public POI(int id, String name, double latitude, double longtitude, String category) {
		this.id = id;
		this.name = name;
		this.latitude = latitude;
		this.longtitude = longtitude;
		this.category = category;
	}
	
	public void setID(int id) {
		this.id = id;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}
	
	public void setLongtitude(double longtitude) {
		this.longtitude = longtitude;
	}
	
	public void setCategory(String category) {
		this.category = category;
	}
	
	public int getID() {
		return id;
	}
	
	public String getName() {
		return name;
	}
	
	public double getLatitude() {
		return latitude;
	}
	
	public double getLongtitude() {
		return longtitude;
	}
	
	public String getCategory() {
		return category;
	}
	
	public void print() {
		System.out.println("ID: "+id);
		System.out.println("Name: "+name);
		System.out.println("Longtitude: "+longtitude);
		System.out.println("Latitude: "+latitude);
		System.out.println("Category: "+category);
	}
}