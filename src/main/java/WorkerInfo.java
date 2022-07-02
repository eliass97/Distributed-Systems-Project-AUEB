import java.net.*;

//A class that keeps all the data about a worker
public class WorkerInfo {
	private Socket connection;
	private int cores;
	private long memory;
	//The parts of L and R which are assigned to this specific worker
	//The worker can only change values in the spaces [Lstart,Lfinish] and [Rstart,Rfinish] of L and R matrices
	private int Lstart, Lfinish, Rstart, Rfinish;
	
	//Constructor
	public WorkerInfo(Socket connection) {
		this.connection = connection;
	}
	
	public Socket getConnection() {
		return connection;
	}
	
	public int getCores() {
		return cores;
	}
	
	public long getMemory() {
		return memory;
	}
	
	public void setCores(int cores) {
		this.cores = cores;
	}
	
	public void setMemory(long memory) {
		this.memory = memory;
	}
	
	public void setRstart(int x) {
		Rstart = x;
	}
	
	public void setLstart(int x) {
		Lstart = x;
	}
	
	public void setRfinish(int x) {
		Rfinish = x;
	}
	
	public void setLfinish(int x) {
		Lfinish = x;
	}
	
	public int getRstart() {
		return Rstart;
	}
	
	public int getLstart() {
		return Lstart;
	}
	
	public int getRfinish() {
		return Rfinish;
	}
	
	public int getLfinish() {
		return Lfinish;
	}
	
	public synchronized void print() {
		System.out.println("IP: "+connection.getInetAddress());
		System.out.println("Port: "+connection.getPort());
		System.out.println("Cores: "+cores);
		System.out.println("Free Memory: "+memory);
		System.out.println("Rstart: "+Rstart+" Rfinish: "+Rfinish);
		System.out.println(" Lstart: "+Lstart+" Lfinish: "+Lfinish);
	}
}