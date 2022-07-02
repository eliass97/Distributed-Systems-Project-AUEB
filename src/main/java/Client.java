import java.net.*;
import java.io.*;
import java.util.*;

public class Client {
	static int id;
	static POI poi;
	static int Ktop;
	
	public Client(int id, POI poi, int Ktop) {
		this.id = id;
		this.Ktop = Ktop;
		this.poi = poi;
	}
	
	public static void main(String args[]) {
		//Fake temporary POI for the user
		//We cant use it for part A of the assignment
		POI temp = new POI(0,"POI_0",0,0,"Category");
		Scanner scan = new Scanner(System.in);
		//Client simply enters his ID and the amount of POIs he wants in return
		System.out.print("Insert User ID: ");
		int x = scan.nextInt();
		System.out.print("Insert Number of Requested POIs: ");
		int y = scan.nextInt();
		Client client = new Client(x,temp,y);
		ObjectOutputStream out = null;
		ObjectInputStream in = null;
		Socket requestSocket = null;
		try {
			//Client then has to connect to the server
			Scanner scanner = new Scanner(System.in);
			System.out.print("Insert Server IP: ");
			String connectToIP = scanner.nextLine();
			System.out.print("Insert Server Port: ");
			int Port = scanner.nextInt();
			requestSocket = new Socket(connectToIP,Port);
			out = new ObjectOutputStream(requestSocket.getOutputStream());
			in = new ObjectInputStream(requestSocket.getInputStream());
			//Client sends hid id and K
			out.writeInt(id);
			out.flush();
			out.writeInt(Ktop);
			out.flush();
			System.out.println("Results:");
			//Finally he receives the results from the server
			for(int i=0;i<Ktop;i++) {
				System.out.println((i+1)+")");
				//Print the returned POI's data
				try {
					((POI)in.readObject()).print();
				} catch(ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
		} catch (UnknownHostException unknownHost) {
			System.err.println("You are trying to connect to an unknown host!");
		} catch (IOException ioException) {
			ioException.printStackTrace();
		} finally {
			try {
				in.close();
				out.close();
				requestSocket.close();
			} catch (IOException ioException) {
				ioException.printStackTrace();
			}
		}
	}
}