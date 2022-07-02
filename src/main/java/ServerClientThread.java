import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;

import java.io.*;
import java.net.*;

//Thread that execute the recommendation part of the server
public class ServerClientThread extends Thread {
	ObjectInputStream in;
    ObjectOutputStream out;
	//Connection between server and the client
	Socket connection;
	//The matrices for X and Y
	RealMatrix L;
	RealMatrix R;
	POI[] pois;
	int rowsUsers, columnsPOIs, K;
	
	public ServerClientThread(Socket connection, RealMatrix L, RealMatrix R, POI[] pois, int rowsUsers, int columnsPOIs, int K) {
		this.connection = connection;
		this.L = L;
		this.R = R;
		this.pois = pois;
		this.rowsUsers = rowsUsers;
		this.columnsPOIs = columnsPOIs;
		this.K = K;
		try {
			out = new ObjectOutputStream(connection.getOutputStream());
			in = new ObjectInputStream(connection.getInputStream());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void run() {
		try {
			//The thread gets the user's id and how many POIs he wants
            int userID = in.readInt();
            int Ktop = in.readInt();
            System.out.println("User #"+userID+" requested the top "+Ktop+" POIs");
            //The thread uses the function to calculate the result
            int[] results = recommend(userID,Ktop);
            //Finally the thread returns the K top POIs that the user requested
            for(int i=0;i<Ktop;i++) {
                out.writeObject(pois[results[i]]);
			    out.flush();
            }
			System.out.println("User #"+userID+" has received the results");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				in.close();
				out.close();
			} catch (IOException ioException) {
				ioException.printStackTrace();
			}
		}
	}
	
	//Recommends the num most suitable places for the user to visit
	public int[] recommend(int user, int num) {
		double[] P = new double[columnsPOIs];
		RealMatrix Xu = MatrixUtils.createRealMatrix(getXu(user));
		for(int j=0;j<columnsPOIs;j++) {
			RealMatrix Yi = MatrixUtils.createRealMatrix(getYi(j));
			P[j] = (Xu.multiply(Yi.transpose())).getEntry(0,0);
		}
		int[] recommendations = new int[num];
		double max = -999;
		int pointer = -1;
		for(int i=0;i<num;i++) {
			for(int j=0;j<columnsPOIs;j++) {
				if(P[j]>max) {
					max = P[j];
					pointer = j;
				}
			}
			recommendations[i] = pointer;
			P[pointer] = -1;
			pointer = -1;
			max = -1;
		}
		return recommendations;
	}

	//Method taken from Server.java
	//Returns Xu
	public double[][] getXu(int i) {
		double[][] arr = new double[1][K];
		for (int j=0;j<K;j++) {
			arr[0][j] = R.getEntry(i,j);
		}
		return arr;
	}

	//Method taken from Server.java
	//Returns Yi
	public double[][] getYi(int i) {
		double[][] arr = new double[1][K];
		for (int j=0;j<K;j++) {
			arr[0][j] = L.getEntry(i,j);
		}
		return arr;
	}
}