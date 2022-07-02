import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.QRDecomposition;
import org.apache.commons.math3.linear.RealMatrix;

import java.io.*;
import java.net.*;
import java.util.*;

public class Worker extends Thread {
	//Each worker is connected to a specific IP(server) and has a number of cores and RAM
	int cores;
	long ram;
	String connectToIP;
	int Port;
	//For more info on those matrices check ServerThread
    int[][] matrix;
	int[][] pui;
	int[][] cui;
	RealMatrix L;
	RealMatrix R;
	//Basically the parts of R and L he is working on
	int Lstart, Lfinish, Rstart, Rfinish;
	int K, alpha, rowsUsers, columnsPOIs;
	double lamda;
	boolean stop;
	
	//Constructor
	public Worker(int cores, long ram, String connectToIP, int Port) {
		this.cores = cores;
		this.ram = ram;
		this.connectToIP = connectToIP;
		this.Port = Port;
		stop = false;
	}
	
	//The worker will first find the number of cores and RAM he has
	//Then he will send those to the server for identification
	public static void main(String args[]) {
		int c = Runtime.getRuntime().availableProcessors();
		System.out.println("Cores: "+c);
		long m = Runtime.getRuntime().freeMemory();
		System.out.println("Free Memory: "+m);
		Scanner scanner = new Scanner(System.in);
		System.out.print("Insert Server IP: ");
		String IP = scanner.nextLine();
		System.out.print("Insert Server Port: ");
		int port = scanner.nextInt();
		new Worker(c,m,IP,port).start();
	}
	
	public void run() {
		Socket requestSocket = null;
		ObjectOutputStream out = null;
		ObjectInputStream in = null;
		try {
			int i, j;
			//First the worker requests a connection to the server
			requestSocket = new Socket(connectToIP,Port);
			out = new ObjectOutputStream(requestSocket.getOutputStream());
			System.out.println("Waiting for response from the server...");
			in = new ObjectInputStream(requestSocket.getInputStream());
			//Then he gives info about his cores and max memory
			out.writeInt(cores);
			out.flush();
			out.writeLong(ram);
			out.flush();
			//First it gets from the thread some important data (check ServerThread for more info)
			Lstart = in.readInt();
			System.out.println("Lstart: "+Lstart);
			Lfinish = in.readInt();
			System.out.println("Lfinish: "+Lfinish);
			Rstart = in.readInt();
			System.out.println("Rstart: "+Rstart);
			Rfinish = in.readInt();
			System.out.println("Rfinish: "+Rfinish);
			rowsUsers = in.readInt();
			System.out.println("RowsUsers: "+rowsUsers);
			columnsPOIs = in.readInt();
			System.out.println("ColumnsPOIs: "+columnsPOIs);
			K = in.readInt();
			System.out.println("K: "+K);
			alpha = in.readInt();
			System.out.println("Alpha: "+alpha);
			lamda = in.readDouble();
			System.out.println("Lamda: "+lamda);
			//Then we need to receive the C, P, L, R matrices
			cui = new int[rowsUsers][columnsPOIs];
			pui = new int[rowsUsers][columnsPOIs];
			L = MatrixUtils.createRealMatrix(columnsPOIs,K);
			R = MatrixUtils.createRealMatrix(rowsUsers,K);
			System.out.println("Receiving the matrix...");
			//Worker will receive the matrix and then create C and P according to it
            matrix = new int[rowsUsers][columnsPOIs];
			for(i=0;i<rowsUsers;i++) {
				for(j=0;j<columnsPOIs;j++) {
					matrix[i][j] = 0;
				}
			}
			int value = in.readInt();
			//The worker will save the given values to the matrix
            //When the worker receives -1 as value he will stop the loop
            //After that he will create the C and P matrices
			while(value != -1) {
			    i = in.readInt();
			    j = in.readInt();
			    matrix[i][j] = value;
			    value = in.readInt();
            }
			CreateCP();
			//Then the worker will receive L and R matrices
			System.out.println("Receiving L & R matrices...");
			for(i=0;i<K;i++) {
				for(j=0;j<columnsPOIs;j++) {
					L.setEntry(j,i,in.readDouble());
				}
				for(j=0;j<rowsUsers;j++) {
					R.setEntry(j,i,in.readDouble());
				}
			}
			System.out.println("The training has begun!");
			//Start the training
			while(!stop) {
				//The worker will calculate the new values of L (for the part that he is assigned to)
				System.out.println("Calculating L matrix...");
				calculateY();
				System.out.println("Calculated L matrix");
				//After the calculations are done, he will send the new part of L back to the thread
				System.out.println("Sending L matrix...");
				for(i=0;i<K;i++) {
					for(j=Lstart;j<=Lfinish;j++) {
						out.writeDouble(L.getEntry(j,i));
						out.flush();
					}
				}
				System.out.println("Sent L matrix");
				//Now the worker will receive the fully updated L matrix from the server
                System.out.println("Receiving L matrix...");
				for(i=0;i<K;i++) {
					for(j=0;j<columnsPOIs;j++) {
						L.setEntry(j,i,in.readDouble());
					}
				}
				System.out.println("Reiceived L matrix");
				//Now he can calculate the new values for R based on the new L
				System.out.println("Calculating R matrix...");
				calculateX();
				System.out.println("Calculated R matrix");
				//Then he will send back to the thread all the changes he made on R
				System.out.println("Sending R matrix...");
				for(i=0;i<K;i++) {
					for(j=Rstart;j<=Rfinish;j++) {
						out.writeDouble(R.getEntry(j,i));
						out.flush();
					}
				}
				System.out.println("Sent R matrix");
				//After all threads have fully updated the R matrix in the server, the worker will receive it
				System.out.println("Reiceiving R matrix...");
				for(i=0;i<K;i++) {
					for(j=0;j<rowsUsers;j++) {
						R.setEntry(j,i,in.readDouble());
					}
				}
				System.out.println("Reiceived R matrix");
				//Finally the worker will receive a boolean value that indicates whether he must stop the training or not
				System.out.println("Waiting for response from server...");
				stop = in.readBoolean();
				if(!stop) {
					System.out.println("Server ordered 1 more round of training");
				}
			}
			System.out.println("Training has been completed");
			System.out.println("Worker is off!");
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

	//Creates C and P matrices
    public void CreateCP() {
        int i,j;
        for(i=0;i<rowsUsers;i++) {
            for(j=0;j<columnsPOIs;j++) {
                if(matrix[i][j] == 0) {
                    pui[i][j] = 0;
                    cui[i][j] = 1;
                } else {
                    pui[i][j] = 1;
                    cui[i][j] = 1+alpha*matrix[i][j];
                }
            }
        }
    }

	//Returns the array P(u) which is all the preferences of a certain user (1xM)
	public RealMatrix createPu(int user) {
		double[][] Pu = new double[columnsPOIs][1];
		for(int i=0;i<columnsPOIs;i++) {
			Pu[i][0] = pui[user][i];
		}
		return MatrixUtils.createRealMatrix(Pu);
	}

	//Returns a diagonal array which has the following property:
	//For a certain user when one spot on the array we have x=y this tile contains C[u][i]
	public RealMatrix createCu(int user) {
		double[] Cu=new double[columnsPOIs];
		for(int j=0;j<columnsPOIs;j++) {
			Cu[j] = cui[user][j];
		}
		return MatrixUtils.createRealDiagonalMatrix(Cu);
	}

	//Creates the I matrix, which contains 1 when we have x=y,this matrix is (i,i) in dimensions
	public RealMatrix createI(int i) {
		double[] temp = new double[i];
		for(int j=0;j<i;j++) {
			temp[j] = 1;
		}
		return MatrixUtils.createRealDiagonalMatrix(temp);
	}

	//Returns the array P(u) which is all the preferences of a certain user (1xM)
	public RealMatrix createPi(int item) {
		double[][] Pi = new double[rowsUsers][1];
		for(int i=0;i<rowsUsers;i++) {
			Pi[i][0]=pui[i][item];
		}
		return MatrixUtils.createRealMatrix(Pi);
	}

	//Returns a diagonal array which has the following property :
	//for a certain user when one spot on the array we have x=y this
	//tile contains C[u][i]
	public RealMatrix createCi(int item) {
		double[] Ci = new double[rowsUsers];
		for (int j = 0; j < rowsUsers; j++) {
			Ci[j] = cui[j][item];
		}
		return MatrixUtils.createRealDiagonalMatrix(Ci);
	}

    public void setXuRow(int row, RealMatrix mat) {
        for (int i = 0; i < K; i++) {
            R.setEntry(row, i, mat.getEntry(i, 0));
        }
    }

    public void setYiRow(int row,RealMatrix mat) {
        for(int i=0;i<K;i++) {
            L.setEntry(row,i,mat.getEntry(i,0));
        }
    }

    public void calculateX() {
		RealMatrix Y = L;
		RealMatrix Ytemp = Y.transpose();
		RealMatrix I1 = createI(columnsPOIs);
		RealMatrix I2 = createI(K);
		RealMatrix temp1 = Ytemp.multiply(Y);
		RealMatrix temp3 = I2.scalarMultiply(lamda);
		for(int i=Rstart;i<=Rfinish;i++) {
			//We split the multiplications so that the code looks more elegant
			RealMatrix Cu = createCu(i);//we create this in order to not calculate it again
			//temp1,2,3 all add into part1, which is the part that we inverse from the array.
			RealMatrix temp2 = (Ytemp.multiply(Cu.subtract(I1))).multiply(Y);
			RealMatrix t1 = (temp1.add(temp2)).add(temp3);
			QRDecomposition t2 = new QRDecomposition(t1);
			RealMatrix part1 = t2.getSolver().getInverse();
			RealMatrix part2 = (Ytemp.multiply(Cu)).multiply(createPu(i));
			//Multiplies the 2 parts and then it edits the L matrix by making sure that the i'th row has the
			//values that the matrix multiplication gave us
			setXuRow(i,part1.multiply(part2));
		}
	}

	public void calculateY() {
		RealMatrix X = R;
		RealMatrix Xtemp = X.transpose();
		RealMatrix I4 = createI(rowsUsers);
		RealMatrix I5 = createI(K);
		RealMatrix temp1 = Xtemp.multiply(X);
		RealMatrix temp3 = I5.scalarMultiply(lamda);
		for(int i=Lstart;i<=Lfinish;i++) {
			//we split the multiplications so that the code looks more elegant
			RealMatrix Ci = createCi(i);//we create this in order to not calculate it again
			RealMatrix temp2 = (Xtemp.multiply(Ci.subtract(I4))).multiply(X);
			RealMatrix t1 = (temp1.add(temp2)).add(temp3);
			QRDecomposition t2 = new QRDecomposition(t1);
			RealMatrix part1 = t2.getSolver().getInverse();
			//!!On part2 i've transposed the matrix so that it can be multiplied with the other things in Xu
			RealMatrix part2 = (Xtemp.multiply(Ci)).multiply(createPi(i));
			//Multiplies the 2 parts and then it edits the L matrix
			setYiRow(i,part1.multiply(part2));
		}
	}
}