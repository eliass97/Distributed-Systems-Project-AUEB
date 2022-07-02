import org.apache.commons.math3.linear.RealMatrix;

import java.io.*;
import java.net.*;
import java.util.concurrent.CountDownLatch;

//This class is used so that the server threads can identify the workers and then assign work to them
//They get their cores and RAM
//Then they save the info in the workers array
//And finally they execute the training
//After the training this class will not be used anymore
public class ServerWorkerThread extends Thread {
    ObjectInputStream in;
    ObjectOutputStream out;
	//Connection between server and a specific worker
	Socket connection;
	//The board in which we have data about all connected workers
	WorkerInfo[] workers;
	int pos;
	//Workers will have the matrix from which C and P will be created by each one of them
	//C,P,L and R will be used during the training
	int[][] matrix;
	//The matrices for X and Y
	RealMatrix L;
	RealMatrix R;
	//We need this to synchronize specific server-thread actions
	CountDownLatch latch;
	double lamda;
	int K, alpha, rowsUsers, columnsPOIs;
	//We use this variable to define when the training will stop
	boolean stop;
	
	public ServerWorkerThread(CountDownLatch latch, int position, WorkerInfo[] workers, int[][] matrix, RealMatrix L, RealMatrix R, double lamda, int alpha) {
		this.latch = latch;
		pos = position;
		connection = workers[pos].getConnection();
		try {
			out = new ObjectOutputStream(connection.getOutputStream());
			in = new ObjectInputStream(connection.getInputStream());
		} catch (IOException e) {
			e.printStackTrace();
		}
		//Pointers to the Server matrices
		//That way we pass the changes to the server for common use
		this.workers = workers;
		//L and R change during the training so we will have to send and receive them time by time
		this.matrix = matrix;
		this.L = L;
		this.R = R;
		this.lamda = lamda;
		this.alpha = alpha;
		stop = false;
	}
	
	public void run() {
		int cores;
		long memory;
		try {
			int i,j;
			//First we get the cores and max memory
			cores = in.readInt();
			memory = in.readLong();
			//Now that we have the cores+memory of the worker we need to update the array
			synchronized(workers) {
				workers[pos].setCores(cores);
				workers[pos].setMemory(memory);
			}
			System.out.println("Added Cores & Memory: "+workers[pos].getConnection().getInetAddress()+" "+workers[pos].getConnection().getPort());
			//Notify the server that this thread is done getting the cores+memory
			//Once he gets notified by all workers he will continue
			latch.countDown();
			//The thread now has to wait until the server completes the needed changes in workers array
			//The server will compute the amount of R and L each worker will be assigned
			synchronized(this) {
				try {
					wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			//Now the thread must send all that data to the worker
			//First it will send Lstart, Lfinish, Rstart, Rfinish
			//Then rows, columns, K, a and lamda
			//And finally L and R
			out.writeInt(workers[pos].getLstart());
			out.flush();
			out.writeInt(workers[pos].getLfinish());
			out.flush();
			out.writeInt(workers[pos].getRstart());
			out.flush();
			out.writeInt(workers[pos].getRfinish());
			out.flush();
			rowsUsers = matrix.length;
			out.writeInt(rowsUsers);
			out.flush();
			columnsPOIs = matrix[0].length;
			out.writeInt(columnsPOIs);
			out.flush();
			K = R.getColumnDimension();
			out.writeInt(K);
			out.flush();
			out.writeInt(alpha);
			out.flush();
			out.writeDouble(lamda);
			out.flush();
			//First we will send the matrix values (only non-zeros)
			for(i=0;i<rowsUsers;i++) {
				for(j=0;j<columnsPOIs;j++) {
					if(matrix[i][j] != 0) {
						out.writeInt(matrix[i][j]);
						out.flush();
						out.writeInt(i);
						out.flush();
						out.writeInt(j);
						out.flush();
					}
				}
			}
			//Send this to notify the worker that he got all the needed values for the matrix
			out.writeInt(-1);
			out.flush();
			//Now we need to send the L and R matrices
			for(i=0;i<K;i++) {
				for(j=0;j<columnsPOIs;j++) {
					out.writeDouble(L.getEntry(j,i));
					out.flush();
				}
				for(j=0;j<rowsUsers;j++) {
					out.writeDouble(R.getEntry(j,i));
					out.flush();
				}
			}
			System.out.println("Worker #"+pos+" has received all primary data");
			//Notify the server that this thread has sent all primary data
			latch.countDown();
			//Wait for the server to notify this thread for the begining of the training
			synchronized(this) {
				try {
					wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			//Start the training
			while(!stop) {
				//Receive the part of L that the worker (for this thread) has changed
				//Meanwhile copy those changes to the actual L matrix
				for(i=0;i<K;i++) {
					for(j=workers[pos].getLstart();j<=workers[pos].getLfinish();j++) {
						L.setEntry(j,i,in.readDouble());
					}
				}
				System.out.println("Thread #"+pos+" has received and updated L");
				//Notify the server that this thread has updated L matrix
				latch.countDown();
				//Wait for the server to notify this thread (server will wake up when all threads have updated L matrix)
				synchronized(this) {
					try {
						wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				//Now that the L matrix has been fully updated by each thread, we can send it to the worker
				//Send the fully updated L matrix
				for(i=0;i<K;i++) {
					for(j=0;j<columnsPOIs;j++) {
						out.writeDouble(L.getEntry(j,i));
						out.flush();
					}
				}
				System.out.println("Thread #"+pos+" has sent full L");
				//The worker will calculate R based on the new L
				//The thread will receive the part of R that the worker (for this thread) has changed
				//Meanwhile the thread will copy the changes to the actual R matrix
				for(i=0;i<K;i++) {
					for(j=workers[pos].getRstart();j<=workers[pos].getRfinish();j++) {
						R.setEntry(j,i,in.readDouble());
					}
				}
				System.out.println("Thread #"+pos+" has received and updated R");
				//Notify the server about the update on R matrix
				latch.countDown();
				//Wait for the server to notify this thread (server will wake up when all threads have updated R matrix)
				synchronized(this) {
					try {
						wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				//Now that the R matrix has been fully updated we can send it to the worker
				//Send the fully updated R matrix
				for(i=0;i<K;i++) {
					for(j=0;j<rowsUsers;j++) {
						out.writeDouble(R.getEntry(j,i));
						out.flush();
					}
				}
				System.out.println("Thread #"+pos+" has sent full R");
				//Notify the server that this thread has completed this round of training
				latch.countDown();
				//Wait for response from the server (all threads must have updated and sent R to their workers)
				//This time, before the server wakes up the thread he will renew the boolean variable stop
				//If the training has reached a desired level then the value will be changed to true
				//Thus ending the loop
				synchronized(this) {
					try {
						wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				//No matter if the variable changed or not we need to notify the worker
				//We will send him the variable stop to notify him weather he should stop the training or not
				out.writeBoolean(stop);
				out.flush();
			}
			//When the loop ends the workers will stop their function too (since they received the value of true)
			//After that the server can enter recMode where he will receive requests from clients
			System.out.println("Thread #"+pos+" is off!");
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

	//That way the server can notify the threads when to stop the training
	public void renewStop(boolean stop) {
		this.stop = stop;
	}

	//That way the server can renew the variable latch so that he can wait for all the threads again
	public void renewLatch(CountDownLatch latch) {
		this.latch = latch;
	}
	
	public void wakeUp() {
		synchronized(this) {
			notify();
		}
	}
}