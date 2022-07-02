import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class Server {
	//Data set file name
	//The file names for R and L data (they will be created after training)
	//They may also be used during recommendation mode without training
	final String filename = "C:/Users/USER/Desktop/Erg/src/main/java/data/input_matrix_no_zeros.csv"; //Set it manually!
	final String filenameR = "C:/Users/USER/Desktop/Erg/src/main/java/data/Rdata.csv"; //Set it manually!
	final String filenameL = "C:/Users/USER/Desktop/Erg/src/main/java/data/Ldata.csv"; //Set it manually!
	//Columns and rows of the matrix
	final int columnsPOIs = 1964; //Set it manually!
	final int rowsUsers = 765; //Set it manually!
	//K << Max(rows,columns)
	final int K = 20; //Set it manually!
	//Value needed for cui matrix calculation
	final int alpha = 40; //Set it manually!
	//Value needed for the cost function
	final double lamda = 0.01; //Set it manually!
	//The error difference required for the training to stop
	final double threshold = 0.1; //Set it manually!
	//Define whether you want to train L and R before using the recommendation mode
	//Insert the value of false if you already have created L and R previously and you want to use them
	static boolean train;
	//Server's provider socket
	ServerSocket providerSocket;
	Socket connection;
	//Server's port
	static int Port;
	//An array to keep all the info about our workers
	//Threads will have a pointer to it in order to complete some functions
	WorkerInfo[] workers;
	//The matrix from the data set
	int[][] matrix = new int[rowsUsers][columnsPOIs];
	//As they are mentioned in the paper - xu and yi matrices
	RealMatrix L = MatrixUtils.createRealMatrix(columnsPOIs,K);
	RealMatrix R = MatrixUtils.createRealMatrix(rowsUsers,K);
	//Binary matrix that indicates the preference of user u to POI i
	int[][] P = new int[rowsUsers][columnsPOIs];
	//Integer matrix that indicates the confidence in observing P
	int[][] C = new int[rowsUsers][columnsPOIs];
	//A way to stop the server while the threads are doing some stuff
	CountDownLatch latch;
	//An array list to keep all the threads
	ArrayList<Thread> threads;
	//We define when to stop the training loop using this boolean variable
	static boolean stop;
	//An array to keep all the info about each POI
	POI[] pois = new POI[columnsPOIs];
	
	public static void main(String args[]) {
		Scanner scanner = new Scanner(System.in);
		System.out.print("Training (true/false): ");
		train = scanner.nextBoolean();
		System.out.print("Insert Port: ");
		Port = scanner.nextInt();
		//For train == true we call the openServer method to initiate everything
		//Then TrainMode will be called for the training
		//And finally RecommendationMode for the clients
		//For train == false only RecommendationMode will be called
		if(train) {
			System.out.print("Insert time to wait(ms): ");
			int time = scanner.nextInt();
			new Server().openServer(time);
		} else {
			new Server().openRecommendationMode();
		}
		System.out.println("Server is off!");
	}

	public void openServer(int TimeToWait) {
		try {
			providerSocket = new ServerSocket(Port);
			//After each new worker connection the server will wait 5 more seconds for more workers
            providerSocket.setSoTimeout(TimeToWait);
			//We will keep all the worker threads here
			threads = new ArrayList<Thread>();
			int i,j;
			//Create the basic matrices we need and read the data set
			CreateMatrices();
			System.out.println("Waiting for the workers to connect...");
			//For now we will save all the connected workers here
			ArrayList<WorkerInfo> temp = new ArrayList<WorkerInfo>();
			try {
				//First we accept connection from all possible workers
				while(true) {
					connection = providerSocket.accept();
					temp.add(new WorkerInfo(connection));
					System.out.println("Accepted connection from IP:"+connection.getInetAddress()+" Port:"+connection.getPort());
				}
			} catch(SocketTimeoutException Ex) {
				System.out.println("Total workers: "+temp.size());
			}
			workers = new WorkerInfo[temp.size()];
			//Now we will move them all to the array that we will use later
			j = temp.size();
			for(i=0;i<j;i++) {
				workers[i] = temp.remove(0);
			}
			//Then for each worker we start the thread on that connection
			latch = new CountDownLatch(workers.length);
			for(i=0;i<workers.length;i++) {
				Thread t = new ServerWorkerThread(latch,i,workers,matrix,L,R,lamda,alpha);
				t.start();
				//We add it to the list
				threads.add(t);
			}
			//The server now has to wait for all workers to finish writing their data on the board
			try {
				latch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			//Now we need to set boundaries for each worker on L and R matrices
			System.out.println("Distributing the work...");
			int TotalCores = 0;
			int TotalMemory = 0;
			//First we calculate the total memory and cores we have
			for(i=0;i<workers.length;i++) {
				TotalCores += workers[i].getCores();
				TotalMemory += workers[i].getMemory();
			}
			//Then for each worker we calculate the percentage of them he owns
			float percentCores;
			float percentMemory;
			float[] percent = new float[workers.length];
			for(i=0;i<workers.length;i++) {
				percentCores = workers[i].getCores()/(float)TotalCores;
				System.out.println("PercentCores #"+(i+1)+": "+percentCores);
				percentMemory = workers[i].getMemory()/(float)TotalMemory;
				System.out.println("PercentMemory #"+(i+1)+": "+percentMemory);
				//We get the average value between cores-percentage and memory-percentage
				percent[i] = (percentCores+percentMemory)/(float)2;
				System.out.println("Percentage #"+(i+1)+": "+percent[i]);
			}
			//And at last we will set boundaries to R and L for each worker according to that percentage
			DistributeWork(percent);
			for(i=0;i<workers.length;i++) {
				System.out.println("- Worker #"+(i+1)+" -");
				workers[i].print();
			}
			//Wake up all the threads
			latch = new CountDownLatch(workers.length);
			for(i=0;i<threads.size();i++) {
				((ServerWorkerThread)(threads.get(i))).renewLatch(latch);
				((ServerWorkerThread)(threads.get(i))).wakeUp();
			}
			System.out.println("Sending required data to each worker...");
			//The server will wait until all threads have done sending the data
			try {
				latch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println("Workers are ready for training");
			//Start the training mode
            openTrainingMode();
		} catch (IOException ioException) {
			ioException.printStackTrace();
		} finally {
			try {
				//After the training the server shuts down the socket
				providerSocket.close();
			} catch (IOException ioException) {
				ioException.printStackTrace();
			}
		}
		//Remove all the threads from the array list
		while(!threads.isEmpty()) {
			threads.remove(0);
		}
		//Start the recommendation mode
		openRecommendationMode();
	}
	
	public void openTrainingMode() {
        //Basically we have a thread for each worker to send and receive the data
        //This will be happening in a loop until the error difference gets a very small value
        //Then the created matrices will be ready for client use
		//Variable stop will get the value of true when the cost diff is smaller than the threshold
		int i;
		stop = false;
		//We wake up all the threads (start of the training)
		latch = new CountDownLatch(workers.length);
		for(i=0;i<threads.size();i++) {
			((ServerWorkerThread)(threads.get(i))).renewLatch(latch);
			((ServerWorkerThread)(threads.get(i))).wakeUp();
		}
		double previousCost = 10000000;
		double currentCost = 0;
		Scanner scanner = new Scanner(System.in);
		while(!stop) {
			//Now the threads are waiting for their workers to fix the L matrix
			//Once that's done each thread will receive the part of the L that his worker fixed
			//That thread will then change the values of the L(here) with the new values
			//Once all the threads have done that the server will wake up
			try {
				latch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			//We are using this system of latch+wakeup to keep an order at which the threads are working
			//For example that way we exclude the possibility of a thread to send the "fully updated L matrix"
			//back to his worker when other threads are not done updating it yet
			//Now the server will notify all the threads to wake up
			latch = new CountDownLatch(workers.length);
			for(i=0;i<threads.size();i++) {
				((ServerWorkerThread)(threads.get(i))).renewLatch(latch);
				((ServerWorkerThread)(threads.get(i))).wakeUp();
			}
			//The server will wait for the threads to send the fully updated L matrix back to each worker
			//Then the workers will execute the calculations for the R matrix based on the new L matrix
			//And finally the workers will send back the new values of R matrix to the threads
			//That way each thread can update the part of the R matrix that it is responsible for
			//The server has to wait for all that process to be done
			//Once all threads have updated the R matrix the server will wake up
			try {
				latch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			//Now he will wake up all the threads again
			//The woken up threads will send the fully updated R matrix back to each worker
			latch = new CountDownLatch(workers.length);
			for(i=0;i<threads.size();i++) {
				((ServerWorkerThread)(threads.get(i))).renewLatch(latch);
				((ServerWorkerThread)(threads.get(i))).wakeUp();
			}
			try {
				latch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			//Now we are in the phase where the workers have received the fully updated R matrix
			//While the threads are waiting, the server will have to calculate the cost
			//currentCost = calculateCostFunction();
			//Now that we have calculated the cost we need to check the difference with the previous value of it
			//If we have the desired result we will send the boolean value of true in each thread
			//That way the will break the loop and stop the training
			//If we don't have the desired result then we will have to do 1 or more rounds of training
			//In that case we will send to each thread the boolean value of false
			//That way we notify them to keep on training for 1 more round
			currentCost = calculateCostFunction();
			System.out.println("Cost: "+currentCost);
			latch = new CountDownLatch(workers.length);
			//Renew the values of latch and stop and then wake up all threads
			if(Math.abs(previousCost-currentCost) < threshold) {
				stop = true;
			}
            for(i=0;i<threads.size();i++) {
                ((ServerWorkerThread)(threads.get(i))).renewLatch(latch);
                ((ServerWorkerThread)(threads.get(i))).renewStop(stop);
                ((ServerWorkerThread)(threads.get(i))).wakeUp();
            }
			previousCost = currentCost;
			//If stop has the value of true then the loop will break
			//If not then we will do 1 more round of training
			//As mentioned above, the server will wait for the threads to received the new parts of L matrix etc
			System.out.println("Writing the 2 produced matrices in data folder...");
			//Write the produced X and Y in csv files
			//Firstly we need that in order to be able to use those 2 matrices in recommendations without training
			//Secondly this command is placed inside the loop for safety reasons
			//For example if a worker is down then the whole training procedure will crash
			//But we won't have to worry about that since we will have saved the data
			//That way we will be able to load them and continue the training from the part it stopped
			writeDataLR();
		}
		System.out.println("The training has been completed");
	}

	public void openRecommendationMode() {
		//If we are using L and R data from previous training then we just have to read them from their text files
		if(!train) {
			//We have stored the results from the previous training in the 2 csv files
			//All we have to do is read the data for the L and R matrices
			readDataLR();
		}
		//For this part we will use random fake data
		//This method will be removed and replaced when we will be given the POI data set
		createPOIs();
		try {
			providerSocket = new ServerSocket(Port);
			System.out.println("The server is now accepting client requests");
			try {
				//For each new client connection we start a thread
				//The thread ServerClient contains all the needed functions to calculate the recommendations
				while(true) {
					connection = providerSocket.accept();
					System.out.println("Accepted connection from IP:"+connection.getInetAddress()+" Port:"+connection.getPort());
					new ServerClientThread(connection,L,R,pois,rowsUsers,columnsPOIs,K).start();
				}
			} catch(SocketTimeoutException Ex) {
				System.err.println("No client connection received!");
			}
		} catch (IOException ioException) {
			ioException.printStackTrace();
		} finally {
		 	try {
				//After the training the server shuts down the socket
				providerSocket.close();
			} catch (IOException ioException) {
				ioException.printStackTrace();
			}
		}
	}

	//Set Rstart, Rfinish, Lstart and Lfinish to each worker according to his cores and memory
	//A simple method to distribute the work to each worker
	public void DistributeWork(float[] percentage) {
		int i;
		//The first worker gets to start from the beginning of both L and R matrices
		workers[0].setRstart(0);
		workers[0].setLstart(0);
		//According to his percentage we will set how many slots he can work on R and LDAPCertStoreParameters
		//Minus 1 is used in case we only have 1 worker (he gets the full matrix)
		workers[0].setRfinish((int)(percentage[0]*rowsUsers-1));
		workers[0].setLfinish((int)(percentage[0]*columnsPOIs-1));
		//Now we have to do the same thing for all others
		for(i=1;i<workers.length;i++) {
			//We get the slot where the previous worker ended and we begin at the exact next slot
			workers[i].setRstart(workers[i-1].getRfinish()+1);
			workers[i].setLstart(workers[i-1].getLfinish()+1);
			//As for the end we have 2 cases
			//If hes not the final worker then we set the boundaries like we did before
			//But if he is, then we just give him what's left from the matrices
			//The percentages don't exactly add up to 100 so the last worker gets a little bit more work (not much really)
			if(i==workers.length-1) {
				workers[i].setRfinish(rowsUsers-1);
				workers[i].setLfinish(columnsPOIs-1);
			} else {
				workers[i].setRfinish((int)(percentage[i]*rowsUsers)+workers[i-1].getRfinish());
				workers[i].setLfinish((int)(percentage[i]*columnsPOIs)+workers[i-1].getLfinish());
			}
		}
	}
	
	//Creates the C, P, L and R matrices
	public void CreateMatrices() {
		int i,j;
		//Initialize the matrix with zeros in all slots
		System.out.println("Loading the data set...");
		for(i=0;i<rowsUsers;i++) {
			for(j=0;j<columnsPOIs;j++) {
				matrix[i][j] = 0;
			}
		}
		//Create the matrix from the given data set
		readDataSetMatrix();
		System.out.println("K value: "+K);
        System.out.println("Alpha value: "+alpha);
		System.out.println("Lamda value: "+lamda);
		System.out.println("Threshold: "+threshold);
		//Filling R and L with random values from [0,1]
		Random rand = new Random();
		for(i=0;i<K;i++) {
			for(j=0;j<rowsUsers;j++) {
				R.setEntry(j,i,rand.nextDouble());
			}
			for(j=0;j<columnsPOIs;j++) {
				L.setEntry(j,i,rand.nextDouble());
			}
		}
		//Creating C and P matrices
		for(i=0;i<rowsUsers;i++) {
			for(j=0;j<columnsPOIs;j++) {
				if(matrix[i][j] == 0) {
					P[i][j] = 0;
					C[i][j] = 1;
				} else {
					P[i][j] = 1;
					C[i][j] = 1+alpha*matrix[i][j];
				}
			}
		}

	}
	
	//Method to read the data set from the csv file
	public void readDataSetMatrix() {
		String line;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(filename));
			while((line = br.readLine()) != null) {
				String[] words = line.split(", ");
				matrix[Integer.parseInt(words[0])][Integer.parseInt(words[1])] = Integer.parseInt(words[2]);
			}
		} catch(FileNotFoundException e) {
			e.printStackTrace();
		} catch(IOException e) {
			e.printStackTrace();
		} finally {
			if(br != null) {
				try {
					br.close();
				} catch(IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	//Writes in csv files all the data contained in L and R matrices
	//That way we will be able to run the client mode without training
	public void writeDataLR() {
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(filenameL));
			writer.write(String.valueOf(columnsPOIs)+" x "+String.valueOf(K)+"\n");
			for(int i=0;i<columnsPOIs;i++) {
				for(int j=0;j<K;j++) {
					writer.write(String.valueOf(L.getEntry(i,j)));
					writer.write(", ");
				}
				writer.write("\n");
			}
		} catch(Exception e) {
			System.err.println("Failed to write L data!");
			e.printStackTrace();
		} finally {
			try {
				writer.close();
			} catch(Exception e) {
				System.err.println("Failed to close BufferedWriter!");
			}
		}
		try {
			writer = new BufferedWriter(new FileWriter(filenameR));
			writer.write(String.valueOf(rowsUsers)+" x "+String.valueOf(K)+"\n");
			for(int i=0;i<rowsUsers;i++) {
				for(int j=0;j<K;j++) {
					writer.write(String.valueOf(R.getEntry(i,j)));
					writer.write(", ");
				}
				writer.write("\n");
			}
		} catch(Exception e) {
			System.err.println("Failed to write R data!");
			e.printStackTrace();
		} finally {
			try {
				writer.close();
			} catch(Exception e) {
				System.err.println("Failed to close BufferedWriter!");
			}
		}
	}

	//Reads the L and R matrices (created in previous training)
	public void readDataLR() {
		String line;
		BufferedReader br = null;
		//Reads the data for L matrix from csv
		try {
			System.out.println("Reading L matrix...");
			br = new BufferedReader(new FileReader(filenameL));
			br.readLine();
			for(int i=0;i<columnsPOIs;i++) {
				line = br.readLine();
				String[] words = line.split(", ");
				for(int j=0;j<K;j++) {
					L.setEntry(i,j,Double.parseDouble(words[j]));
				}
			}
		} catch(FileNotFoundException e) {
			e.printStackTrace();
		} catch(IOException e) {
			e.printStackTrace();
		} finally {
			if(br != null) {
				try {
					br.close();
				} catch(IOException e) {
					e.printStackTrace();
				}
			}
		}
		//Reads data for R matrix from csv
		try {
			System.out.println("Reading R matrix...");
			br = new BufferedReader(new FileReader(filenameR));
			br.readLine();
			for(int i=0;i<rowsUsers;i++) {
				line = br.readLine();
				String[] words = line.split(", ");
				for(int j=0;j<K;j++) {
					R.setEntry(i,j,Double.parseDouble(words[j]));
				}
			}
		} catch(FileNotFoundException e) {
			e.printStackTrace();
		} catch(IOException e) {
			e.printStackTrace();
		} finally {
			if(br != null) {
				try {
					br.close();
				} catch(IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	//Calculates the cost
	public double calculateCostFunction() {
		double cost = 0;
		for (int i=0;i<rowsUsers;i++) {
			for (int j=0;j<columnsPOIs;j++) {
				double retXuYi=retXuYi(i,j);
				cost = cost + (C[i][j] * (P[i][j] - retXuYi) * (P[i][j] - retXuYi));
			}
		}
		return (cost+ lamda * (calculateNorm(L, R)));
	}

	//Calculates the norm
	public double calculateNorm(RealMatrix L, RealMatrix R) {
		double part1 = 0;
		double part2 = 0;
		for (int i=0;i<rowsUsers;i++) {
			for(int j=0;j<K;j++) {
				part1 = part1 + R.getEntry(i, j)*R.getEntry(i,j);
			}
		}
		for (int i=0;i<columnsPOIs;i++) {
			for(int j=0;j<K;j++)
			{
				part2=part2+L.getEntry(i,j)*L.getEntry(i,j);
			}
		}
		return part1 + part2;
	}

	//Returns Xu
	public double[][] getXu(int i) {
		double[][] arr = new double[1][K];
		for (int j=0;j<K;j++) {
			arr[0][j] = R.getEntry(i,j);
		}
		return arr;
	}
	
	//Returns Yi
	public double[][] getYi(int i) {
		double[][] arr = new double[1][K];
		for (int j=0;j<K;j++) {
			arr[0][j] = L.getEntry(i,j);
		}
		return arr;
	}

	//Calculates Xu*Yi
	public double retXuYi(int x, int y) {
		RealMatrix Xu = MatrixUtils.createRealMatrix(getXu(x));
		RealMatrix Yi = MatrixUtils.createRealMatrix(getYi(y)).transpose();
		RealMatrix result = Xu.multiply(Yi);
		return result.getEntry(0, 0);
	}

	//We can only use fake data for now
	public void createPOIs() {
		int i;
		for(i=0;i<columnsPOIs;i++) {
			pois[i] = new POI(i,"POI_"+String.valueOf(i),0,0,"Category");
		}
	}
}