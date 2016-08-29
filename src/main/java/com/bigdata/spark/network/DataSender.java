package com.bigdata.spark.network;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;

public class DataSender {

	public static void main(String[] args) throws Exception {
		
		Random random = new Random();
		String[] arrays = new String[]{"A","B","C","D","E"};
		
		ServerSocket ss = new ServerSocket(9999);
		Socket socket = ss.accept();
		OutputStream os = socket.getOutputStream();
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os));
		
		while(true) {
			int index = random.nextInt(4);
			System.out.println(arrays[index]);
			bw.write(arrays[index]);
			bw.newLine();
			bw.flush();
			Thread.currentThread().sleep(1000l);
		}
	}

}
