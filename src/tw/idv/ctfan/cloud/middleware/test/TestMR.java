package tw.idv.ctfan.cloud.middleware.test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.net.Socket;

import tw.idv.ctfan.cloud.middleware.Cluster.JobType;
import tw.idv.ctfan.cloud.middleware.MapReduce.MRJobType;

public class TestMR {

	public static void main(String[] args) {
		try {
		
			//String m_URL = "10.133.200.1";
			String m_URL = "120.126.145.102";
			String m_port = "50031";
			String m_pathToFile = "C:\\ctfan\\AssoRule2013.jar";
			String m_paramInput = "/usr/ctfan/input";
			String m_paramOutput ="/usr/ctfan/output/output";
			
			JobType mapR = new MRJobType();
			
//			int testSize[] = {2};
//			int testSize[] = {5,5,5};
//			int testSize[] = {4,5,5,5,5,5,5,5,5,5,5};
//			int testSize[] = {5,5,5,5,5,5,5,5,2};
//			int testSize[] = {5,5,4};
//			int testSize[] = {5,5,5,5,5};
//			int testSize[] = {5,5,5,5,5,5,5,5,5,5};
			
			/// 5, 10, 15, 20 test data
//			int testSize[] = {3,1,4,2,0};
//			int testSize[] = {6,9,3,7,5,1,0,8,2,4};
//			int testSize[] = {8,6,2,0,3,4,2,3,4,1,1,7,0,9,5};
//			int testSize[] = {0,9,3,8,4,4,6,0,9,5,5,8,2,2,3,1,7,1,7,6};

			int size = 20;
			int testSize[] = new int[size];
			java.util.Random rand = new java.util.Random();
			for(int i=0; i<size; i++)
				testSize[i] = rand.nextInt(10);
			
			for(int i=0; i<testSize.length; i++)
			{
				FileInputStream fin = new FileInputStream(m_pathToFile);
				ByteArrayOutputStream binary = new ByteArrayOutputStream();

				byte[] buff = new byte[102400];
				int len = 0;
				long counter = 0;
				while( (len = fin.read(buff)) > 0)
				{
					counter += len;
					binary.write(buff, 0, len);
					//System.out.println("Current:\t" + counter/1024);
				}
				fin.close();		
				
				Socket s = new Socket(m_URL, Integer.parseInt(m_port));
				
				DataOutputStream stream = new DataOutputStream(s.getOutputStream());
				
				stream.write("JobType:".getBytes());
				stream.write(mapR.getTypeName().getBytes());
				stream.write("\n".getBytes());
								
				stream.write("InputFolder:".getBytes());
				stream.write(m_paramInput.getBytes());
				stream.write(Integer.toString(testSize[i]).getBytes());
				stream.write("\n".getBytes());

				stream.write("OutputFolder:".getBytes());
				stream.write(m_paramOutput.getBytes());
				stream.write(Integer.toString(i).getBytes());
				stream.write("\n".getBytes());
				
				stream.write("Command:".getBytes());
				stream.write(m_paramInput.getBytes());
				stream.write(Integer.toString(testSize[i]).getBytes());
				stream.write(" ".getBytes());
				stream.write(m_paramOutput.getBytes());
				stream.write(Integer.toString(i).getBytes());
				stream.write("\n".getBytes());
				
				stream.write("BinaryDataLength:".getBytes());
				stream.write(Integer.toString(binary.size()).getBytes());
				stream.write("\n".getBytes());
				
				stream.write(binary.toByteArray());				
				
				s.close();
				
				//System.out.println("" + i + "\tdone");
				
//				if(i>6)
					Thread.sleep(5000);
			}
			
			
		} catch(Exception e)
		{
			e.printStackTrace();
		}

	}

}
