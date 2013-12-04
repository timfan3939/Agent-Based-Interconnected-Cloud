package tw.idv.ctfan.cloud.middleware.test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.net.Socket;

import tw.idv.ctfan.cloud.middleware.Cluster.JobType;
import tw.idv.ctfan.cloud.middleware.Java.JavaJobType;
import tw.idv.ctfan.cloud.middleware.MPI.MPIJobType;
import tw.idv.ctfan.cloud.middleware.MapReduce.MRJobType;

public class TestAllJob {

	/**
	 * @param args
	 */

	public static void main(String[] args) {
		try {
		
			String m_URL = "120.126.145.102";
			String m_port = "50031";
			
			
			// Job Definitions
			String[] jobFiles = {
					"C:\\ctfan\\ComputePi.jar",
					"C:\\ctfan\\Calculus.jar",
					"C:\\ctfan\\AssoRule2013.jar",
					"C:\\ctfan\\hello",
			};
			
			JobType java = new JavaJobType();
			JobType MPI = new MPIJobType();
			JobType hadoop = new MRJobType();
			
			JobType[] jobTypes = {
					java,
					java,
					hadoop,
					MPI,
			};
			
			int[] randomSize = {
					10000/*0*/,
					10000/*0*/,
					10,
					99,					
			};
			
			int[] randomMinSize = {
					50000,
					50000,
					0,
					2
			};
			
			int submitSize = 30;
			java.util.Random rand = new java.util.Random();	

			
			for(int i=0; i<submitSize; i++)
			{
				int job = rand.nextInt(jobFiles.length);
				
				
				FileInputStream fin = new FileInputStream(jobFiles[job]);
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
				stream.write(jobTypes[job].getTypeName().getBytes());
				stream.write("\n".getBytes());
				
				stream.write("Name:".getBytes());
				stream.write("Unknown".getBytes());
				stream.write("\n".getBytes());
				
				if(jobTypes[job] == java) {					
					stream.write("Command:".getBytes());
					stream.write(Integer.toString( randomMinSize[job] + rand.nextInt(randomSize[job]) ).getBytes());
					stream.write("\n".getBytes());
				}
				else if(jobTypes[job] == MPI) {		
					stream.write("Thread:".getBytes());
					stream.write(Integer.toString( randomMinSize[job] + rand.nextInt(randomSize[job]) ).getBytes());
					stream.write("\n".getBytes());					
				}
				else if(jobTypes[job] == hadoop) {
					int input = randomMinSize[job] + rand.nextInt(randomSize[job]);
					String m_paramInput = "/usr/ctfan/input";
					String m_paramOutput ="/usr/ctfan/output/output";
					
					stream.write("InputFolder:".getBytes());
					stream.write(m_paramInput.getBytes());
					stream.write(Integer.toString(input).getBytes());
					stream.write("\n".getBytes());

					stream.write("OutputFolder:".getBytes());
					stream.write(m_paramOutput.getBytes());
					stream.write(Integer.toString(i).getBytes());
					stream.write("\n".getBytes());
					
					stream.write("Command:".getBytes());
					stream.write(m_paramInput.getBytes());
					stream.write(Integer.toString(input).getBytes());
					stream.write(" ".getBytes());
					stream.write(m_paramOutput.getBytes());
					stream.write(Integer.toString(i).getBytes());
					stream.write("\n".getBytes());					
				}
				
				// TODO: this section is still no implemented yet
//				stream.write("Deadline:".getBytes());
//				//if(rand.nextInt(2)==0)
//				if(i%2==0)
//					stream.write(Integer.toString(120000).getBytes());
//				else
//					stream.write(Integer.toString(100000).getBytes());
//				stream.write("\n".getBytes());
				
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
