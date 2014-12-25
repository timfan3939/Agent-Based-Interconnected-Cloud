package tw.idv.ctfan.cloud.middleware.test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.net.Socket;

import tw.idv.ctfan.cloud.middleware.Cluster.JobType;
import tw.idv.ctfan.cloud.middleware.Java.JavaJobType;

public class TestJava {
	public static void main(String[] args) {
		try {
		
			//String m_URL = "10.133.200.1";
			//String m_URL = "120.126.145.117";
			//String m_URL = "120.126.145.114";
//			String m_URL = "10.133.70.64";
//			String m_URL = "120.126.145.3";
			String m_URL = "120.126.145.102";
			String m_port = "50031";
//			String m_pathToFile = "C:\\ctfan\\Calculus.jar";
			String m_pathToFile = "C:\\ctfan\\ComputePi.jar";
			

			int size = 10;
			int testSize[] = new int[size];
//			java.util.Random rand = new java.util.Random();
//			for(int i=0; i<size; i++)
//				testSize[i] = rand.nextInt(100000)+50000;
			for(int i=0; i<size; i++)
				testSize[i] = 59000; // Exact 1 minute job 
			JobType java = new JavaJobType();
			
//			int testSize[] = {50000, 53333, 56666, 60000, 63333, 66666, 70000, 73333, 76666, 80000,
//					70906, 75966, 71628, 78253, 74603, 67247, 50262, 60559, 74905, 61142,
//					72939, 70590, 54744, 75390, 58276, 74974, 66862, 76812, 57568, 50488,
//					51946, 54565, 79418, 71282, 73902, 76384, 55898, 69447, 72685, 51448,
//					51591, 51939, 56639, 59982, 66154, 67324, 58738, 53562, 79166, 62096,
//					57051, 52859, 60163, 77976, 72650, 52291, 67649, 70822, 72379, 68072,
//					54266, 58160, 63289, 62024, 65785, 71000, 58333, 51260, 69846, 54983,
//					70420, 59943, 63083, 58198, 72782, 76172, 72273, 57817, 73411, 75226,
//					72755, 53464, 63440, 52613, 57508, 72763, 69263, 68689, 60845, 56722,
//					65525, 66429, 78894, 69278, 78996, 75578, 68534, 64896, 58321, 55159,
//					68878, 62267, 63504, 65483, 70463, 73592, 73627, 57757, 70068, 63607,
//					71058, 77075, 77162, 52924, 67516, 69750, 57027, 50382, 72603, 54350,
//					67594, 67111, 51662, 75615, 54850, 66920, 61039, 73504, 52983, 55517,
//					77533, 67315, 63557, 63640, 63941, 66422, 77207, 60717, 52633, 54966,
//					65144, 79369, 56066, 76506, 56438, 57232, 59666, 56371, 52676, 77762,
//					52850, 62062, 69550, 60953, 74314, 70194, 79783, 72273, 73999, 73597,
//					67423, 76036, 66358, 58774, 71457, 62113, 53687, 77358, 51544, 65720,
//					67437, 64361, 77793, 57349, 71193, 77570, 65259, 52935, 57350, 60560,
//					59259, 58172, 66520, 53283, 50924, 72059, 57422, 78778, 64132, 60749,
//					62182, 57333, 75090, 66904, 68015, 75442, 57780, 50474, 72730, 61719,
//			};
			
			for(int i=0; i<testSize.length; i++)
			{
				System.out.println("Sending " + i );
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
				stream.write(java.getTypeName().getBytes());
				stream.write("\n".getBytes());
												
				stream.write("Command:".getBytes());
				stream.write(Integer.toString(testSize[i]).getBytes());
				stream.write("\n".getBytes());
				
				stream.write("Name:".getBytes());
//				stream.write("Calculus".getBytes());
				stream.write("ComputePi".getBytes());
				stream.write("\n".getBytes());
				
				//if(rand.nextInt(2)==0)
//				if(i%2==0 && i>5) {
//					stream.write("Deadline:".getBytes());
//					stream.write(Integer.toString(12000).getBytes());
//					stream.write("\n".getBytes());
//				}
//				else if(i>20) {
//					stream.write("Deadline:".getBytes());
//					stream.write(Integer.toString(120).getBytes());
//					stream.write("\n".getBytes());
//				}
//				else {
//					stream.write("Deadline:".getBytes());
//					stream.write(Integer.toString(100000).getBytes());
//					stream.write("\n".getBytes());
//				}
				// The deadline seems to be in second
				// The overhead of the system is around 20 second
				// set the overhead as 25 sec
				stream.write("Deadline:".getBytes());
				stream.write(Integer.toString(75).getBytes());
				stream.write("\n".getBytes());
				
				stream.write("BinaryDataLength:".getBytes());
				stream.write(Integer.toString(binary.size()).getBytes());
				stream.write("\n".getBytes());
				
				stream.write(binary.toByteArray());				
				
				s.close();
				
				//System.out.println("" + i + "\tdone");
				
//				if(i>6)
				for(int t=0; t<1; t++){
					System.out.print("" + (t) + "0 " );
					Thread.sleep(10000);
				}
				System.out.println();
			}
			
			
		} catch(Exception e)
		{
			e.printStackTrace();
		}
	}

}
