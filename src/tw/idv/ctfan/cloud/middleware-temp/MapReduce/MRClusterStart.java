package tw.idv.ctfan.cloud.Middleware.MapReduce;

import java.io.BufferedInputStream;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;

public class MRClusterStart {
	
	private static JobClient jobClient;
	private static Process p;
	
	static BufferedInputStream m_buffErrorInput;
	static BufferedInputStream m_buffNormalInput;
	static StringBuffer m_output = new StringBuffer();
	static byte[] buff2k = new byte[0x400];
	static int buffLen;
	
	// TODO: output process message

	public static void main(String[] args) { 
		StartMapReduce();
		CheckSafeMode();
		StartMiddleWare();
		System.out.println("Complete");
	}
	
	private static void StartMiddleWare() {
		try {
			Runtime rt = Runtime.getRuntime();
			String command = "/home/hadoop/ctfan/jade/java70_64.sh";
			p = rt.exec(command);
			m_buffErrorInput = new BufferedInputStream(p.getErrorStream());
			m_buffNormalInput= new BufferedInputStream(p.getInputStream());
			
			
			Thread.sleep(1000);
			
			while(true) {
				try {
					if( (buffLen = m_buffErrorInput.read(buff2k)) >0 ) {
						m_output.append("\n-Error-----------------------\n");
						m_output.append(new String(buff2k), 0, buffLen);
						m_output.append("\n-----------------------------\n");
						
						System.out.println(m_output.toString());
						m_output.delete(0, m_output.length());
					}
					if( (buffLen = m_buffNormalInput.read(buff2k)) >0 ) {
						m_output.append("\n-Normal----------------------\n");
						m_output.append(new String(buff2k), 0, buffLen);
						m_output.append("\n-----------------------------\n");
						
						System.out.println(m_output.toString());
						m_output.delete(0, m_output.length());
					}
					
					p.exitValue();
					break;
				} catch(IllegalThreadStateException e) {
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}	
	
	public static void CheckSafeMode() {
		try {
			while(true) {
				try {
					InetSocketAddress local = new InetSocketAddress("localhost", 9001);
					Configuration conf = new Configuration();
					jobClient = new JobClient(local, conf);
					break;
				} catch (Exception e) {
					System.out.println("jobClient not ready yet");
					e.printStackTrace();
					Thread.sleep(5000);
				}
			}
			
			while(true) {
				try {
					if (jobClient.getClusterStatus().getJobTrackerState() == org.apache.hadoop.mapred.JobTracker.State.INITIALIZING) {
						System.out.println("It is initializing right now");
						Thread.sleep(5000);
						continue;
					}
					break;
				}
				catch(Exception e) {
					System.out.println("Checking problem");
					Thread.sleep(5000);
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public static void StartMapReduce() {
		try {
			Runtime rt = Runtime.getRuntime();
			Process p;
			String command = "/root/hadoop/bin/start-all.sh";
			p = rt.exec(command);
			m_buffErrorInput = new BufferedInputStream(p.getErrorStream());
			m_buffNormalInput= new BufferedInputStream(p.getInputStream());
			
			while(true) {
				try {
					if( (buffLen = m_buffErrorInput.read(buff2k)) >0 ) {
						m_output.append("\n-Error-----------------------\n");
						m_output.append(new String(buff2k), 0, buffLen);
						m_output.append("\n-----------------------------\n");
						System.out.println(m_output.toString());
						m_output.delete(0, m_output.length());
					}

					if( (buffLen = m_buffNormalInput.read(buff2k)) >0 ) {
						m_output.append("\n-Normal----------------------\n");
						m_output.append(new String(buff2k), 0, buffLen);
						m_output.append("\n-----------------------------\n");
						
						System.out.println(m_output.toString());
						m_output.delete(0, m_output.length());
					}
					
					p.exitValue();
					break;
				} catch(IllegalThreadStateException e) {
					System.out.println("Starting Cluster is not finished yet");
					Thread.sleep(5000);
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
