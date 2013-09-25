package tw.idv.ctfan.cloud.middleware.MapReduce;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;

public class MRClusterStart {
	
	private static JobClient jobClient;
	private static Process p;

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
			
			Thread.sleep(1000);
			
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
			
			while(true) {
				try {
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
