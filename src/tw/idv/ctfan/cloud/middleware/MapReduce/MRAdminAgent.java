package tw.idv.ctfan.cloud.middleware.MapReduce;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;

import tw.idv.ctfan.cloud.middleware.Cluster.AdminAgent;

public class MRAdminAgent extends AdminAgent {
	private static final long serialVersionUID = -6536487006364985284L;
	
	// hadoop hdfs and job tracker interfaces
	private JobClient m_jobTracker;
//	private FileSystem m_nameNode;

	public MRAdminAgent() {
		super(new MRJobType());
		
		try {
			m_jobTracker = new JobClient(new InetSocketAddress("localhost", 9001), new Configuration());
		} catch (Exception e) {
			System.err.println("Job Tracker Problem");
			e.printStackTrace();
		}
		
//		try {
//			m_nameNode = FileSystem.get(new URI("hdfs://localhost:9000"), new Configuration());
//		} catch (Exception e) {
//			System.err.println("Name Node Problem");
//			e.printStackTrace();
//		} 
	}

	@Override
	public String GetJobAgentClassName() {
		return tw.idv.ctfan.cloud.middleware.MapReduce.MRJobAgent.class.getName();
	}

	@Override
	public void OnDecodeNewJob(JobListNode jn, String head, String tail) {
		jn.attributes.put(head, tail);
		System.out.println(jn.name + " " + head + ":" + tail);
	}

	@Override
	protected String OnEncodeJobInfo(JobListNode jn) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected String OnEncodeClusterLoadInfo() {
		return (super.m_jobList.size()>0?"Busy":"Free");
	}

	@Override
	public String OnEncodeNewJobAgent(JobListNode jn) {
		if(jn.attributes.containsKey("Command"))
			return jn.attributes.get("Command");
		return "";
	}

	@Override
	public void OnTerminateCluster() {
		System.out.println("I'm going to be terminated.");
		// TODO Auto-generated method stub

	}

	@Override
	public boolean InitilizeCluster() {
		if(m_jobTracker!=null) {
			try {
				return m_jobTracker.getClusterStatus().getJobTrackerState() == org.apache.hadoop.mapred.JobTracker.State.RUNNING;
			} catch (IOException e) {
				System.err.println("Get cluster status error");
				e.printStackTrace();
			}
		}
		return false;
	}

}
