package tw.idv.ctfan.cloud.middleware.Cluster;

import java.util.ArrayList;

import tw.idv.ctfan.cloud.middleware.Cluster.AdminAgent;
import tw.idv.ctfan.cloud.middleware.policy.data.JobNode;

public abstract class JobType {
	
	private ArrayList<AdminAgent> m_clusterList;	
	private String m_typeName;
	
	protected String getTypeName() {
		return m_typeName;
	}

	public JobType(String name){
		m_typeName = name;
	}
	
	ArrayList<AdminAgent> getClusterList() {
		return m_clusterList;
	}
	
	public abstract boolean varifyJob(JobNode jn);
	public abstract int DecodeLoadInfo();
	//TODO: Add proper classes
}
