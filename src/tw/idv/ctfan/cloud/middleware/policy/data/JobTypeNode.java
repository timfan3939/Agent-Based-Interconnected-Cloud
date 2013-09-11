package tw.idv.ctfan.cloud.middleware.policy.data;

import java.util.ArrayList;

import tw.idv.ctfan.cloud.middleware.Cluster.AdminAgent;

public abstract class JobTypeNode {
	
	private ArrayList<AdminAgent> m_clusterList;	
	private String m_typeName;
	
	protected String getTypeName() {
		return m_typeName;
	}

	public JobTypeNode(String name){
		m_typeName = name;
	}
	
	ArrayList<AdminAgent> getClusterList() {
		return m_clusterList;
	}
	
	public abstract boolean varifyJob(JobNode jn);
	public abstract int DecodeLoadInfo();
	//TODO: Add proper classes
}
