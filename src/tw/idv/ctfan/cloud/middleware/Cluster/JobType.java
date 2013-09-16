package tw.idv.ctfan.cloud.middleware.Cluster;

import jade.core.ContainerID;
import jade.lang.acl.ACLMessage;

import java.util.ArrayList;

import tw.idv.ctfan.cloud.middleware.Cluster.AdminAgent;
import tw.idv.ctfan.cloud.middleware.policy.data.JobNode;

public abstract class JobType {
	
	private ArrayList<AdminAgent> m_clusterList;	
	private String m_typeName;
	
	public String getTypeName() {
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
	public abstract void SetJobInfo(JobNode jn);
	public abstract ContainerID ExtractContainer(ACLMessage msg);
	public abstract String EncodeParameter(JobNode jn);
	public abstract byte[] EncodeJobNode(JobNode jn, String fileDirectory);
	//TODO: Add proper classes

	public abstract String GetExtension();
}
