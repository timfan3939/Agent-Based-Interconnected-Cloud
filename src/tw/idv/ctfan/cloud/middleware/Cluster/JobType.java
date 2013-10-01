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
	
	/**
	 * Decode the cluster's load information.
	 * This function is accompany to the {@link AdminAgent#OnEncodeClusterLoadInfo}
	 * @param line
	 * @return
	 */
	public abstract int DecodeClusterLoadInfo(String line);
	
	/**
	 * Decode and update the job's information
	 * This function is accompany to {@link AdminAgent#OnEncodeJobInfo}
	 * @param line
	 * @param jn
	 */
	public abstract void UpdateJobNodeInfo(String line, JobNode jn);
	
	/**
	 * Help verify the job.  check if the job meets the standard of the type
	 * @param jn
	 * @return True if the job is OK, false otherwise
	 */
	public abstract boolean varifyJob(JobNode jn);
	
	/**
	 * You can add additional information about the job.
	 * For example, the size of the job.
	 * @param jn
	 */
	public abstract void SetJobInfo(JobNode jn);
	
	/**
	 * You can use this function to extract the container ID where the agent lives
	 * @param msg
	 * @return
	 */
	public abstract ContainerID ExtractContainer(ACLMessage msg);
	
	
	/**
	 * 
	 * <b>Removed because JobNode can encapsulate a job itself</b>
	 * 
	 * Before the new-job-request message is sent to the {@link AdminAgent},
	 * you should define the information that is used to execute a job
	 * Note that both UID and binaryFile should not handle here.
	 * This function is accompany to {@link AdminAgent#OnEncodeNewJob} 
	 * @param jn
	 * @return
	 */
//	public abstract String OnDispatchJobMsg(JobNode jn);
	
	/**
	 * This function return the extension type of this kind of job.
	 * Please return the full extension.  For example, ".jar"
	 */
	public abstract String GetExtension();
}
