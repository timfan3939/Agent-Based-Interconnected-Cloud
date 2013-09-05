package tw.idv.ctfan.cloud.middleware.policy;

import java.util.ArrayList;

import tw.idv.ctfan.cloud.middleware.policy.Decision.DispatchDecision;
import tw.idv.ctfan.cloud.middleware.policy.Decision.MigrationDecision;
import tw.idv.ctfan.cloud.middleware.policy.Decision.VMManagementDecision;
import tw.idv.ctfan.cloud.middleware.policy.data.ClusterNode;
import tw.idv.ctfan.cloud.middleware.policy.data.JobNodeBase;
import tw.idv.ctfan.cloud.middleware.policy.data.VMMasterNode;

public abstract class Policy {
	
	ArrayList<VMMasterNode> m_VMMasterList;
	ArrayList<ClusterNode> m_runningClusterList;
	ArrayList<ClusterNode> m_availableClusterList;
	ArrayList<JobNodeBase> m_runningJobList;
	ArrayList<JobNodeBase> m_finishJobList;
	ArrayList<JobNodeBase> m_waitingJobList;
	
	protected static Policy onlyInstance;
	
	protected Policy()
	{
		m_VMMasterList = new ArrayList<VMMasterNode>();
		m_runningClusterList = new ArrayList<ClusterNode>();
		m_availableClusterList = new ArrayList<ClusterNode>();
		m_runningJobList = new ArrayList<JobNodeBase>();
		m_finishJobList = new ArrayList<JobNodeBase>();
		m_waitingJobList = new ArrayList<JobNodeBase>();
	}
	
	public abstract DispatchDecision GetNewJobDestination();
	
	public abstract MigrationDecision GetMigrationDecision();
	
	public abstract VMManagementDecision GetVMManagementDecision();
	
	public abstract void OnNewClusterArrives(ClusterNode cn);
	
	public abstract void OnOldClusterLeaves(ClusterNode cn);
		

	public ArrayList<VMMasterNode> GetVMMaster() {
		return m_VMMasterList;
	}
	
	public ArrayList<ClusterNode> GetRunningCluster()
	{
		return m_runningClusterList;
	}
	
	public ArrayList<ClusterNode> GetAvailableCluster() {
		return m_availableClusterList;
	}
	
	public ArrayList<JobNodeBase> GetRunningJob()
	{
		return m_runningJobList;
	}
	
	public ArrayList<JobNodeBase> GetWaitingJob()
	{
		return m_waitingJobList;
	}
	
	public ArrayList<JobNodeBase> GetFinishJob()
	{
		return m_finishJobList;
	}
	
	public static Policy GetPolicy()
	{
		return onlyInstance;
	}

}
