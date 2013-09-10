package tw.idv.ctfan.cloud.middleware.policy;

import java.util.ArrayList;

import tw.idv.ctfan.cloud.middleware.policy.Decision.DispatchDecision;
import tw.idv.ctfan.cloud.middleware.policy.Decision.MigrationDecision;
import tw.idv.ctfan.cloud.middleware.policy.Decision.VMManagementDecision;
import tw.idv.ctfan.cloud.middleware.policy.data.ClusterNode;
import tw.idv.ctfan.cloud.middleware.policy.data.JobNode;
import tw.idv.ctfan.cloud.middleware.policy.data.VMController;

public abstract class Policy {
	
	ArrayList<VMController> m_VMMasterList;
	ArrayList<ClusterNode> m_runningClusterList;
	ArrayList<ClusterNode> m_availableClusterList;
	ArrayList<JobNode> m_runningJobList;
	ArrayList<JobNode> m_finishJobList;
	ArrayList<JobNode> m_waitingJobList;
	ArrayList<VMController> m_vmControllerList;
	
	
	protected static Policy onlyInstance;
	
	protected Policy()
	{
		m_VMMasterList = new ArrayList<VMController>();
		m_runningClusterList = new ArrayList<ClusterNode>();
		m_availableClusterList = new ArrayList<ClusterNode>();
		m_runningJobList = new ArrayList<JobNode>();
		m_finishJobList = new ArrayList<JobNode>();
		m_waitingJobList = new ArrayList<JobNode>();
		m_vmControllerList = new ArrayList<VMController>();
	}
	
	public abstract DispatchDecision GetNewJobDestination();
	
	public abstract MigrationDecision GetMigrationDecision();
	
	public abstract VMManagementDecision GetVMManagementDecision();
	
	public abstract void OnNewClusterArrives(ClusterNode cn);
	
	public abstract void OnOldClusterLeaves(ClusterNode cn);
	
	public abstract ArrayList<VMController> InitVMMasterList();
	
	public abstract void InitClusterList();
		

	public ArrayList<VMController> GetVMMaster() {
		return m_VMMasterList;
	}
	
	public ArrayList<ClusterNode> GetRunningCluster()
	{
		return m_runningClusterList;
	}
	
	public ArrayList<ClusterNode> GetAvailableCluster() {
		return m_availableClusterList;
	}
	
	public ArrayList<JobNode> GetRunningJob()
	{
		return m_runningJobList;
	}
	
	public ArrayList<JobNode> GetWaitingJob()
	{
		return m_waitingJobList;
	}
	
	public ArrayList<JobNode> GetFinishJob()
	{
		return m_finishJobList;
	}
	
	public static Policy GetPolicy()
	{
		return onlyInstance;
	}

}
