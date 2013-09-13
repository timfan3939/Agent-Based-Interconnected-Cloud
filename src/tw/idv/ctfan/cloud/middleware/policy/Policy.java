package tw.idv.ctfan.cloud.middleware.policy;

import java.util.ArrayList;

import tw.idv.ctfan.cloud.middleware.Cluster.JobType;
import tw.idv.ctfan.cloud.middleware.policy.Decision.DispatchDecision;
import tw.idv.ctfan.cloud.middleware.policy.Decision.MigrationDecision;
import tw.idv.ctfan.cloud.middleware.policy.Decision.VMManagementDecision;
import tw.idv.ctfan.cloud.middleware.policy.data.ClusterNode;
import tw.idv.ctfan.cloud.middleware.policy.data.JobNode;
import tw.idv.ctfan.cloud.middleware.policy.data.VMController;
import tw.idv.ctfan.cloud.middleware.policy.data.VirtualMachineNode;

public abstract class Policy {
	
	// Cluster related
	ArrayList<VMController> m_vmControllerList;
	ArrayList<ClusterNode> m_runningClusterList;
	ArrayList<ClusterNode> m_availableClusterList;
	ArrayList<VirtualMachineNode> m_vmList;
	ArrayList<JobType> m_jobTypeList;
	
	// Job Related
	ArrayList<JobNode> m_runningJobList;
	ArrayList<JobNode> m_finishJobList;
	ArrayList<JobNode> m_waitingJobList;
	
	
	protected static Policy onlyInstance;
	
	protected Policy()
	{
		m_vmControllerList = new ArrayList<VMController>();
		m_runningClusterList = new ArrayList<ClusterNode>();
		m_availableClusterList = new ArrayList<ClusterNode>();
		m_runningJobList = new ArrayList<JobNode>();
		m_finishJobList = new ArrayList<JobNode>();
		m_waitingJobList = new ArrayList<JobNode>();
		m_vmList = new ArrayList<VirtualMachineNode>();
		m_jobTypeList = new ArrayList<JobType>();
	}
	
	public abstract DispatchDecision GetNewJobDestination();
	
	public abstract MigrationDecision GetMigrationDecision();
	
	public abstract VMManagementDecision GetVMManagementDecision();
	
	public abstract void OnNewClusterArrives(ClusterNode cn);
	
	public abstract void OnOldClusterLeaves(ClusterNode cn);
	
	public abstract ArrayList<VMController> InitVMMasterList();
	
	public abstract void InitClusterList();
		

	public ArrayList<VMController> GetVMControllerList() {
		return m_vmControllerList;
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
	
	public ArrayList<VirtualMachineNode> GetVMList() {
		return m_vmList;
	}
	
	public ArrayList<JobType> GetJobTypeList() {
		return m_jobTypeList;
	}
	
	public static Policy GetPolicy()
	{
		return onlyInstance;
	}

}
