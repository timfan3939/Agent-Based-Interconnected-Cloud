package tw.idv.ctfan.cloud.middleware.policy;

import java.util.ArrayList;
import java.util.Collections;

import tw.idv.ctfan.cloud.middleware.policy.Decision.DispatchDecision;
import tw.idv.ctfan.cloud.middleware.policy.Decision.MigrationDecision;
import tw.idv.ctfan.cloud.middleware.policy.Decision.VMManagementDecision;
import tw.idv.ctfan.cloud.middleware.policy.data.ClusterNode;
import tw.idv.ctfan.cloud.middleware.policy.data.HadoopJobNode;
import tw.idv.ctfan.cloud.middleware.policy.data.JobNodeBase;


public class JobCountPolicy extends Policy {	
	
	
	private JobCountPolicy()
	{
		super();
		
		onlyInstance = this;
	}
	
	public DispatchDecision GetNewJobDestination()
	{
		if(m_runningClusterList.size()<=0)
			return null;
		else {
			JobNodeBase jn = m_waitingJobList.get(0);
			m_waitingJobList.remove(jn);
			return new DispatchDecision(jn,m_runningClusterList.get(0));
		}
//		else if(m_runningClusterList.size()==1)
//			return m_runningClusterList.get(0);
//		
//		int jobs[] = new int[m_runningClusterList.size()];
//		for(int i=0; i<m_runningClusterList.size(); i++)
//			jobs[i] = 0;
//		
//		for(int i=0; i<m_runningClusterList.size(); i++)
//		{
//			ClusterNode cn = m_runningClusterList.get(i);
//			
//			for(int j=0; j<m_runningJobList.size(); j++)
//			{
//				JobNode jn = m_runningJobList.get(j);
//				if(jn.currentPosition.compare(cn))
//					jobs[i]++;
//			}
//		}
//		
//		int least = 0;
//		for(int i=0; i<jobs.length; i++)
//		{
//			if(jobs[least]>jobs[i])
//				least = i;
//		}
//		
//		return m_runningClusterList.get(least);
	}
	
	public MigrationDecision GetMigrationDecision()
	{
		if(m_runningClusterList.size()<=1)
			return null;
		

		int jobs[] = new int[m_runningClusterList.size()];
		for(int i=0; i<m_runningClusterList.size(); i++)
			jobs[i] = 0;
		
		for(int i=0; i<m_runningClusterList.size(); i++)
		{
			ClusterNode cn = m_runningClusterList.get(i);
			
			for(int j=0; j<m_runningJobList.size(); j++)
			{
				if(!m_runningJobList.get(i).jobType.matches(HadoopJobNode.JOBTYPENAME))
					continue;
				HadoopJobNode jn = (HadoopJobNode)m_runningJobList.get(j);
				if(jn.currentPosition.compare(cn))
					jobs[i]++;
			}
		}
		
		for(int i=0; i<m_runningJobList.size(); i++)
		{
			if(m_runningJobList.get(i).hasMigrate>0)
				m_runningJobList.get(i).hasMigrate--;
		}
		
		// get most and least job node
		int least = 0;
		int most  = 0;
		for(int i=0; i<jobs.length; i++)
		{
			if(jobs[least]>jobs[i])
				least = i;
			if(jobs[most]<jobs[i])
				most = i;
		}
		System.out.println("Least: " + least + "(" + jobs[least] + ")\t" +
						   "Most : " + most  + "(" + jobs[most ] + ")");
		
		if(least==most)
			return null;
		else if(jobs[most] - jobs[least] <= 1 )
			return null;
		
		Collections.sort(m_runningJobList);
		
		HadoopJobNode jobToMove = null;
		for(int i=m_runningJobList.size()-1; i>=0; i--)
		{
			if(m_runningJobList.get(i).jobType.matches(HadoopJobNode.JOBTYPENAME)&&m_runningJobList.get(i).currentPosition.compare(m_runningClusterList.get(most)))
			{
				if(m_runningJobList.get(i).jobStatus == 0 && m_runningJobList.get(i).hasMigrate <=0)
				{
					jobToMove = (HadoopJobNode)m_runningJobList.get(i);
					jobToMove.hasMigrate = 30;
					break;
				}
			}
		}
		
		if(jobToMove == null)
		{
			System.out.println("No Jobs can be moved");
			return null;
		}
		
		System.out.println("Move\t" + jobToMove + "\tto\t" + m_runningClusterList.get(least));
		
		return new MigrationDecision(jobToMove, m_runningClusterList.get(least));
	}
	
	public ArrayList<ClusterNode> GetCluster()
	{
		return m_runningClusterList;
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
		if(onlyInstance != null)
			return onlyInstance;
		else
			return new JobCountPolicy();
	}

	@Override
	public VMManagementDecision GetVMManagementDecision() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void OnNewClusterArrives(ClusterNode cn) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void OnOldClusterLeaves(ClusterNode cn) {
		// TODO Auto-generated method stub
		
	}

}
