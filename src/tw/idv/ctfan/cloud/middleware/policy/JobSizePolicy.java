package tw.idv.ctfan.cloud.middleware.policy;

import tw.idv.ctfan.cloud.middleware.policy.Decision.DispatchDecision;
import tw.idv.ctfan.cloud.middleware.policy.Decision.MigrationDecision;
import tw.idv.ctfan.cloud.middleware.policy.Decision.VMManagementDecision;
import tw.idv.ctfan.cloud.middleware.policy.data.ClusterNode;
import tw.idv.ctfan.cloud.middleware.policy.data.HadoopJobNode;
import tw.idv.ctfan.cloud.middleware.policy.data.JobNodeBase;

public class JobSizePolicy extends Policy {	
	
	private JobSizePolicy() {
		super();		
		onlyInstance = this;
	}

	@Override
	public MigrationDecision GetDecision() {	
		if(m_runningClusterList.size()<=1)
			return null;

		int clusterSize = m_runningClusterList.size();		
		int size[] = new int[clusterSize];
		int jobs[] = new int[clusterSize];
		for(int i=0; i<clusterSize; i++)
			jobs[i] = size[i] = 0;
		
		for(int i=0; i<clusterSize; i++) {
			ClusterNode cn = m_runningClusterList.get(i);			
			for(int j=0; j<m_runningJobList.size(); j++) {
				if(!m_runningJobList.get(i).jobType.matches(HadoopJobNode.JOBTYPENAME))
					continue;
				HadoopJobNode jn = (HadoopJobNode)m_runningJobList.get(j);
				if(jn.currentPosition.compare(cn)) {
					size[i] += (1-jn.mapStatus/100)*jn.mapNumber;
					jobs[i]++;
					jn.hasMigrate--;
				}
			}
		}
		
		int least = 0;
		int most  = 0;
		for(int i=0; i<clusterSize; i++){
			if(size[least]>size[i])
				least=i;
			if(size[most] <size[i])
				most = i;
		}
		System.out.println("Least: " + least + "(" + size[least] + ")\t" +
						   "Most : " + most  + "(" + size[most ] + ")");
		
		if(least==most)
			return null;
		if(jobs[most]==1)
			return null;
		else if(size[most]-size[least] < 5)
			return null;
		
		int difference = size[most]-size[least];
		
		HadoopJobNode jobToMove = null;
		ClusterNode jn = m_runningClusterList.get(most);
		for(int i=0; i<m_runningJobList.size(); i++) {
			if(m_runningJobList.get(i).jobType.matches(HadoopJobNode.JOBTYPENAME)&&m_runningJobList.get(i).currentPosition.compare(jn))
				if(m_runningJobList.get(i).jobStatus==HadoopJobNode.WAITING&&m_runningJobList.get(i).hasMigrate<=0) {
					if((difference-((HadoopJobNode)m_runningJobList.get(i)).mapNumber)>0)
						if(jobToMove==null)
							jobToMove = (HadoopJobNode)m_runningJobList.get(i);
						else if((difference-((HadoopJobNode)m_runningJobList.get(i)).mapNumber)<(difference-jobToMove.mapNumber))
							jobToMove = (HadoopJobNode)m_runningJobList.get(i);							
				}					
		}
		
		
		if(jobToMove == null) {
			System.out.println("No Jobs can be moved");
			return null;
		} else 
			jobToMove.hasMigrate = 30;
		
		System.out.println("Move\t" + jobToMove + "\tto\t" + m_runningClusterList.get(least));
		
		return new MigrationDecision(jobToMove, m_runningClusterList.get(least));
	}

	@Override
	public DispatchDecision GetNewJobDestination() {
		if(m_runningClusterList.size()<=0)
			return null;
		else if(m_runningClusterList.size()==1) {
			JobNodeBase jn = m_waitingJobList.get(0);
			m_waitingJobList.remove(jn);
			return new DispatchDecision(jn, m_runningClusterList.get(0));			
		}
		
		int clusterSize = m_runningClusterList.size();		
		int size[] = new int[clusterSize];
		for(int i=0; i<clusterSize; i++)
			size[i] = 0;
		
		for(int i=0; i<clusterSize; i++) {
			ClusterNode cn = m_runningClusterList.get(i);			
			for(int j=0; j<m_runningJobList.size(); j++) {
				if(!m_runningJobList.get(i).jobType.matches(HadoopJobNode.JOBTYPENAME))
					continue;
				HadoopJobNode jn = (HadoopJobNode)m_runningJobList.get(j);
				if(jn.currentPosition.compare(cn))
					size[i] += (1-jn.mapStatus/100)*jn.mapNumber;
			}
		}
		
		int least = 0;
		for(int i=0; i<clusterSize; i++)
			if(size[least]>size[i])
				least=i;
		
		return new DispatchDecision(m_waitingJobList.remove(0), m_runningClusterList.get(least));
	}
	
	public static Policy GetPolicy() {
		return (onlyInstance==null?new JobSizePolicy():onlyInstance);
	}

	@Override
	public VMManagementDecision GetVMManagementDecision() {
		// TODO Auto-generated method stub
		return null;
	}

}
