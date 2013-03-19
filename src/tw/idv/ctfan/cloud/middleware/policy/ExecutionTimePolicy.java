package tw.idv.ctfan.cloud.middleware.policy;

import java.util.ArrayList;

import tw.idv.ctfan.RoughSet.*;
import tw.idv.ctfan.cloud.middleware.policy.Decision.DispatchDecision;
import tw.idv.ctfan.cloud.middleware.policy.Decision.MigrationDecision;
import tw.idv.ctfan.cloud.middleware.policy.Decision.VMManagementDecision;
import tw.idv.ctfan.cloud.middleware.policy.data.ClusterNode;
import tw.idv.ctfan.cloud.middleware.policy.data.HadoopJobNode;
import tw.idv.ctfan.cloud.middleware.policy.data.JobNodeBase;

public class ExecutionTimePolicy extends Policy {
	
	/**
	 * Current element's attributes:
	 *  
	 *  				Name				Discrete/Continuous		From
	 *  ----------------------------------------------------------------------------------
	 *  Condition Attributes:
	 *  				ALGORITHM			Discrete				JobNode.algorithm
	 *  				MEMORY				Continuous				JobNode.currentPosition.memory
	 *  				CPU SPEED			Continuous				JobNode.currentPosition.CPUSpeed
	 *  				CORE				Continuous				JobNode.currentPosition.core
	 *  				Job Size			Continuous				JobNode.jobSize
	 *  				Map Total			Continuous				JobNode.mapNumber
	 *  				Input Size Total	Continuous				JobNode.inputFileSize
	 *  Decision Attributes:
	 *  				Execution Time		Continuous				JobNode.executionTime
	 *  				Output Size Total	Continuous				JobNode.outputFileSize
	 */
	
	
	private static final int mapNumber = 0;
	private static final int reduceNumber = 1;
	private static final int jobSize = 2;
	private static final int mapTotal = 3;
	private static final int inputSizeTotal = 4;
	
	private int numberOfConditionElement = inputSizeTotal+1;
	
//	private static final int ExecutionTime = 5;
//	private static final int outputSizeTotal = 6;
//	
//	private ArrayList<Integer> attrMapNumber;
//	private ArrayList<Integer> attrReduceNumber;;
//	private ArrayList<Long> attrJobSize;
//	private ArrayList<Integer> attrMapTotal;
//	private ArrayList<Long> attrInputSizeTotal;
//	private ArrayList<Long> attrExecutionTime;
//	private ArrayList<Long> attrOutputSizeTotal;
	
	/// Maxima number
	private int maxMapNumber;
	private int maxReduceNumber;
	private long maxJobSize;
	private int maxMapTotal;
	private long maxInputSizeTotal;
	private long maxExecutionTime;
	private long maxOutputSizeTotal;
	
	/// minimun number
	private int minMapNumber;
	private int minReduceNumber;
	private long minJobSize;
	private int minMapTotal;
	private long minInputSizeTotal;
	private long minExecutionTime;
	private long minOutputSizeTotal;

	private int divMapNumber;
	private int divReduceNumber;
	private long divJobSize;
	private int divMapTotal;
	private long divInputSizeTotal;
	private long divExecutionTime;
//	private long divOutputSizeTotal;
	
	long[] decExecutionTime;
	
	private final int recalculateRoughSet = 5;	
	private final int maxSubsetSize = 5;	
	private int numberOfSubset;	
	
	RoughSet set;

	private ExecutionTimePolicy() {
		super();
		
		ArrayList<JobNodeBase> list = this.m_finishJobList;

		
		list.add(new HadoopJobNode(4,4,11455464,20,408000,73784,0));
		list.add(new HadoopJobNode(4,4,11455464,90,1836000,192731,0));
		list.add(new HadoopJobNode(4,4,11455464,70,1428000,150373,0));
		list.add(new HadoopJobNode(4,4,11455464,80,1632000,164062,0));
		list.add(new HadoopJobNode(4,4,11455464,100,2040000,207616,0));
		list.add(new HadoopJobNode(4,4,11455464,100,2040000,243471,0));
		list.add(new HadoopJobNode(4,4,11455464,30,612000,90401,0));
		list.add(new HadoopJobNode(4,4,11455464,90,1836000,189321,0));
		list.add(new HadoopJobNode(4,4,11455464,10,204000,36066,0));
		list.add(new HadoopJobNode(4,4,11455464,20,408000,70687,0));
		list.add(new HadoopJobNode(4,4,11455464,10,204000,48162,0));
		list.add(new HadoopJobNode(4,4,11455464,60,1224000,129289,0));
		list.add(new HadoopJobNode(4,4,11455464,70,1428000,181740,0));
		list.add(new HadoopJobNode(4,4,11455464,80,1632000,168411,0));
		list.add(new HadoopJobNode(4,4,11455464,60,1224000,135270,0));
		list.add(new HadoopJobNode(4,4,11455464,50,1020000,114165,0));
		list.add(new HadoopJobNode(4,4,11455464,40,816000,112655,0));
		list.add(new HadoopJobNode(4,4,11455464,40,816000,99624,0));
		list.add(new HadoopJobNode(4,4,11455464,30,612000,78616,0));
		list.add(new HadoopJobNode(4,4,11455464,50,1020000,135221,0));
		list.add(new HadoopJobNode(4,4,11455464,20,408000,74300,0));
		list.add(new HadoopJobNode(4,4,11455464,90,1836000,192797,0));
		list.add(new HadoopJobNode(4,4,11455464,70,1428000,150397,0));
		list.add(new HadoopJobNode(4,4,11455464,80,1632000,198225,0));
		list.add(new HadoopJobNode(4,4,11455464,100,2040000,213415,0));
		list.add(new HadoopJobNode(4,4,11455464,100,2040000,249469,0));
		list.add(new HadoopJobNode(4,4,11455464,90,1836000,186413,0));
		list.add(new HadoopJobNode(4,4,11455464,10,204000,48084,0));
		list.add(new HadoopJobNode(4,4,11455464,30,612000,90511,0));
		list.add(new HadoopJobNode(4,4,11455464,20,408000,57123,0));
		list.add(new HadoopJobNode(4,4,11455464,10,204000,45071,0));
		list.add(new HadoopJobNode(4,4,11455464,60,1224000,135207,0));
		list.add(new HadoopJobNode(4,4,11455464,70,1428000,180387,0));
		list.add(new HadoopJobNode(4,4,11455464,80,1632000,174415,0));
		list.add(new HadoopJobNode(4,4,11455464,50,1020000,114192,0));
		list.add(new HadoopJobNode(4,4,11455464,60,1224000,159280,0));
		list.add(new HadoopJobNode(4,4,11455464,40,816000,96211,0));
		list.add(new HadoopJobNode(4,4,11455464,40,816000,111212,0));
		list.add(new HadoopJobNode(4,4,11455464,50,1020000,114127,0));
		list.add(new HadoopJobNode(4,4,11455464,30,612000,81813,0));
		
		onlyInstance = this;
	}
	
	public void RefreshRoughSet() {
		if(this.m_finishJobList.size()<2*recalculateRoughSet)
			return;		
		ComputeMaxMinAttribute();
		
		set = new RoughSet(numberOfConditionElement);
		
		decExecutionTime = new long[numberOfSubset];
		int[] setCount = new int[numberOfSubset];
		
		for(int i=0; i<this.m_finishJobList.size(); i++) {
			if(!m_finishJobList.get(i).jobType.matches(HadoopJobNode.JOBTYPENAME))
				continue;
			
			HadoopJobNode jn = (HadoopJobNode)m_finishJobList.get(i);
			
			int[] element = new int[numberOfConditionElement];
			int decision = 0;
			
			if(divMapNumber!=0)
				element[mapNumber] =(jn.currentPosition.maxMapSlot-minMapNumber)/divMapNumber;
			if(divReduceNumber!=0)
				element[reduceNumber] = ((jn.currentPosition.maxReduceSlot-minReduceNumber)/divReduceNumber);
			if(divJobSize!=0)
				element[jobSize] = ((int) ((jn.jobSize-minJobSize)/divJobSize));
			if(divMapTotal!=0)
				element[mapTotal] = ((jn.mapNumber-minMapTotal)/divMapTotal);
			if(divInputSizeTotal!=0)
				element[inputSizeTotal] = ((int) ((jn.inputFileSize-minInputSizeTotal)/divInputSizeTotal));	
			if(divExecutionTime!=0)
				decision = ((int) ((jn.executeTime-minExecutionTime)/divExecutionTime));

			for(int j=0; j<element.length; j++)
				if(element[j]==numberOfSubset)
					element[j]--;
			
			if(decision==numberOfSubset)
				decision--;
			
//			for(int j=0; j<element.length; j++)
//				System.out.print(element[j] + " ");
//			System.out.println(decision);
			
			setCount[decision]++;
			decExecutionTime[decision] += jn.executeTime;
						
			set.AddElement(element, decision);
		}
		
		for(int i=0; i<decExecutionTime.length; i++) {
			if(setCount[i]!=0)
				decExecutionTime[i] /= setCount[i];
			//System.out.println("" + i + " " + decExecutionTime[i]);
		}
		
		set.FindCore();
		set.FindDecisionList();
		
		
		// compute reduce and decisions
	}
	
	public long GetDecision(HadoopJobNode jn)
	{		
		RefreshRoughSet();
		
		int element[] = new int[numberOfConditionElement];
		if(divMapNumber!=0)
			element[mapNumber] =(jn.currentPosition.maxMapSlot-minMapNumber)/divMapNumber;
		if(divReduceNumber!=0)
			element[reduceNumber] = ((jn.currentPosition.maxReduceSlot-minReduceNumber)/divReduceNumber);
		if(divJobSize!=0)
			element[jobSize] = ((int) ((jn.jobSize-minJobSize)/divJobSize));
		if(divMapTotal!=0)
			element[mapTotal] = ((jn.mapNumber-minMapTotal)/divMapTotal);
		if(divInputSizeTotal!=0)
			element[inputSizeTotal] = ((int) ((jn.inputFileSize-minInputSizeTotal)/divInputSizeTotal));

		for(int j=0; j<element.length; j++)
			if(element[j]==numberOfSubset)
				element[j]--;
		
		long r = 0;
		Object[] d = set.GetExactDecision(element);
		if(d==null)
			return -1;
		
		
		for(int i=0; i<d.length; i++) {
			r += decExecutionTime[(Integer) d[i]];
			//System.out.print(d[i] + " ");
		}
		//System.out.println("");
		
		r /= d.length;
		
		return r;
	}
	
	private void ComputeMaxMinAttribute() {
		for(int i=0; i<this.m_finishJobList.size(); i++) {
			if(!m_finishJobList.get(i).jobType.matches(HadoopJobNode.JOBTYPENAME)) 
				continue;
			HadoopJobNode jn = (HadoopJobNode)m_finishJobList.get(i);
			if(i==0) {
				minMapNumber = maxMapNumber = jn.currentPosition.maxMapSlot;
				minReduceNumber = maxReduceNumber = jn.currentPosition.maxReduceSlot;
				minJobSize = maxJobSize = jn.jobSize;
				minMapTotal = maxMapTotal = jn.mapNumber;
				minInputSizeTotal = maxInputSizeTotal = jn.inputFileSize;
				minExecutionTime = maxExecutionTime = jn.executeTime;
				minOutputSizeTotal = maxOutputSizeTotal = jn.outputFileSize;				
			} else {
				if(minMapNumber>jn.currentPosition.maxMapSlot)
					minMapNumber = jn.currentPosition.maxMapSlot;
				else if(maxMapNumber<jn.currentPosition.maxMapSlot)
					maxMapNumber = jn.currentPosition.maxMapSlot;

				if(minReduceNumber>jn.currentPosition.maxReduceSlot)
					minReduceNumber = jn.currentPosition.maxReduceSlot;
				else if(maxReduceNumber<jn.currentPosition.maxReduceSlot)
					maxReduceNumber = jn.currentPosition.maxReduceSlot;

				if(minJobSize>jn.jobSize)
					minJobSize = jn.jobSize;
				else if(maxJobSize<jn.jobSize)
					maxJobSize = jn.jobSize;

				if(minMapTotal>jn.mapNumber)
					minMapTotal = jn.mapNumber;
				else if(maxMapTotal<jn.mapNumber)
					maxMapTotal = jn.mapNumber;

				if(minInputSizeTotal>jn.inputFileSize)
					minInputSizeTotal = jn.inputFileSize;
				else if(maxInputSizeTotal<jn.inputFileSize)
					maxInputSizeTotal = jn.inputFileSize;

				if(minExecutionTime>jn.executeTime)
					minExecutionTime = jn.executeTime;
				else if(maxExecutionTime<jn.executeTime)
					maxExecutionTime = jn.executeTime;

				if(minOutputSizeTotal>jn.outputFileSize)
					minOutputSizeTotal = jn.outputFileSize;
				else if(maxOutputSizeTotal<jn.outputFileSize)
					maxOutputSizeTotal = jn.outputFileSize;				
			}
		}
		
		numberOfSubset = this.m_finishJobList.size()/this.maxSubsetSize;
		//System.out.println("Number of Subset " + numberOfSubset);

		divMapNumber = (maxMapNumber - minMapNumber)/numberOfSubset;
		divReduceNumber = (maxReduceNumber - minReduceNumber)/numberOfSubset;
		divJobSize = (maxJobSize - minJobSize)/numberOfSubset;
		divMapTotal = (maxMapTotal - minMapTotal)/numberOfSubset;
		divInputSizeTotal = (maxInputSizeTotal - minInputSizeTotal)/numberOfSubset;
		divExecutionTime = (maxExecutionTime - minExecutionTime)/numberOfSubset;
//		divOutputSizeTotal = (maxOutputSizeTotal - minOutputSizeTotal)/numberOfSubset;
	}

	@Override
	public MigrationDecision GetDecision() {
		if(m_runningClusterList.size()<=1)
			return null;
		
		int clusterSize = m_runningClusterList.size();
		long time[] = new long[clusterSize];
		int jobs[] = new int[clusterSize];
		
		for(int i=0; i<clusterSize; i++) {
			ClusterNode cn = m_runningClusterList.get(i);
			for(int j=0; j<m_runningJobList.size(); j++) {
				if(!m_runningJobList.get(i).jobType.matches(HadoopJobNode.JOBTYPENAME)) 
					continue;
				HadoopJobNode jn = (HadoopJobNode)m_runningJobList.get(j);
				if(jn.currentPosition.compare(cn)) {
					time[i] += ((200-(jn.mapStatus+jn.reduceStatus))*jn.executeTime)/100;
					jobs[i]++;
					jn.hasMigrate--;
				}
			}
		}
		
		int least = 0; 
		int most  = 0;
		for(int i=0; i<clusterSize; i++) {
			if(time[least]>time[i])
				least = i;
			if(time[most ]<time[i])
				most = i;
		}
		System.out.println("Least: " + least + "(" + time[least] + ")\t" +
						   "Most : " + most  + "(" + time[most ] + ")");

		if(least==most)
			return null;
		if(jobs[most]==1)
			return null;
		
		long difference = time[most]-time[least];
		
		/**
		 * diff = time(most) - time(least)

			preJob.exe = time(most , Job)
			aftJob.exe = time(least, Job)
			
			
			===BEFORE===
			
			diff = time(most) - time(least)
			
			
			====AFTER===
			
			diff2 = | time(most)-preJob.exe - time(least)-aftJob.exe |
			      = | diff - preJob.exe - aftJob.exe |
			
				  
			diff - diff2 >0
			
			diff - | diff - preJob.exe - aftJob.exe | > 0
			
			diff - diff + preJob.exe + aftJob.exe > 0  V
			
			diff + diff - preJob.exe - aftJob.exe > 0
		 */
		
		HadoopJobNode jobToMove = null;
		HadoopJobNode jobAfterMove = null;
		ClusterNode cn = m_runningClusterList.get(most);
		for(int i=0; i<m_runningJobList.size(); i++) {
			if(m_runningJobList.get(i).jobType.matches(HadoopJobNode.JOBTYPENAME)&&m_runningJobList.get(i).currentPosition.compare(cn))
			{
				if(m_runningJobList.get(i).jobStatus==HadoopJobNode.WAITING&&m_runningJobList.get(i).hasMigrate<=0) {
					HadoopJobNode preJN = (HadoopJobNode)m_runningJobList.get(i);
					HadoopJobNode aftJN = new HadoopJobNode(cn.maxMapSlot,
												cn.maxReduceSlot,
												preJN.jobSize,
												preJN.mapNumber,
												preJN.inputFileSize,
												0,0);
					aftJN.executeTime = this.GetDecision(aftJN);
					if( (2 * difference - preJN.executeTime - aftJN.executeTime) > 0 )
					{
						if(jobToMove==null){
							jobToMove = (HadoopJobNode)m_runningJobList.get(i);
							jobAfterMove = aftJN;
						}
						else if( (+preJN.executeTime+aftJN.executeTime) < (jobToMove.executeTime+jobAfterMove.executeTime) )
						{
							jobToMove = (HadoopJobNode)m_runningJobList.get(i);
							jobAfterMove = aftJN;
						}
					}
				}		
			}
		}
		
		if(jobToMove == null) {
			System.out.println("NoJobs can be moved");
			return null;
		} else
			jobToMove.hasMigrate = 30;
		
		System.out.println("Move\t" + jobToMove + "\tto\t" + m_runningClusterList.get(least));
		
		return new MigrationDecision(jobToMove, m_runningClusterList.get(least));
		
	}

	@Override
	public DispatchDecision GetNewJobDestination() {
		return this.GetNewJobDestination(0);
	}
	
	public DispatchDecision GetNewJobDestination(int i) {
		if(!m_waitingJobList.get(i).jobType.matches(HadoopJobNode.JOBTYPENAME))
			return null;
		HadoopJobNode jn = (HadoopJobNode)m_waitingJobList.get(i);
		jn.executeTime = this.GetDecision(jn);
		
		//return (m_runningClusterList.size()>0?m_runningClusterList.get(0):null);
		if(m_runningClusterList.size()<=0)
			return null;
		else {
			m_waitingJobList.remove(jn);
			return new DispatchDecision(jn, m_runningClusterList.get(0));
		}
	}
	
	public static Policy GetPolicy() {
		if(onlyInstance != null)
			return onlyInstance;
		else
			return new ExecutionTimePolicy();
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
