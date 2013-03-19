package tw.idv.ctfan.cloud.middleware.policy;

import java.io.File;
import java.io.FileOutputStream;
import java.util.HashMap;
import tw.idv.ctfan.RoughSet.*;
import tw.idv.ctfan.cloud.middleware.policy.Decision.DispatchDecision;
import tw.idv.ctfan.cloud.middleware.policy.Decision.MigrationDecision;
import tw.idv.ctfan.cloud.middleware.policy.Decision.VMManagementDecision;
import tw.idv.ctfan.cloud.middleware.policy.data.ClusterNode;
import tw.idv.ctfan.cloud.middleware.policy.data.JavaJobNode;
import tw.idv.ctfan.cloud.middleware.policy.data.JobNodeBase;
import tw.idv.ctfan.cloud.middleware.policy.data.VMMasterNode;

/**
 * 
 * @author C.T.Fan
 * 
 * Java Policy second posibility
 * 
 * The job is thrown according to the deadline
 * 
 * If the deadline is short, go to the public cloud
 * 
 * If the deadline is long, go to the private cloud
 */

public class JavaPolicy2 extends Policy {
		
	FileOutputStream fout;
	
	private static final long DecisionValue = 120000;
	
	int policyPrivateVMState;
	int policyPublicVMState;
	private static final int Normal = 0x401;
	private static final int AskForVM = 0x402;
	private static final int RequestingVM = 0x403;
	private static final int StartingVM = 0x404;
	// not null if some cluster is booting up, just wait
	ClusterNode clusterToStart = null;
	
	/**
	 * Current elment's attributes
	 * 
	 * APPLICAITON NAME
	 * DATA SIZE
	 * NO. OF CORE
	 * CPU CLOCK RATE
	 * ALLOCATED MEMORY
	 * ---------------------------------
	 * EXECUTION TIME
	 */
	
	private class Attribute {
		long max;
		long min;
		long div;
		HashMap<String,Integer> dict;
		
		public void CalculateDiv(int numSubSet) {
			this.div = (this.max - this.min)/numSubSet;
		}
	}
	
	private static final int appName = 0;
	private static final int dataSize = 1;
	private static final int numCore = 2;
	private static final int cpuRate = 3;
	private static final int alloMem = 4;
	private final int numberOfConditionElement = alloMem + 1;

	private Attribute AppName = new Attribute();
	private Attribute DataSize = new Attribute();
	private Attribute NumCore = new Attribute();
	private Attribute CPURate = new Attribute();
	private Attribute AlloMem = new Attribute();
	private Attribute ExeTime = new Attribute();
	
	long[] decExecutionTime;
	
	
	private final int recalculateRoughSet = 5;
	private final int maxSubsetSize = 5;
	private int numberOfSubset;
	
	RoughSet set;
	
	public long GetDecision(JobNodeBase jn) {
		
		if(set == null)
			return jn.jobSize;
		
		int element[] = new int[numberOfConditionElement];
		
//		System.out.println("JobName " + jn.jobName);
		element[appName] = (AppName.dict.containsKey(jn.jobName)?AppName.dict.get(jn.jobName):AppName.dict.size());
		element[dataSize]= (int)(DataSize.div==0?0:(jn.jobSize-DataSize.min)/DataSize.div);
		element[numCore] = (int)(NumCore.div==0?0:(jn.currentPosition.core-NumCore.min)/NumCore.div);
		element[cpuRate] = (int)(CPURate.div==0?0:(jn.currentPosition.CPURate-CPURate.min)/CPURate.div);
		element[alloMem] = (int)(AlloMem.div==0?0:(jn.currentPosition.memory-AlloMem.min)/AlloMem.div);
		
		for(int j=0; j<element.length; j++) 
			if(element[j]==numberOfSubset)
				element[j]--;
		long r=0;
		Object[] d = set.GetExactDecision(element);
		if(d==null)
			return jn.jobSize;
		else if(d.length==0)
			return jn.jobSize;
		
		for(int i=0; i<d.length; i++) {
			r += decExecutionTime[(Integer)d[i]];
		}
		r /= d.length;
//		System.out.println("Result " + r);
		return r;
	}
	
	private int lastFinishedNumber = 0;
	
	private void RefreshRoughSet() {
		if(this.m_finishJobList.size()<2*recalculateRoughSet)
			return;
		if(lastFinishedNumber == this.m_finishJobList.size())
			return;
		lastFinishedNumber = this.m_finishJobList.size();

		ComputeMaxMinAttribute();
		
		set = new RoughSet(numberOfConditionElement);
		
		decExecutionTime = new long[numberOfSubset];
		int[] setCount = new int[numberOfSubset];
		
		for(int i=0; i<this.m_finishJobList.size(); i++) {
			if(!m_finishJobList.get(i).jobType.matches(JavaJobNode.JOBTYPENAME))
				continue;
			
			JobNodeBase jn = m_finishJobList.get(i);
			
			int[] element = new int[numberOfConditionElement];
			int decision = 0;
			
			element[appName] = (AppName.dict.containsKey(jn.jobName)?AppName.dict.get(jn.jobName):AppName.dict.size());
			element[dataSize]= (int)(DataSize.div==0?0:(jn.jobSize-DataSize.min)/DataSize.div);
			element[numCore] = (int)(NumCore.div==0?0:(jn.currentPosition.core-NumCore.min)/NumCore.div);
			element[cpuRate] = (int)(CPURate.div==0?0:(jn.currentPosition.CPURate-CPURate.min)/CPURate.div);
			element[alloMem] = (int)(AlloMem.div==0?0:(jn.currentPosition.memory-AlloMem.min)/AlloMem.div);
			
			decision = (int)(ExeTime.div==0?0:(jn.executeTime-ExeTime.min)/ExeTime.div);
			
			for(int j=0; j<element.length; j++)
				if(element[j]==numberOfSubset)
					element[j]--;
			if(decision==numberOfSubset)
				decision--;
			
			setCount[decision]++;
			decExecutionTime[decision] += jn.executeTime;
			
//			for(int test=0; test<element.length; test++)
//				System.out.print(element[test] + " ");
//			System.out.println(decision);
			
			set.AddElement(element, decision);
			
//			System.out.println("Refresh complete: " + (i));
		}
		
		for(int i=0; i<decExecutionTime.length; i++) {
			if(setCount[i]!=0) {
				decExecutionTime[i]/=setCount[i];
			}
		}
//		System.out.println("Find core start");
		set.FindCore();
//		System.out.println("Find core finish");
//		System.out.println("Find decision start");
		set.FindDecisionList();
//		System.out.println("Find decision finish");
		
	}
	
	private void ComputeMaxMinAttribute() {
		for(int i=0; i<this.m_finishJobList.size(); i++) {
			JobNodeBase jn = this.m_finishJobList.get(i);
			if(jn.jobType.matches(JavaJobNode.JOBTYPENAME)) {
				if(i!=0) {
					if(!AppName.dict.containsKey(jn.jobName))
						AppName.dict.put(jn.jobName, AppName.dict.size());
					
					DataSize.min = (DataSize.min>jn.jobSize?jn.jobSize:DataSize.min);
					DataSize.max = (DataSize.max<jn.jobSize?jn.jobSize:DataSize.max);
					
					NumCore.min = (NumCore.min>jn.currentPosition.core?jn.currentPosition.core:NumCore.min);
					NumCore.max = (NumCore.max<jn.currentPosition.core?jn.currentPosition.core:NumCore.max);
					
					CPURate.min = (CPURate.min>jn.currentPosition.CPURate?jn.currentPosition.CPURate:CPURate.min);
					CPURate.max = (CPURate.max<jn.currentPosition.CPURate?jn.currentPosition.CPURate:CPURate.max);
					
					AlloMem.min = (AlloMem.min>jn.currentPosition.memory?jn.currentPosition.memory:CPURate.min);
					AlloMem.max = (AlloMem.max<jn.currentPosition.memory?jn.currentPosition.memory:AlloMem.max);
					
					ExeTime.min = (ExeTime.min>jn.executeTime?jn.executeTime:ExeTime.min);
					ExeTime.max = (ExeTime.max<jn.executeTime?jn.executeTime:ExeTime.max);
				}
				else {
					AppName.dict = new HashMap<String, Integer>();
					AppName.dict.put(jn.jobName, AppName.dict.size());
					DataSize.min = DataSize.max = jn.jobSize;
					NumCore.min = DataSize.max = jn.currentPosition.core;
					CPURate.min = CPURate.max = jn.currentPosition.CPURate;
					AlloMem.min = AlloMem.max = jn.currentPosition.memory;
					ExeTime.min = ExeTime.max = jn.executeTime;
				}
			}
		}
		
		numberOfSubset = this.m_finishJobList.size()/this.maxSubsetSize;
		
		AppName.div = 1;
		DataSize.CalculateDiv(numberOfSubset);
		NumCore.CalculateDiv(numberOfSubset);
		CPURate.CalculateDiv(numberOfSubset);
		AlloMem.CalculateDiv(numberOfSubset);
		ExeTime.CalculateDiv(numberOfSubset);
	}
	
	
	public JavaPolicy2() {
		super();		
		onlyInstance = this;

		try {
			 fout = new FileOutputStream(new File("C:\\ctfan\\middlewareLog\\" + System.currentTimeMillis() + ".log"));
		} catch(Exception e) {
			System.err.println("Error while opening log file");
			e.printStackTrace();
		}		

		RefreshRoughSet();
		policyPrivateVMState = policyPublicVMState = Normal;
		System.out.println("=====Java Policy2 ready=====");
		
		
	}
	
	private void WriteLog(String msg) {
		try {
			fout.write(msg.getBytes());
			fout.write('\n');
		} 
		catch(Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public MigrationDecision GetDecision() {
		return null;
	}
	
	// Jobs that large enough can be dispatch to private cloud
	private DispatchDecision GetNewJobPrivateDestination() {
//		System.out.println("Test Private Cloud " + policyPrivateVMState);
		long startTime = System.nanoTime(); // predict start time
		if(this.m_runningClusterList.size()!=0) {
			// Calculate each cluster's remain time
			
			long[] remainTime = new long[m_runningClusterList.size()];
			
			for(int jobCount = 0; jobCount < this.m_runningJobList.size(); jobCount++) {
				JobNodeBase jn = this.m_runningJobList.get(jobCount);
				for(int clusterCount = 0; clusterCount < remainTime.length; clusterCount++) {
					if(jn.currentPosition!=null && jn.currentPosition.compare(m_runningClusterList.get(clusterCount))) {
						if((jn.predictTime - jn.hasBeenExecutedTime)<0 || remainTime[clusterCount]==Long.MAX_VALUE)
							remainTime[clusterCount] = Long.MAX_VALUE;
						else
							remainTime[clusterCount] += (jn.predictTime - jn.hasBeenExecutedTime);
					}
				}
			}

			int[]  vmType     = new int[m_runningClusterList.size()];
			for(int i=0; i<m_runningClusterList.size(); i++) {
				if(m_runningClusterList.get(i).vmMaster!=null)
					vmType[i] = m_runningClusterList.get(i).vmMaster.masterType;
				else
					System.err.println("vmMaster is null");
			}
			
			
			// Calculate new job on each cluster's status
			long[] estimatedTime = new long[remainTime.length];
			
			// Select the job that can be in private cloud in FIFO
			JobNodeBase jn = null;
			for(int i=0; i<m_waitingJobList.size(); i++){
				if(m_waitingJobList.get(i).deadline>=DecisionValue){
					jn = this.m_waitingJobList.get(i);
					break;
				}
			}
			if(jn == null) return null;
			
//			System.out.println("Rough Set refresh start");
			RefreshRoughSet();
//			System.out.println("Rough Set refresh finish");
//			System.out.println("Rough Set predict start");
			for(int clusterCount = 0; clusterCount < estimatedTime.length; clusterCount++) {
//				System.out.println("Rough Set predict start " + clusterCount);
				jn.currentPosition = m_runningClusterList.get(clusterCount);
				estimatedTime[clusterCount] = GetDecision(jn);
				jn.currentPosition = null;
//				System.out.println("Rough Set predict finish " + clusterCount);
			}
			
			// select appropriate place to dispatch			
			long[] deadline = new long[remainTime.length];
			int least = -1;
			for(int i=0; i<deadline.length; i++) {
				if(remainTime[i]!=Long.MAX_VALUE)
					if(vmType[i]==VMMasterNode.PRIVATE)
						deadline[i] = remainTime[i] + estimatedTime[i];
					else
						deadline[i] = remainTime[i] = Long.MAX_VALUE;
				else
					deadline[i] = Long.MAX_VALUE;
				if(deadline[i]<jn.deadline || remainTime[i]==0) {
					if(least==-1)
						least = i;
					else if(deadline[least]>deadline[i])
						least = i;
				}
			}
			long finishTime = System.nanoTime(); // predict finishTime
			WriteLog("" + (finishTime - startTime));
			
			for(int i=0; i<remainTime.length; i++)
				System.out.print(remainTime[i] + "\t");
			System.out.println();
			for(int i=0; i<estimatedTime.length; i++)
				System.out.print(estimatedTime[i] + "\t");
			System.out.println();
			for(int i=0; i<deadline.length; i++)
				System.out.print(deadline[i] + "\t");
			System.out.println();
			System.out.println("least\t" + least);
			System.out.println("deadL\t" + jn.deadline);
			System.out.println("Settg\t" + jn.command);
			
			
			if(least != -1) {
				jn.predictTime = estimatedTime[least];
				// return where the newly job should goto
				m_waitingJobList.remove(jn);
				System.out.println("Private Dispatch " + jn.UID + " with " + jn.deadline);
				return new DispatchDecision(jn,m_runningClusterList.get(least));
			}
			else {
				if(policyPrivateVMState == Normal && policyPublicVMState == Normal) {
					policyPrivateVMState = AskForVM;
				}				
			}			
		}
		return null;			
	}
	
	private DispatchDecision GetNewJobPublicDestination() {
//		System.out.println("Test Public Cloud " + policyPublicVMState);
		long startTime = System.nanoTime(); // predict start time
		if(this.m_runningClusterList.size()!=0) {
			// Calculate each cluster's remain time
			
			long[] remainTime = new long[m_runningClusterList.size()];
			
			for(int jobCount = 0; jobCount < this.m_runningJobList.size(); jobCount++) {
				JobNodeBase jn = this.m_runningJobList.get(jobCount);
				for(int clusterCount = 0; clusterCount < remainTime.length; clusterCount++) {
					if(jn.currentPosition!=null && jn.currentPosition.compare(m_runningClusterList.get(clusterCount))) {
						if((jn.predictTime - jn.hasBeenExecutedTime)<0 || remainTime[clusterCount]==Long.MAX_VALUE)
							remainTime[clusterCount] = Long.MAX_VALUE;
						else
							remainTime[clusterCount] += (jn.predictTime - jn.hasBeenExecutedTime);
					}
				}
			}

			int[]  vmType     = new int[m_runningClusterList.size()];
			for(int i=0; i<m_runningClusterList.size(); i++) {
				if(m_runningClusterList.get(i).vmMaster!=null)
					vmType[i] = m_runningClusterList.get(i).vmMaster.masterType;
				else
					System.err.println("vmMaster is null");;
			}
			
			
			// Calculate new job on each cluster's status
			long[] estimatedTime = new long[remainTime.length];
			
			// Select the job that can be in private cloud in FIFO
			JobNodeBase jn = null;
			for(int i=0; i<m_waitingJobList.size(); i++){
				if(m_waitingJobList.get(i).deadline<DecisionValue){
					jn = this.m_waitingJobList.get(i);
					break;
				}
			}
			if(jn == null) return null;
			
//			System.out.println("Rough Set refresh start");
			RefreshRoughSet();
//			System.out.println("Rough Set refresh finish");
//			System.out.println("Rough Set predict start");
			for(int clusterCount = 0; clusterCount < estimatedTime.length; clusterCount++) {
//				System.out.println("Rough Set predict start " + clusterCount);
				jn.currentPosition = m_runningClusterList.get(clusterCount);
				estimatedTime[clusterCount] = GetDecision(jn);
				jn.currentPosition = null;
//				System.out.println("Rough Set predict finish " + clusterCount);
			}
			
			// select appropriate place to dispatch			
			long[] deadline = new long[remainTime.length];
			int least = -1;
			for(int i=0; i<deadline.length; i++) {
				if(remainTime[i]!=Long.MAX_VALUE)
					if(vmType[i]==VMMasterNode.PUBLIC)
						deadline[i] = remainTime[i] + estimatedTime[i];
					else
						deadline[i] = remainTime[i] = Long.MAX_VALUE;
				else
					deadline[i] = Long.MAX_VALUE;
				if(deadline[i]<jn.deadline || remainTime[i]==0) {
					if(least==-1)
						least = i;
					else if(deadline[least]>deadline[i])
						least = i;
				}
			}
			long finishTime = System.nanoTime(); // predict finishTime
			WriteLog("" + (finishTime - startTime));
			
			for(int i=0; i<remainTime.length; i++)
				System.out.print(remainTime[i] + "\t");
			System.out.println();
			for(int i=0; i<estimatedTime.length; i++)
				System.out.print(estimatedTime[i] + "\t");
			System.out.println();
			for(int i=0; i<deadline.length; i++)
				System.out.print(deadline[i] + "\t");
			System.out.println();
			System.out.println("least\t" + least);
			System.out.println("deadL\t" + jn.deadline);
			System.out.println("Settg\t" + jn.command);
			
			
			if(least != -1) {
				jn.predictTime = estimatedTime[least];
				// return where the newly job should goto
				m_waitingJobList.remove(jn);
				System.out.println("Public Dispatch " + jn.UID + " with " + jn.deadline);
				return new DispatchDecision(jn,m_runningClusterList.get(least));
			}
			else {
				if(policyPrivateVMState == Normal && policyPublicVMState == Normal) {
					policyPublicVMState = AskForVM;
				}				
			}			
		}
		return null;			
	}
	
	int count=0;
	boolean policySwitch = false;
	
	@Override
	public DispatchDecision GetNewJobDestination() {
		long startTime = System.nanoTime(); // predict start time
//		System.out.println("Running VM Count: " + m_runningClusterList.size());
//		System.out.println("Shutted VM Count: " + m_availableClusterList.size());
//		System.out.println("clusterToStart is " + (clusterToStart==null?"":"not") + " null");
		// finding newly started VM
		// Than copy the information and delete the original
		if(policyPrivateVMState == StartingVM || policyPublicVMState == StartingVM) {
			for(int i=0; i<m_runningClusterList.size(); i++) {
				if(m_runningClusterList.get(i).vmUUID == null) {
					ClusterNode cn = m_runningClusterList.get(i);
					clusterToStart.name = cn.name;
					clusterToStart.container = cn.container;
					clusterToStart.address = cn.address;
					m_runningClusterList.remove(i);
					m_runningClusterList.add(clusterToStart);
					clusterToStart = null;
					policyPrivateVMState = policyPublicVMState = Normal;
					break;
				}
			}
		} 
//		System.out.println("Running VM Count: " + m_runningClusterList.size());
//		System.out.println("Shutted VM Count: " + m_availableClusterList.size());
//		System.out.println("clusterToStart is " + (clusterToStart==null?"":"not") + " null");
		
		// no job to dispatch
		if(m_waitingJobList.size()<=0)
			return null;
		
		DispatchDecision decision = null;
		if(this.m_runningClusterList.size()!=0) {
			if(!policySwitch) {
				
				// Calculate each cluster's remain time
				
				long[] remainTime = new long[m_runningClusterList.size()];
				for(int i=0; i<remainTime.length; i++)
					remainTime[i] = 0;
				
				for(int jobCount = 0; jobCount < this.m_runningJobList.size(); jobCount++) {
					JobNodeBase jn = this.m_runningJobList.get(jobCount);
					for(int clusterCount = 0; clusterCount < remainTime.length; clusterCount++) {
						if(jn.currentPosition!=null && jn.currentPosition.compare(m_runningClusterList.get(clusterCount))) {
							if((jn.predictTime - jn.hasBeenExecutedTime)<0 || remainTime[clusterCount]==Long.MAX_VALUE)
								remainTime[clusterCount] = Long.MAX_VALUE;
							else
								remainTime[clusterCount] += (jn.predictTime - jn.hasBeenExecutedTime);
						}
					}
				}
				
				
				// Calculate new job on each cluster's status
				long[] estimatedTime = new long[remainTime.length];
				JobNodeBase jn = this.m_waitingJobList.get(0);
				for(int i=0; i<estimatedTime.length; i++)
					estimatedTime[i] = 0;
				
	//			System.out.println("Rough Set refresh start");
				RefreshRoughSet();
	//			System.out.println("Rough Set refresh finish");
	//			System.out.println("Rough Set predict start");
				for(int clusterCount = 0; clusterCount < estimatedTime.length; clusterCount++) {
	//				System.out.println("Rough Set predict start " + clusterCount);
					jn.currentPosition = m_runningClusterList.get(clusterCount);
					estimatedTime[clusterCount] = GetDecision(jn);
					jn.currentPosition = null;
	//				System.out.println("Rough Set predict finish " + clusterCount);
				}
				
				// select appropriate place to dispatch			
				long[] deadline = new long[remainTime.length];
				int least = -1;
				for(int i=0; i<deadline.length; i++) {
					if(remainTime[i]!=Long.MAX_VALUE)
						deadline[i] = remainTime[i] + estimatedTime[i];
					else
						deadline[i] = Long.MAX_VALUE;
					if(deadline[i]<jn.deadline || remainTime[i]==0) {
						if(least==-1)
							least = i;
						else if(deadline[least]>deadline[i])
							least = i;
					}
				}
				long finishTime = System.nanoTime(); // predict finishTime
				WriteLog("" + (finishTime - startTime));
				
				for(int i=0; i<remainTime.length; i++)
					System.out.print(remainTime[i] + "\t");
				System.out.println();
				for(int i=0; i<estimatedTime.length; i++)
					System.out.print(estimatedTime[i] + "\t");
				System.out.println();
				for(int i=0; i<deadline.length; i++)
					System.out.print(deadline[i] + "\t");
				System.out.println();
				System.out.println("least\t" + least);
				System.out.println("deadL\t" + jn.deadline);
				System.out.println("Settg\t" + jn.command);
				
	//			if((jn.deadline <= deadline[least]&&deadline[least]!=Long.MAX_VALUE)||deadline[least]==estimatedTime[least]) {
				
				
				if(least != -1) {
					jn.predictTime = estimatedTime[least];
					// return where the newly job should goto
					m_waitingJobList.remove(0);
					decision = new DispatchDecision(jn,m_runningClusterList.get(least));
				}
				else {
					if(policyPrivateVMState == Normal)
						policyPrivateVMState = AskForVM;				
				}
			}
			
			else {			
				if(count%2==0){
					decision = GetNewJobPrivateDestination();
					if(decision == null)
						decision = GetNewJobPublicDestination();
				} else {
					decision = GetNewJobPublicDestination();
					if(decision == null)
						decision = GetNewJobPrivateDestination();
				}
				count++;
			}
			return decision;
		}
		else {
			if(policyPrivateVMState == Normal && policyPublicVMState == Normal) {
				if(m_waitingJobList.get(0).deadline >= DecisionValue)
					policyPrivateVMState = AskForVM;
					// TODO: re-write here;
				else
					policyPrivateVMState = AskForVM;
			}			
		}
		return null;			
	}
	
	public static Policy GetPolicy() {
		if(onlyInstance != null)
			return onlyInstance;
		else
			return new JavaPolicy2();
	}

	@Override
	public VMManagementDecision GetVMManagementDecision() {
		if(policyPublicVMState==AskForVM) {
			policyPublicVMState = RequestingVM;
			if(m_availableClusterList.size()>0) {
				for(int i=0; i<m_availableClusterList.size(); i++) {					
					if(m_availableClusterList.get(i).vmMaster.masterType==VMMasterNode.PUBLIC){
						clusterToStart = m_availableClusterList.get(i);
						m_availableClusterList.remove(i);
						policyPublicVMState = StartingVM;
						return new VMManagementDecision(clusterToStart, VMManagementDecision.START_VM);
					}				
				}
				policyPublicVMState = Normal;
			}
			else
				policyPublicVMState = Normal;
		} else if(policyPrivateVMState==AskForVM) {
			policyPrivateVMState = RequestingVM;
			if(m_availableClusterList.size()>0) {
				for(int i=0; i<m_availableClusterList.size(); i++) {					
					if(m_availableClusterList.get(i).vmMaster.masterType==VMMasterNode.PRIVATE){
						clusterToStart = m_availableClusterList.get(i);
						m_availableClusterList.remove(i);
						policyPrivateVMState = StartingVM;
						return new VMManagementDecision(clusterToStart, VMManagementDecision.START_VM);
					}				
				}
				policySwitch = true;
				System.out.println("===Start Init Public Cloud===");
				policyPrivateVMState = Normal;
			}
			else
				policyPrivateVMState = Normal;
		}
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
