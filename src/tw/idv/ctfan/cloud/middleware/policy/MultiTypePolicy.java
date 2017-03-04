package tw.idv.ctfan.cloud.middleware.policy;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;

import com.xensource.xenapi.VM;

import tw.idv.ctfan.RoughSet.RoughSet;
import tw.idv.ctfan.cloud.middleware.Cluster.JobType;
import tw.idv.ctfan.cloud.middleware.Java.JavaJobType;
//import tw.idv.ctfan.cloud.middleware.MPI.MPIJobType;
//import tw.idv.ctfan.cloud.middleware.MapReduce.MRJobType;
import tw.idv.ctfan.cloud.middleware.Workflow.WorkflowJobType;
import tw.idv.ctfan.cloud.middleware.policy.Decision.DispatchDecision;
import tw.idv.ctfan.cloud.middleware.policy.Decision.MigrationDecision;
import tw.idv.ctfan.cloud.middleware.policy.Decision.VMManagementDecision;
import tw.idv.ctfan.cloud.middleware.policy.data.AttributeType;
import tw.idv.ctfan.cloud.middleware.policy.data.ClusterNode;
import tw.idv.ctfan.cloud.middleware.policy.data.JobNode;
import tw.idv.ctfan.cloud.middleware.policy.data.VMController;
import tw.idv.ctfan.cloud.middleware.policy.data.VirtualMachineNode;

/* * * * * * * * * *
 * This policy2 is a copy of policy.
 * Policy2 is duplicated for testing workflow on parallel system.
 * 
 * It should have following features:
 * 1. All VM is up, but the number of different types of VMs used are depends on setting.
 * 1.1 The property is set by System Monitoring Agent
 * 2. VM having more cores will dispatched first
 * 2.1 The VM having more cores will have fewer execution time
 */


public class MultiTypePolicy extends Policy {
		
	FileOutputStream fout;
	
	private final int recalculateRoughSet = 5;
	private int lastFinishedNumber = 0;
		
	public static Policy GetPolicy() {
		if(onlyInstance == null) {
			onlyInstance = new MultiTypePolicy();
		}
		return onlyInstance;
	}
	
	private MultiTypePolicy() {
		super();
		onlyInstance = this;
		
		try {
			fout = new FileOutputStream(new File("C:\\ctfan\\middlewareLog\\" + System.currentTimeMillis() + ".html"));
		} catch (Exception e) {
			System.err.println("Error while opening log file");
			e.printStackTrace();
		}
		
		JobNode.attributeType.put("PredictionTime", AttributeType.Continuous);
//		JobNode.attributeType.put("Thread", AttributeType.Continuous);
		
		RefreshRoughSet();
		System.out.println("=====Policy ready=====");
		WriteLog("=====Policy ready=====");
		WriteLog("<table>");
		
	}
	
	public void WriteLog(String msg) {
		try {
			fout.write(msg.getBytes());
			fout.write('\n');
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	/**************************************************************************
	 * Rough Set Functions
	 */
	RoughSet set;
	
	private ArrayList<Attribute> attributes;
	private int numberOfConditionElement;
	private Attribute executionTimeAttribute;
	
	private final int maxSubSetSize = 5;
	int numSubSet;
	long[] decisionExecutionTime;
	
	/**
	 * Regenerate a rough set<br/>
	 * 
	 * Procedure of refreshing a rough set object<br/>
	 * <ol>
	 * <li>Check if updating the rough set is needed.  If not needed, just return directly.</li>
	 * <li>Get all the Attribute Names, calculate the number of attributes</li>
	 * <li>Calculate each attribute's Minimum and Maximum</li>
	 * <li>Calculate each attribute's Division number</li>
	 * <li>Calculate the decision value array</li>
	 * <li>Create new {@link RoughSet} object</li>
	 * <li>Fill the new {@link RoughSet} object with each {@link JobNode}</li>
	 * <li>Run {@link RoughSet#Calculate}</li>
	 * <li>Calculate the result to the new coming {@link JobNode}</li>
	 * </ol>
	 */
	private void RefreshRoughSet(){
		if(this.m_finishJobList.size()<2*recalculateRoughSet) {
			return;
		}
		if(lastFinishedNumber == this.m_finishJobList.size()) {
			return;
		}
		lastFinishedNumber = this.m_finishJobList.size();
		
		ObtainAllAttributes();
		
		RemoveUnusedAttribute();
				
		// Re-calculate the rough set
		ComputeMaxMinAttribute();
		
		
		set = new RoughSet(numberOfConditionElement);
		long[] element;
		int[] setCount = new int[numSubSet];
		decisionExecutionTime = new long[numSubSet];
		
		for(JobNode jn:m_finishJobList) {
			
			element = this.FillConditinAttributes(jn);
			
			long decision = 0;
			decision = executionTimeAttribute.CalculateGroup(jn.completionTime);
			if(decision>numSubSet) decision = numSubSet;
			if(decision==numSubSet) decision--;
			if(decision<0) decision=0;
			setCount[(int) decision]++;
			decisionExecutionTime[(int) decision] += jn.completionTime;
			set.AddElement(set.new Element(element, decision));
		}
		
		for(int i=0; i<decisionExecutionTime.length; i++) {
			if(setCount[i]!=0)
				decisionExecutionTime[i] /= setCount[i];
		}
		
		set.Calculate();
	}
	
	private void ObtainAllAttributes() {
		this.attributes = new ArrayList<Attribute>();
		
		final AttributeType Discrete = AttributeType.Discrete;
		final AttributeType Continuous = AttributeType.Continuous;
		
		// Job Related internal Informations
//		this.attributes.add(new Attribute("Job.JobType", Discrete));
		
		for(String key : JobNode.attributeType.keySet()) {
			this.attributes.add(new Attribute("Job." + key, JobNode.attributeType.get(key)));
		}
		
		// Cluster Related internal Informations
		this.attributes.add(new Attribute("Cluster.ClusterName", Discrete));
		this.attributes.add(new Attribute("Cluster.Core", Continuous));
		this.attributes.add(new Attribute("Cluster.Memory", Continuous));
		
		for(String key: ClusterNode.attributeType.keySet()) {
			this.attributes.add(new Attribute("Cluster." + key, ClusterNode.attributeType.get(key)));
		}
	}
	
	private void RemoveUnusedAttribute() {
		// Remove unnecessary attribute
		for(int i=0; i<attributes.size(); i++) {
			Attribute attribute = attributes.get(i);
			if(attribute.attributeName.equals("Job.Command") ||
					attribute.attributeName.equals("Job.PredictionTime") ||
					// following are added because of hadoop (140830)
					attribute.attributeName.equals("Job.OutputFolder") ||
					attribute.attributeName.equals("Job.InputFolder")||
					attribute.attributeName.equals("Job.JobType")||
					attribute.attributeName.equals("Cluster.ClusterName")
				) {
				attributes.remove(attribute);
				i--;
			}
		}		
	}
	
	
	private class Attribute {
		public AttributeType attributeType;
		public String attributeName;
		public long maxValue;
		public long minValue;
		long divValue;
		public ArrayList<String> allValues;
		
		public Attribute(String name, AttributeType type) {
			attributeName = name;
			attributeType = type;
		}
		
		public int GetAttributeSize(){
			switch(attributeType) {
			case Discrete:
				return allValues.size() + 1;
			case Continuous:
				return 2;
			}
			return 0;
		}
		
		public void CalculateDiv(int numSubSet) {
			if(attributeType==AttributeType.Continuous && numSubSet!=0)
				this.divValue = (10*this.maxValue-10*this.minValue+10)/numSubSet;
			// Prevents potential errors
			else if(numSubSet == 0)
				this.divValue = 1;
		}
		
		public long CalculateGroup(long value) {
			if(attributeType==AttributeType.Continuous) {
				long result = (this.divValue==0 ? 0: (10*value-10*this.minValue)/this.divValue);
				if(result>0) return result;
			}
			return 0;
		}
		
		public String toString() {
			String s = "";
			s += this.attributeName;
			s += ":";
			if(this.attributeType==AttributeType.Continuous) {
				s += "Continuous[" + this.minValue + "/" + this.maxValue + "/" + this.divValue + "]";
			}
			else {
				s += "Discrte[";
				for(String v:this.allValues) {
					s+= v+":";
				}
				s += "]";
			}
			return s;
		}
	}
	
	
	private void ComputeMaxMinAttribute(){
		numberOfConditionElement = 0;
		numSubSet = m_finishJobList.size()/this.maxSubSetSize;
		// Added to prevent it becomes zero
		if(numSubSet == 0) 
			numSubSet = 1;
		boolean isFirst; 
		
		// Getting every possible attributes and find the minimum and maximum of the value
		for(Attribute attribute:attributes) {
			String[] fullKey = attribute.attributeName.split("\\.");
			String keySource = fullKey[0];
			String key = fullKey[1]; 
			isFirst = true;
			
			switch(attribute.attributeType) {
			case Continuous: {
				for(JobNode jn:this.m_finishJobList) {
					long value;
					if(keySource.equals("Job")) {
						value = jn.GetContinuousAttribute(key);
					}
					else if(keySource.equals("Cluster")) {
						value = jn.runningCluster.GetContinuousAttribute(key);
					}
					else {
						value = -1;
					}
					
					if(value != -1) {
						if(!isFirst) {
							if(attribute.maxValue<value)
								attribute.maxValue = value;
							else if(attribute.minValue>value)
								attribute.minValue = value;
						} else {
							attribute.maxValue = attribute.minValue = value;
							isFirst = false;
						}
					}
				}
				attribute.CalculateDiv(numSubSet);
			}	break;
			case Discrete: {
				for(JobNode jn:this.m_finishJobList) {
					String value;
					if(keySource.equals("Job")) {
						value = jn.GetDiscreteAttribute(key);
					}
					else if(keySource.equals("Cluster")) {
						value = jn.runningCluster.GetDiscreteAttribute(key);
					}
					else {
						value = null;
					}
					if(value != null) {
						if(isFirst) {
							attribute.allValues = new ArrayList<String>();
							isFirst = false;
						}
						if(!attribute.allValues.contains(value)){
							attribute.allValues.add(value);
						}
					}
				}
			}	break;
			}
//			System.out.println(attribute);
		}
		
		this.numberOfConditionElement = 0;
		for(Attribute a:this.attributes) {
			this.numberOfConditionElement += a.GetAttributeSize();
		}
		
		// Calculate execution time min max
		isFirst = true;
		executionTimeAttribute = new Attribute("Job.ExecutionTime", AttributeType.Continuous);
		for(JobNode jn : m_finishJobList) {
			if(!isFirst) {
				if(executionTimeAttribute.maxValue<jn.completionTime)
					executionTimeAttribute.maxValue = jn.completionTime;
				else if(executionTimeAttribute.minValue>jn.completionTime)
					executionTimeAttribute.minValue = jn.completionTime;
			} else {
				executionTimeAttribute.maxValue = 
					executionTimeAttribute.minValue = 
						jn.completionTime;
				isFirst = false;
			}
		}
		executionTimeAttribute.CalculateDiv(numSubSet);
		System.out.println(executionTimeAttribute);
	}
	
	private long[] FillConditinAttributes(JobNode jn) {
		long[] element = new long[numberOfConditionElement];
		int elementCount = 0;
		Arrays.fill(element, 0);
		for(Attribute attribute:attributes) {
			String[] fullKey = attribute.attributeName.split("\\.");
			String keySource = fullKey[0];
			String key = fullKey[1]; 
			
			switch(attribute.attributeType) {
			case Continuous:{
				long value;
				if(keySource.equals("Job")) {
					value = jn.GetContinuousAttribute(key);
				}
				else if(keySource.equals("Cluster")) {
					value = jn.runningCluster.GetContinuousAttribute(key);
				}
				else {
					value = -1;
				}
				
				if(value>=0) {
					element[elementCount] = 1;
					element[elementCount+1] = attribute.CalculateGroup(value);
				}
				else {
					element[elementCount] = 0;
					element[elementCount+1] = 0;
				}
			}	break;
			case Discrete:{
				String value;
				if(keySource.equals("Job")) {
					value = jn.GetDiscreteAttribute(key);
				}
				else if(keySource.equals("Cluster")) {
					value = jn.runningCluster.GetDiscreteAttribute(key);
				}
				else {
					value = null;
				}
				
				if(value!=null) {
					element[elementCount] = 1;
					int index = attribute.allValues.indexOf(value);
					if(index != -1)
						element[elementCount + index + 1] = 1;					
				} else {
					element[elementCount] = 0;
				}
				
			}	break;
			}
			elementCount += attribute.GetAttributeSize();
		}		
		return element;
	}
	
	public void OnNewJobAdded(JobNode newJob){
//		System.out.println("ininin");
		ClusterNode cn = null;
		for(ClusterNode cn2:this.m_availableClusterList) {
			if(newJob.jobType == cn2.jobType) {
				cn = cn2;
				break;
			}
		}
		
		if(cn!=null) {
			long prediction = 0;
			try {
				newJob.runningCluster = cn;
				prediction = this.GetPredictionResult(newJob);
				// The above line is commented and the following line is added to do the deadline constraint job.
//				prediction = 70000;
				newJob.AddContinuousAttribute("PredictionTime", prediction);
			} catch (Exception e) {
				prediction = MultiTypePolicy.defaultPredictionTime;
			} finally {
				newJob.AddContinuousAttribute("PredictionTime", prediction);
				newJob.runningCluster = null;
//				System.out.println("New Job " + newJob.UID + " Prediction Time: " + prediction);
			}
		} else {
			newJob.AddContinuousAttribute("PredictionTime", MultiTypePolicy.defaultPredictionTime);
		}
		
	}
	
	public long GetPredictionResult(JobNode jn) {
//		if(set == null)
//			return MultiTypePolicy.defaultPredictionTime;
//		
//		long[] element = this.FillConditinAttributes(jn);
//		
//		long result = 0;
//		long[] d = set.GetDecision(set.new Element(element, -1));
//		if(d==null){
//			System.out.println("No Match Objects");
//			return MultiTypePolicy.defaultPredictionTime;
//		}
//		else if(d.length==0){
//			System.out.println("No Match Objects");
//			return MultiTypePolicy.defaultPredictionTime;
//		}
//		
//		for(long i: d) {
//			result += this.decisionExecutionTime[(int) i];
//		}
//		
//		result /= d.length;
//		System.out.println("Final Result " + result );
//				
//		return result;
		
		/* * * * * * * * * *
		 * Modified on 2017.03.01 14:54
		 * Only returns the default execution time and the core available.
		 * It should cause the system to choose the VM that having more core first
		 */
		if(jn.runningCluster != null)
			return MultiTypePolicy.defaultPredictionTime / (jn.runningCluster.core + 1);
		else
			return MultiTypePolicy.defaultPredictionTime;
	}
	
	/**************************************************************************
	 * Policy Related Functions
	 */

	@Override
	public MigrationDecision GetMigrationDecision() {
		// Currently no migration is make.
		// This will be future work
		return null;
	}
	
	private int nextJobType = 0;
	private JobNode GetNextJob() {
		if(this.m_waitingJobList.size() == 0) {
//			System.out.println("There are no jobs to be dispatched.");
			return null;
		}
		
		JobNode nextJob = null;
		int jobTypeSize = this.m_jobTypeList.size();
		int endJobType = (nextJobType+jobTypeSize-1)%jobTypeSize;		
		
		for(int jobIndex = nextJobType; 
					jobIndex != endJobType; 
					jobIndex = (jobIndex+1)%jobTypeSize ) {
			JobType jt = m_jobTypeList.get(jobIndex);
			System.out.println("Get Next Job is checking: " + jt.getTypeName());
			
			// First finding jobs with Deadline
			int leastDeadlineJBindex = -1;
			long shortestDeadline = 0;
			for(int i=0; i<this.m_waitingJobList.size(); i++) {
				nextJob = this.m_waitingJobList.get(i);
//				nextJob.DisplayDetailedInfo();
				if(nextJob.jobType == jt && nextJob.isDeadlineJob()) {					
					if(leastDeadlineJBindex >= 0 && shortestDeadline>nextJob.GetTrueDeadline()) {
						System.out.println("Job " + nextJob.UID + " Deadline: " + nextJob.GetTrueDeadline() + " with diff " + (shortestDeadline-nextJob.GetTrueDeadline()));						
						leastDeadlineJBindex = i;
						shortestDeadline=nextJob.GetTrueDeadline();
					} else if(leastDeadlineJBindex < 0){
						leastDeadlineJBindex = i; 
						shortestDeadline = nextJob.GetTrueDeadline();
						System.out.println("Job " + nextJob.UID + " Deadline: " + nextJob.GetTrueDeadline());
					}					
				}
				nextJob = null;
			}
			if(leastDeadlineJBindex>=0) {
				nextJobType = (nextJobType+1)%m_jobTypeList.size();
				System.out.println("Return Deadline Job");
				return this.m_waitingJobList.get(leastDeadlineJBindex);
			}
			
			// Next find jobs without Deadline
			for(int i=0; i<this.m_waitingJobList.size(); i++) {
				nextJob = this.m_waitingJobList.get(i);
				
				System.out.println("checking point 1 " + nextJob.UID);
				
				int disp = 0;
				int dispPre = 0;
				long check = 0;
				for(int dd = 0; dd < nextJob.getdispatchnum(); dd++){
					check = Long.valueOf(nextJob.getdispatchsequence(dd));
					if(nextJob.UID == check){
						disp = dd;
						dispPre = disp-1;
						//System.out.println("check:"+check+",disp:"+disp+",dispPre:"+dispPre);
					}
				}
				System.out.println("checking point 2 " + nextJob.UID);
				if(nextJob.jobType == jt && nextJob.getparentsnum()==0) {
					System.out.println("checking point 3 " + nextJob.UID);
						if(disp == 0){
							System.out.println("checking point 4 " + nextJob.UID);
							nextJobType = (nextJobType+1)%m_jobTypeList.size();
							System.out.println("Send Next Job: " + nextJob.UID);
							return nextJob;
						}else{
							System.out.println("checking point 5 " + nextJob.UID);
							for(int j = 0; j < m_runningJobList.size(); j++){
								System.out.println("checking point 6 " + nextJob.UID);
								if(m_runningJobList.get(j).UID ==Long.valueOf(nextJob.getdispatchsequence(dispPre))){
									System.out.println("checking point 7 " + nextJob.UID);
									nextJobType = (nextJobType+1)%m_jobTypeList.size();
									System.out.println("Send Next Job: " + nextJob.UID);
//									nextJob.DisplayDetailedInfo();
									return nextJob;
								}else{
									System.out.println("checking point 8 " + nextJob.UID);
									for(int k = 0; k < m_finishJobList.size(); k++){
										System.out.println("checking point 10 " + nextJob.UID);
										if(m_finishJobList.get(k).UID ==Long.valueOf(nextJob.getdispatchsequence(dispPre))){
											nextJobType = (nextJobType+1)%m_jobTypeList.size();
											System.out.println("Send Next Job: " + nextJob.UID);
//											nextJob.DisplayDetailedInfo();
											return nextJob;
										}
									}
								}
							}
						}
				}else if (nextJob.jobType == jt){
					System.out.println("checking point 11 " + nextJob.UID);
					int countparent = 0;
					for(int j = 0; j < nextJob.getparentsnum(); j++){
						System.out.println("checking point 12 " + nextJob.UID);
						long a = Long.valueOf(nextJob.getparentsUID(j));
						for(int k = 0; k < m_finishJobList.size(); k++){
							System.out.println("checking point 13 " + nextJob.UID);
							if(m_finishJobList.get(k).UID == a){
								countparent++;
//								System.out.println("prepare JOB:" + nextJob.UID 
//										+ "prepare JOB Pnum:" + nextJob.getparentsnum()
//										+ ",JOB Pcount:"+ countparent);
								if(countparent == nextJob.getparentsnum()){
//									System.out.println("m_finishJob:" + m_finishJobList.get(k).UID+
//											",Long.valueOf:"+Long.valueOf(nextJob.getdispatchsequence(dispPre)));
									if(m_runningJobList.size()!=0){
										System.out.println("checking point 14 " + nextJob.UID);
										for(int l = 0; l < m_runningJobList.size(); l++){
//											System.out.println("m_runningJobList.get(l).UID:" + m_runningJobList.get(l).UID
//													+"\nnextJob.getdispatchsequence(dispPre):"+nextJob.getdispatchsequence(dispPre));
											if(m_runningJobList.get(l).UID ==Long.valueOf(nextJob.getdispatchsequence(dispPre))){
												nextJobType = (nextJobType+1)%m_jobTypeList.size();
												System.out.println("Send Job:" + nextJob.UID);
//												nextJob.DisplayDetailedInfo();
												return nextJob;
											}else{
												for(int m = 0; m < m_finishJobList.size(); m++){
//													System.out.println("m_finishJobList.get(m).UID:" + m_runningJobList.get(l).UID
//															+"\nnextJob.getdispatchsequence(dispPre):"+nextJob.getdispatchsequence(dispPre));
													if(disp == 0 || m_finishJobList.get(m).UID == Long.valueOf(nextJob.getdispatchsequence(dispPre))){
														nextJobType = (nextJobType+1)%m_jobTypeList.size();
														System.out.println("Send Job:" + nextJob.UID);
//														nextJob.DisplayDetailedInfo();
														return nextJob;
													}
												}
											}
										}
									}else{
										System.out.println("checking point 15 " + nextJob.UID);
										for(int m = 0; m < m_finishJobList.size(); m++){
											if(disp == 0 || m_finishJobList.get(m).UID == Long.valueOf(nextJob.getdispatchsequence(dispPre))){
												nextJobType = (nextJobType+1)%m_jobTypeList.size();
												System.out.println("Send Job:" + nextJob.UID);
//												nextJob.DisplayDetailedInfo();
												return nextJob;
											}
										}
									}
//									for(int l = 0; l < m_finishJobList.size(); l++){
//										if(disp == 0 || m_finishJobList.get(l).UID == Long.valueOf(nextJob.getdispatchsequence(dispPre))){
//											nextJobType = (nextJobType+1)%m_jobTypeList.size();
//											System.out.println("Send Job:" + nextJob.UID);
//											return nextJob;
//										}
//									}
								}
							}
						}
					}
				}
				System.out.println("Assigning nextJob to null");
				nextJob = null;
			}
		}
		
		// Updating nextJobType
		nextJobType = (nextJobType+1)%m_jobTypeList.size();
		
		return nextJob;
	}
	
	private static final long defaultPredictionTime = 300000;
	/* * * * * * * * * *
	 * 1-core: 300000
	 * 2-core: 150000
	 * 4-core:  60000
	 */

	/**
	 * This function dispatch the job by predicting the execution time of a job on every machine.  After that, 
	 * the cluster that can finish the job first will be dispatched the job.
	 * 
	 * @return Dispatch Command
	 * @see DispatchDecision
	 */
	
//	private long startGetNewJobDestination = 0;
//	private long counterGetNewJobDestination = 0;
//	
//	private void WritePredictionTime(String remark) {
//		WriteLog("" + counterGetNewJobDestination + "\t" +
//				this.m_finishJobList.size() + "\t" +
//				(System.currentTimeMillis() - startGetNewJobDestination) + "\t" +
//				remark);
//		counterGetNewJobDestination++;
//	}
	
	private int[] VMLimitation;
	
	public void SetVMUsageLimitation(int[] core) {
		/* * * * * * * * * *
		 * Added time: 2017.03.01 15:04
		 * 
		 * Set the upper bound limitation of specific core of VMs that can used.
		 * core[0] is the 0-core and should always be zero
		 * core[1] is the 1-core
		 * core[2] is the 2-core
		 * core[3] is the 3-core and should always be zero
		 * core[4] is the 4-core
		 */
		
		// First write a log to the file
		this.WriteLog("<tr><td>" +
				       "1-core: " + core[1] +
				   "   2-core: " + core[2] +
				   "   4-core: " + core[4]
						   + "</td></tr>");
		
		// Clone the setting to private holder
		this.VMLimitation = core.clone();
	}
	
	@Override
	public DispatchDecision GetNewJobDestination() {
//		startGetNewJobDestination = System.currentTimeMillis();
		
		if(this.m_runningClusterList.size()==0){
			System.out.println("No VMs are running, Hense no job to be dispatched.");
//			WritePredictionTime("No VM running");
			return null;
		}
		
		int runningClusterSize = this.m_runningClusterList.size();
		long[] remainTime = new long[runningClusterSize];
		long[] predictionResult = new long[runningClusterSize];
		int[] jobCount = new int[runningClusterSize];
		int[] deadlineJobCount = new int [runningClusterSize];
		Arrays.fill(jobCount, 0);
		Arrays.fill(deadlineJobCount, 0);
		Arrays.fill(remainTime, 0);
		Arrays.fill(predictionResult, 0);
		JobNode nextJob = GetNextJob();
		if(nextJob==null) {
			System.out.println("No Next Job Found.");
//			WritePredictionTime("No Job");
			return null;
		}
		System.out.println("Next Job Found.");
		
		int[] VMlimit = Arrays.copyOf(this.VMLimitation, this.VMLimitation.length);
		
		for(int i=0; i<runningClusterSize; i++) {
			ClusterNode cn = this.m_runningClusterList.get(i);
			System.out.println(cn.clusterName + " has " + cn.core + "-core cpu.  VMlimit: " + VMlimit[(int)cn.core]);
			if (VMlimit[(int) cn.core] <= 0) {
				remainTime[i] = 3000000;
			}
			else {
				VMlimit[(int) cn.core] --;
				
				for(JobNode jn : this.m_runningJobList) {
					if(jn.runningCluster == cn) {
//						long time = jn.GetContinuousAttribute("PredictionTime");
//						if(time <= 0) time = 2000000;
//						remainTime[i] += (time-jn.completionTime);
						/**
						 * Modified: 2017.03.01 20:13
						 * 
						 * Due to the test, if some on is running on a vm
						 * the vm will not allow second job dispatched.
						 */
						remainTime[i] = 2000000;
						jobCount[i]++;
						if(jn.isDeadlineJob())
							deadlineJobCount[i]++;					
					}
				}				
			}
			System.out.println(cn.clusterName + " remainTime is " + remainTime[i]);
		}
		
		try {
			RefreshRoughSet();
		} catch(Exception e) {
			System.out.println("Error while Refreshing the RoughSet");
			e.printStackTrace();
		}
		
		for(int i=0; i<runningClusterSize; i++) {
			ClusterNode destination = this.m_runningClusterList.get(i);
			if(nextJob.jobType == destination.jobType)
			try {
				nextJob.runningCluster = destination;
				predictionResult[i] = this.GetPredictionResult(nextJob);
				// Above line is commented and the following line is added to do the test about deadline constraint.
				//predictionResult[i] = 70000;
			} catch(Exception e) {
				e.printStackTrace();
				System.out.println("Prediction Error");
				predictionResult[i] = defaultPredictionTime;
			} finally {
				nextJob.runningCluster = null;
			}
			else {
				predictionResult[i] = -1;
			}
		}
		
		int least = -1;
		long[] totalResult = new long[runningClusterSize];
		for(int i=0; i<runningClusterSize; i++) {
			if(predictionResult[i]!=-1) {
				totalResult[i] = remainTime[i] + predictionResult[i];
				System.out.println("" + i + " " + totalResult[i]);
				
				if(least!=-1) {
					if(totalResult[least]>totalResult[i]) {
						least = i;
					}
				} else {
					least = i;
				}
			}
		}
		
		if(least == -1 ) {
//			WritePredictionTime("No Available VM found");
			return null;
		}
		
		// Special case if a job has deadline
		else if( ( nextJob.isDeadlineJob() && jobCount[least]<2 && 
				 	( (System.currentTimeMillis() + totalResult[least] ) < (nextJob.GetTrueDeadline()) ) ) || 
				 ( deadlineJobCount[least]==1 && jobCount[least]==1 && nextJob.isDeadlineJob()) ) {
			DispatchDecision dd = new DispatchDecision(nextJob, this.m_runningClusterList.get(least));
			nextJob.AddContinuousAttribute("PredictionTime", predictionResult[least]);
//			WritePredictionTime("Job dispatched");
			return dd;
		}
		
		else if(jobCount[least]!=0 && (totalResult[least]>20000 || jobCount[least]>=3 )) {
//			WritePredictionTime("");
			return null;
		}
		
		// Pre submit a job.  Currently not implemented yet.
//		else if(jobCount[least]==1 && remainTime[least]<10000) {
//			DispatchDecision dd = new DispatchDecision(nextJob, this.m_runningClusterList.get(least));
//			nextJob.AddContinuousAttribute("PredictionTime", predictionResult[least]);
//			return dd;
//		}
		
		else {
			DispatchDecision dd = new DispatchDecision(nextJob, this.m_runningClusterList.get(least));
			nextJob.AddContinuousAttribute("PredictionTime", predictionResult[least]);
//			WritePredictionTime("Job Dispatched");
			return dd;
		}
	}
	
	private int nextVMManageType = 0;
	@Override
	public VMManagementDecision GetVMManagementDecision() {
		JobType jt = this.m_jobTypeList.get(nextVMManageType);
//		System.out.println("VM Manage is checking: " + jt.getTypeName());
		nextVMManageType = (nextVMManageType+1)%this.m_jobTypeList.size();
		
		long[] clusterFinishTime = new long[this.m_runningClusterList.size()];
		Arrays.fill(clusterFinishTime, Long.MAX_VALUE);
		long currentTimeMilli = System.currentTimeMillis();
		
		// Calculating the finish time of every cluster
		for(int VMindex = 0; VMindex<clusterFinishTime.length; VMindex++) {
			ClusterNode cn = this.m_runningClusterList.get(VMindex);
			if(cn.jobType == jt) {
				clusterFinishTime[VMindex] = currentTimeMilli;
				for(JobNode jn:this.m_runningJobList) {
					if(jn.runningCluster == cn) {
						clusterFinishTime[VMindex] += jn.GetContinuousAttribute("PredictionTime") - jn.completionTime;
					}
				}
			}
		}
		
		// Calculating every job's finish time
		int notMeetDeadlineJobCount = 0;
		for(int JBindex=0; JBindex<this.m_waitingJobList.size(); JBindex++) {
			JobNode jn = this.m_waitingJobList.get(JBindex);
			if(jn.isDeadlineJob()&&jn.jobType==jt) {
				int leastVMindex=0;
				for(int VMindex=1; VMindex<clusterFinishTime.length; VMindex++) {
					if(clusterFinishTime[VMindex]<clusterFinishTime[leastVMindex]) {
						leastVMindex = VMindex;
					}
				}
				
				clusterFinishTime[leastVMindex] += jn.GetContinuousAttribute("PredictionTime");
				long jobFinishTime = clusterFinishTime[leastVMindex];
//				System.out.println("Job " + jn.UID + " (" + jobFinishTime + " - " + jn.GetTrueDeadline() + " = " + (jobFinishTime-(jn.GetTrueDeadline())) + ")");
				
				if( (jn.GetTrueDeadline())<jobFinishTime ) {
//					System.out.println("Job " + jn.UID + " may exceeds deadline: " + (jobFinishTime-(jn.GetTrueDeadline())) );
					notMeetDeadlineJobCount++;
				}
			}
		}
		
		if(notMeetDeadlineJobCount>0) {
			for(ClusterNode cn:this.m_availableClusterList) {
				if(cn.jobType == jt)
					return new VMManagementDecision(cn, VMManagementDecision.Command.START_VM);
			}
		}
				
		return null;
	}

	@Override
	public ArrayList<VMController> InitVMMasterList() {
		String prefix = "10.133.200.";
		String ControllerIP[] = {
				"2",	
				"3",	
				"4",	
				"5",	
				"6",	
				"7",	
				"8",	
				"9",	
				"10",	
				"11",	
				"12",	
				"242",	
				"243",	
				"244",	
				"245",	
				"246",	
				"247",	
				"248",	
				"249",	
				"252",	
				"253",
				"137",
				"138",
				"139",
				"140",
//				"141",
				"142",
				"143",
				"144",
				"145",
				"146",
		};
		
//		this.m_vmControllerList.add(new VMController("10.133.200.4", "root", "unigrid", VMController.VirtualMachineType.Private));
//		this.m_vmControllerList.add(new VMController("10.133.200.3", "root", "unigrid", VMController.VirtualMachineType.Public));
//		this.m_vmControllerList.add(new VMController("10.133.200.9", "root", "unigrid", VMController.VirtualMachineType.Public));
//		this.m_vmControllerList.add(new VMController("10.133.200.12", "root", "unigrid", VMController.VirtualMachineType.Public));
//		this.m_vmControllerList.add(new VMController("10.133.200.247", "root", "unigrid", VMController.VirtualMachineType.Public));
		
		for( int i = 0; i<ControllerIP.length; i++){
			this.m_vmControllerList.add(new VMController(prefix + ControllerIP[i], "root", "unigrid", VMController.VirtualMachineType.Public));
		}
		
		return m_vmControllerList;
	}

	@Override
	public void InitClusterList() {
		JobType java = new JavaJobType();
		JobType Workflow = new WorkflowJobType();
		
		ArrayList<String> ClusterName = new ArrayList<String>();
		ArrayList<String[]> Machines = new ArrayList<String[]>();
		ArrayList<JobType> clusterType = new ArrayList<JobType>();
		
		// 1-cpu VMs
		ClusterName.add("hdp031");
		ClusterName.add("hdp032");
//		ClusterName.add("hdp033");
//		ClusterName.add("hdp034");
//		ClusterName.add("hdp035");
//		ClusterName.add("hdp036");
//		ClusterName.add("hdp037");
//		ClusterName.add("hdp038");
//		ClusterName.add("hdp039");
//		ClusterName.add("hdp040");
//		
//		ClusterName.add("hdp041");
//		ClusterName.add("hdp042");
//		ClusterName.add("hdp043");
//		ClusterName.add("hdp044");
//		ClusterName.add("hdp045");
//		ClusterName.add("hdp046");
//		ClusterName.add("hdp047");
//		ClusterName.add("hdp048");
//		ClusterName.add("hdp049");
//		ClusterName.add("hdp050");
//
//		ClusterName.add("hdp051");
//		ClusterName.add("hdp052");
		
		// 4-cpu VMs
		ClusterName.add("hdp053");
//		ClusterName.add("hdp054");
//		ClusterName.add("hdp055");
//		ClusterName.add("hdp056");
//		ClusterName.add("hdp057");
//		ClusterName.add("hdp058");
//		ClusterName.add("hdp059");
//		ClusterName.add("hdp060");		
//
//		ClusterName.add("hdp061");
//		ClusterName.add("hdp062");
		
//		ClusterName.add("hdp063");
//		ClusterName.add("hdp065");
//		ClusterName.add("hdp067");
		
		// not in used
//		ClusterName.add("hdp064");
//		ClusterName.add("hdp066");
//		ClusterName.add("hdp068");
		
		// 2-cpu VMs
		ClusterName.add("hdp069");
//		ClusterName.add("hdp070");		

//		ClusterName.add("hdp071");  // broken
//		ClusterName.add("hdp072");  // broken
//		ClusterName.add("hdp073");
//		ClusterName.add("hdp074");
//		ClusterName.add("hdp075");
//		ClusterName.add("hdp076");
//		ClusterName.add("hdp077");
//		ClusterName.add("hdp078");
//		ClusterName.add("hdp079");
//		ClusterName.add("hdp080");		
//
//		ClusterName.add("hdp081");
//		ClusterName.add("hdp082");
		
		// other unused vms
//		ClusterName.add("hdp083");
//		ClusterName.add("hdp084");
//		ClusterName.add("hdp085");
//		ClusterName.add("hdp086");
//		ClusterName.add("hdp087");
//		ClusterName.add("hdp088");
//		ClusterName.add("hdp089");
//		ClusterName.add("hdp090");
//		ClusterName.add("hdp091");
//		ClusterName.add("hdp092");
		

		for(int cn=0; cn<ClusterName.size(); cn++) {
			String[] m = {new String(ClusterName.get(cn))};
			Machines.add(m);
			clusterType.add(java);
		}
		
		clusterType.add(Workflow);
		
		
		
		m_jobTypeList.add(java);
//		m_jobTypeList.add(hadoop);
//		m_jobTypeList.add(MPI);
		m_jobTypeList.add(Workflow);
		
//		JobNode.attributeType.put("Command", JobNode.AttributeType.Discrete);
		
		
		try {
			for(int i=0; i<ClusterName.size(); i++) {
				ClusterNode cn = new ClusterNode(ClusterName.get(i), clusterType.get(i));
				for(int j=0; j<Machines.get(i).length; j++) {
					VirtualMachineNode vmn = null;
					Set<VM> vmSet;
					for(VMController vmc:this.m_vmControllerList) {
						vmSet = VM.getByNameLabel(vmc.xenConnection, Machines.get(i)[j]);
						if(vmSet.size()==0) continue;
						for(VM vm:vmSet) {
							if(!vm.getIsASnapshot(vmc.xenConnection)&&
									!vm.getIsATemplate(vmc.xenConnection)&&
									!vm.getIsControlDomain(vmc.xenConnection)&&
									!vm.getIsSnapshotFromVmpp(vmc.xenConnection)) {
								vmn = new VirtualMachineNode(vm.getUuid(vmc.xenConnection),vmc);
								break;
							}
						}
						if(vmn != null) break;
					}
					if(vmn==null) {
						System.err.println("VM "+Machines.get(i)[j]+ " not found");
					} else {
						cn.AddMachine(vmn);
					}
				}
				this.m_availableClusterList.add(cn);
			}
		} catch(Exception e) {
			e.printStackTrace();
			return;
		}
	}
}
