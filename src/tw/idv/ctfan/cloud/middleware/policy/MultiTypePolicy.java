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
import tw.idv.ctfan.cloud.middleware.MPI.MPIJobType;
import tw.idv.ctfan.cloud.middleware.MapReduce.MRJobType;
import tw.idv.ctfan.cloud.middleware.policy.Decision.DispatchDecision;
import tw.idv.ctfan.cloud.middleware.policy.Decision.MigrationDecision;
import tw.idv.ctfan.cloud.middleware.policy.Decision.VMManagementDecision;
import tw.idv.ctfan.cloud.middleware.policy.data.AttributeType;
import tw.idv.ctfan.cloud.middleware.policy.data.ClusterNode;
import tw.idv.ctfan.cloud.middleware.policy.data.JobNode;
import tw.idv.ctfan.cloud.middleware.policy.data.VMController;
import tw.idv.ctfan.cloud.middleware.policy.data.VirtualMachineNode;

public class MultiTypePolicy extends Policy {
		
	FileOutputStream fout;
	
	private final int recalculateRoughSet = 2;
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
			fout = new FileOutputStream(new File("C:\\ctfan\\middlewareLog\\" + System.currentTimeMillis() + ".log"));
		} catch (Exception e) {
			System.err.println("Error while opening log file");
			e.printStackTrace();
		}
		
		JobNode.attributeType.put("PredictionTime", AttributeType.Continuous);
//		JobNode.attributeType.put("Thread", AttributeType.Continuous);
		
		RefreshRoughSet();
		System.out.println("=====Policy ready=====");
		WriteLog("=====Policy ready=====");
		
	}
	
	private void WriteLog(String msg) {
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
					attribute.attributeName.equals("Job.PredictionTime")) {
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
			if(attributeType==AttributeType.Continuous)
				this.divValue = (10*this.maxValue-10*this.minValue+10)/numSubSet;
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
			System.out.println(attribute);
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
		if(set == null)
			return MultiTypePolicy.defaultPredictionTime;
		
		long[] element = this.FillConditinAttributes(jn);
		
		long result = 0;
		long[] d = set.GetDecision(set.new Element(element, -1));
		if(d==null){
			System.out.println("No Match Objects");
			return MultiTypePolicy.defaultPredictionTime;
		}
		else if(d.length==0){
			System.out.println("No Match Objects");
			return MultiTypePolicy.defaultPredictionTime;
		}
		
		for(long i: d) {
			result += this.decisionExecutionTime[(int) i];
		}
		
		result /= d.length;
		System.out.println("Final Result " + result );
				
		return result;
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
//			System.out.println("Get Next Job is checking: " + jt.getTypeName());
			
			// First finding jobs with Deadline
			int leastDeadlineJBindex = -1;
			long shortestDeadline = 0;
			for(int i=0; i<this.m_waitingJobList.size(); i++) {
				nextJob = this.m_waitingJobList.get(i);
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
				if(nextJob.jobType == jt) {
					nextJobType = (nextJobType+1)%m_jobTypeList.size();
//					System.out.println("Return General Job");
					return nextJob;
				}
				nextJob = null;
			}			
		}
		
		// Updating nextJobType
		nextJobType = (nextJobType+1)%m_jobTypeList.size();
		
		return nextJob;
	}
	
	private static final long defaultPredictionTime = 60000;

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
		JobNode nextJob = GetNextJob();
		if(nextJob==null) {
//			System.out.println("No Next Job Found.");
//			WritePredictionTime("No Job");
			return null;
		}
		
		for(int i=0; i<runningClusterSize; i++) {
			ClusterNode cn = this.m_runningClusterList.get(i);
			for(JobNode jn : this.m_runningJobList) {
				if(jn.runningCluster == cn) {
					long time = jn.GetContinuousAttribute("PredictionTime");
					if(time <= 0) time = 2000000;
					remainTime[i] += (time-jn.completionTime);
					jobCount[i]++;
					if(jn.isDeadlineJob())
						deadlineJobCount[i]++;					
				}
			}
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
		
		this.m_vmControllerList.add(new VMController("10.133.200.4", "root", "unigrid", VMController.VirtualMachineType.Private));
		this.m_vmControllerList.add(new VMController("10.133.200.3", "root", "unigrid", VMController.VirtualMachineType.Public));
		
		return m_vmControllerList;
	}

	@Override
	public void InitClusterList() {
		String[] ClusterName = {
							    "Hadoop Cluster 1",
							    "Hadoop Cluster 2",
//								"Java Cluster 1",
//								"Java Cluster 2",
//								"Java Cluster 3",
//								"MPI Cluster 1",
//								"MPI Cluster 2",
		};
		
		String[][] Machines = {				
				// Hadoop Clusters
				{"hdp206", "hdp205", "hdp204", "hdp203"},
				{"hdp214", "hdp216", "hdp212"},
				
				// Java Clusters
				{"hdp201"},
				{"hdp209"},
				{"hdp210"},
				
				// MPI Clusters
				{"hdp202", "hdp207", "hdp208"},
				{"hdp211", "hdp213", "hdp215", "hdp217"},
		};
		
		JobType java = new JavaJobType();
		JobType hadoop = new MRJobType();
		JobType MPI = new MPIJobType();
		JobType[] clusterType = {
				hadoop,
				hadoop,
				java,
				java,
				java,
				MPI,
				MPI,
		};
		
		m_jobTypeList.add(java);
		m_jobTypeList.add(hadoop);
		m_jobTypeList.add(MPI);
		
//		JobNode.attributeType.put("Command", JobNode.AttributeType.Discrete);
		
		
		try {
			for(int i=0; i<ClusterName.length; i++) {
				ClusterNode cn = new ClusterNode(ClusterName[i], clusterType[i]);
				for(int j=0; j<Machines[i].length; j++) {
					VirtualMachineNode vmn = null;
					Set<VM> vmSet;
					for(VMController vmc:this.m_vmControllerList) {
						vmSet = VM.getByNameLabel(vmc.xenConnection, Machines[i][j]);
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
						System.err.println("VM "+Machines[i][j]+ " not found");
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
