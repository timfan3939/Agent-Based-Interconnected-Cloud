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
			fout = new FileOutputStream(new File("C:\\ctfan\\middlewareLog\\" + System.currentTimeMillis() + ".log"));
		} catch (Exception e) {
			System.err.println("Error while opening log file");
			e.printStackTrace();
		}
		
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
	
	public long GetPredictionResult(JobNode jn) {
		if(set == null)
			return 0;
		
		long[] element = this.FillConditinAttributes(jn);
		
		long result = 0;
		long[] d = set.GetDecision(set.new Element(element, -1));
		if(d==null) return 0;
		else if(d.length==0) return 0;
		
		for(long i: d) {
			result += this.decisionExecutionTime[(int) i];
		}
		
		result /= d.length;
				
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

	@Override
	public DispatchDecision GetNewJobDestination() {
		// TODO Auto-generated method stub
		if(this.m_runningClusterList.size()>0)
		{
			RefreshRoughSet();
			System.out.println("Done Refreshing");
			JobNode jn = this.m_waitingJobList.get(0);
			jn.runningCluster = this.m_runningClusterList.get(0);
			System.out.println("start get result");
			long result = this.GetPredictionResult(jn);
			
			jn.runningCluster = null;
			System.out.println("Result " + result);
			System.out.println("Finished: " + this.m_finishJobList.size());
		}
		
		
		if(this.m_waitingJobList.size()!=0 && this.m_runningClusterList.size()!=0) {
			JobNode jn = this.m_waitingJobList.get(0);
			ClusterNode cn = null;
			for(ClusterNode c:this.m_runningClusterList) {
				if(jn.jobType == c.jobType) {
					cn = c;
					break;
				}
			}
			if(cn != null && this.m_runningJobList.size()<3)
			return new DispatchDecision(jn, cn);
		}
		return null;
	}

	@Override
	public VMManagementDecision GetVMManagementDecision() {
		// TODO Auto-generated method stub
//		if(this.m_availableClusterList.size()>0) {
//			return new VMManagementDecision(m_availableClusterList.get(0), VMManagementDecision.Command.START_VM);
//		}
		return null;
	}

	@Override
	public ArrayList<VMController> InitVMMasterList() {
		
		this.m_vmControllerList.add(new VMController("10.133.200.4", "root", "unigrid", VMController.VirtualMachineType.Private));
		
		return m_vmControllerList;
	}

	@Override
	public void InitClusterList() {
		String[] ClusterName = {"Java Cluster 1",
//							    "Hadoop Cluster 1",
		};
		
		String[][] Machines = {
				{"hdp201"},
				{"hdp206", "hdp205", "hdp204", "hdp203"},
		};
		
		JobType java = new JavaJobType();
		JobType hadoop = new MRJobType();
		JobType[] clusterType = {
				java, hadoop
		};
		
		for(JobType jn : clusterType) {
			m_jobTypeList.add(jn);
		}
		
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
