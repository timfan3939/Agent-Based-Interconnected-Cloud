package tw.idv.ctfan.cloud.middleware.policy;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import com.xensource.xenapi.VM;

import tw.idv.ctfan.RoughSet.RoughSet;
import tw.idv.ctfan.cloud.middleware.Cluster.JobType;
import tw.idv.ctfan.cloud.middleware.Java.JavaJobType;
import tw.idv.ctfan.cloud.middleware.MapReduce.MRJobType;
import tw.idv.ctfan.cloud.middleware.policy.Decision.DispatchDecision;
import tw.idv.ctfan.cloud.middleware.policy.Decision.MigrationDecision;
import tw.idv.ctfan.cloud.middleware.policy.Decision.VMManagementDecision;
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
	
	private HashMap<String, Attribute> attributes;
	private int numberOfConditionElement;
	private Attribute executionTimeAttribute;
	
	private final int maxSubSetSize = 5;
	int numSubSet;
	int[] decisionExecutionTime;
	
	/**
	 * Regenerate a rough set
	 */
	private void RefreshRoughSet(){
		if(this.m_finishJobList.size()<2*recalculateRoughSet) {
			return;
		}
		if(lastFinishedNumber == this.m_finishJobList.size()) {
			return;
		}
		lastFinishedNumber = this.m_finishJobList.size();
		
		ComputeMaxMinAttribute();
		set = new RoughSet(numberOfConditionElement);
		int[] element;
		int[] setCount = new int[numSubSet];
		decisionExecutionTime = new int[numSubSet];
		
		for(JobNode jn:m_finishJobList) {
			
			element = this.FillConditinAttributes(jn);
			
			int decision = 0;
			decision = (int) executionTimeAttribute.CalculateGroup(jn.completionTime);
			if(decision==numSubSet) decision--;
			setCount[decision]++;
			decisionExecutionTime[decision] += jn.completionTime;
			set.AddElement(element, decision);
		}
		
		for(int i=0; i<decisionExecutionTime.length; i++) {
			if(setCount[i]!=0)
				decisionExecutionTime[i] /= setCount[i];
		}
		
		set.FindCore();
		set.FindDecisionList();
	}
	
	private class Attribute {
		public long maxValue;
		public long minValue;
		long divValue;
		public ArrayList<String> allValues;
		
		public void CalculateDiv(int numSubSet) {
			this.divValue = (10*this.maxValue-10*this.minValue+10)/numSubSet;
		}
		
		public long CalculateGroup(long value) {
			return (this.divValue==0 ? 0: (10*value-10*this.minValue)/this.divValue);
		}
	}
	
	
	private void ComputeMaxMinAttribute(){
		attributes = new HashMap<String,Attribute>();
		numberOfConditionElement = 0;
		boolean isFirst = true;
		numSubSet = m_finishJobList.size()/this.maxSubSetSize;
		
		for(String key : JobNode.attributeType.keySet()) {
			isFirst = true;
			Attribute a = new Attribute();
			numberOfConditionElement++;  // for each attribute, it has a attribute that tells if the attribute is used or not
			attributes.put(key, a);
			if(JobNode.attributeType.get(key)==JobNode.AttributeType.Continuous) {
				numberOfConditionElement++; // this is where the continuous attribute is put
				for(JobNode jn : m_finishJobList){
					long value = jn.GetContinuousAttribute(key);
					if(value == -1) continue;
					if(isFirst) {
						a.maxValue = a.minValue = value;
						isFirst = false;
					} else {
						if(a.maxValue<value) a.maxValue = value;
						else if(a.minValue>value) a.minValue = value;
					}
				}
				a.CalculateDiv(numSubSet);
			} else {
				a.allValues = new ArrayList<String>();
				for(JobNode jn : m_finishJobList) {
					String value = jn.GetDiscreteAttribute(key);
					if(!a.allValues.contains(value)){
						a.allValues.add(value);
						numberOfConditionElement++;  // for each value, it has a attribute associated with it
					}
				}
			}
		}
		
		// Calculate execution time min max
		isFirst = true;
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
			}
		}
		executionTimeAttribute.CalculateDiv(numSubSet);
	}
	
	private int[] FillConditinAttributes(JobNode jn) {
		int[] element = new int[numberOfConditionElement];
		int elementCount = 0;
		for(String key : JobNode.attributeType.keySet()) {
			Attribute a = attributes.get(key);
			
			if(JobNode.attributeType.get(key) == JobNode.AttributeType.Continuous) {
				long value = jn.GetContinuousAttribute(key);
				if(value>0) {
					element[elementCount] = 1;
					element[elementCount+1] = (int)a.CalculateGroup(value);
				} else {
					element[elementCount] = 0;
					element[elementCount+1] = 0; 
				}
				elementCount += 2;
			} else if(JobNode.attributeType.get(key) == JobNode.AttributeType.Discrete) {
				String value = jn.GetDiscreteAttribute(key);
				for(int i=0; i<a.allValues.size(); i++) {
					element[elementCount+i+1] = 0;
				}
				if(value!=null) {
					element[elementCount] = 1;
					int index = a.allValues.indexOf(value);
					if(index!=-1) {
						element[elementCount + index + 1] = 1;
					}
				} else {
					element[elementCount] = 0;
				}
				elementCount += (a.allValues.size()+1);
			}
		}		
		return element;
	}
	
	public long GetPredictionResult(JobNode jn) {
		if(set == null)
			return 0;
		
		int[] element = this.FillConditinAttributes(jn);
		
		long result = 0;
		Object[] d = set.GetExactDecision(element);
		if(d==null) return 0;
		else if(d.length==0) return 0;
		
		for(Object i: d) {
			result += this.decisionExecutionTime[(Integer)i];
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
		if(this.m_waitingJobList.size()!=0 && this.m_runningClusterList.size()!=0) {
			JobNode jn = this.m_waitingJobList.get(0);
			ClusterNode cn = null;
			for(ClusterNode c:this.m_runningClusterList) {
				if(jn.jobType == c.jobType) {
					cn = c;
					break;
				}
			}
			if(cn != null)
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
							    "Hadoop Cluster 1",
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
