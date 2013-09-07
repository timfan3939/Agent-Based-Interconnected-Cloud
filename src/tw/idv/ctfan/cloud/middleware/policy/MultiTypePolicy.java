package tw.idv.ctfan.cloud.middleware.policy;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashMap;

import tw.idv.ctfan.RoughSet.RoughSet;
import tw.idv.ctfan.cloud.middleware.policy.Decision.DispatchDecision;
import tw.idv.ctfan.cloud.middleware.policy.Decision.MigrationDecision;
import tw.idv.ctfan.cloud.middleware.policy.Decision.VMManagementDecision;
import tw.idv.ctfan.cloud.middleware.policy.data.ClusterNode;
import tw.idv.ctfan.cloud.middleware.policy.data.JobNode;
import tw.idv.ctfan.cloud.middleware.policy.data.VMController;

public class MultiTypePolicy extends Policy {
	
	ArrayList<JobNode> m_finishJobList;
	ArrayList<JobNode> m_runningJobList;
	ArrayList<JobNode> m_waitingJobList;
	
	public enum PolicyVMState {
		Normal, AskForVM, RequestingVM, StartingVM, ClosingVM
	}
	PolicyVMState policyVMState;
	
	FileOutputStream fout;
	
	ClusterNode clusterToStart = null;
	ClusterNode clusterToShut = null;
	
	private final int recalculateRoughSet = 5;
	private int lastFinishedNumber = 0;
	
	public MultiTypePolicy() {
		super();
		onlyInstance = this;
		
		try {
			fout = new FileOutputStream(new File("C:\\ctfan\\middlewareLog\\" + System.currentTimeMillis() + ".log"));
		} catch (Exception e) {
			System.err.println("Error while opening log file");
			e.printStackTrace();
		}

		super.m_finishJobList = null;
		super.m_runningJobList = null;
		super.m_waitingJobList = null;
		
		m_finishJobList = new ArrayList<JobNode>();
		
		RefreshRoughSet();
		policyVMState = PolicyVMState.Normal;
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
			decision = (int) executionTimeAttribute.CalculateGroup(jn.executionTime);
			if(decision==numSubSet) decision--;
			setCount[decision]++;
			decisionExecutionTime[decision] += jn.executionTime;
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
				if(executionTimeAttribute.maxValue<jn.executionTime)
					executionTimeAttribute.maxValue = jn.executionTime;
				else if(executionTimeAttribute.minValue>jn.executionTime)
					executionTimeAttribute.minValue = jn.executionTime;
			} else {
				executionTimeAttribute.maxValue = 
					executionTimeAttribute.minValue = 
						jn.executionTime;
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
		// Auto-generated method stub
		return null;
	}

	@Override
	public DispatchDecision GetNewJobDestination() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public VMManagementDecision GetVMManagementDecision() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void OnNewClusterArrives(ClusterNode cn) {
		if(policyVMState == PolicyVMState.StartingVM) {
			clusterToStart.name = cn.name;
			clusterToStart.container = cn.container;
			clusterToStart.address = cn.address;
			m_runningClusterList.add(clusterToStart);
			WriteLog("Cluster " + clusterToStart.name + " is added to the list");
			clusterToStart = null;
		} else {
			WriteLog("Cluster " + cn.name + " will be added to the list.");
		}
		cn = null;
		policyVMState = PolicyVMState.Normal;
	}

	@Override
	public void OnOldClusterLeaves(ClusterNode cn) {
		m_availableClusterList.add(clusterToShut);
		WriteLog("Shutting Cluster " + cn.name);
		clusterToShut = null;
		cn = null;
		policyVMState = PolicyVMState.Normal;

	}

	@Override
	public ArrayList<VMController> InitVMMasterList() {
		ArrayList<VMController> list = new ArrayList<VMController>();
		
		list.add(new VMController("10.133.200.4", "root", "unigrid", VMController.VirtualMachineType.Private));
		
		return list;
	}

	@Override
	public void InitClusterList() {
		
	}
}
