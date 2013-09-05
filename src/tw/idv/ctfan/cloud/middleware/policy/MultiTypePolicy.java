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

public class MultiTypePolicy extends Policy {
	
	ArrayList<JobNode> m_finishJobList;
	
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
	
	RoughSet set;
	
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
	}
	
	private class Attribute {
		public long maxValue;
		public long minValue;
		public ArrayList<String> allValues;
	}
	
	private HashMap<String, Attribute> attributes;
	private int numberOfConditionElement;
	private Attribute executionTimeAttribute;
	
	private void ComputeMaxMinAttribute(){
		attributes = new HashMap<String,Attribute>();
		numberOfConditionElement = 0;
		boolean isFirst = true;
		
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
	}

	@Override
	public MigrationDecision GetMigrationDecision() {
		// TODO Auto-generated method stub
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
		// TODO Auto-generated method stub

	}

	@Override
	public void OnOldClusterLeaves(ClusterNode cn) {
		// TODO Auto-generated method stub

	}

}
