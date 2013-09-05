package tw.idv.ctfan.cloud.middleware.policy;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.FileOutputStream;
import java.util.HashMap;

import javax.swing.*;

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
 * The job is thrown as usual
 * 
 * But the VM will first open the private cloud
 * 
 * Unless the switch of public cloud is on, no public VM is on.
 *
 */

public class JavaPolicy1 extends Policy {
		
	FileOutputStream fout;
	
	int policyVMState;
	int policyPrivateVMState;
	int policyPublicVMState;
	private static final int Normal = 0x401;
	private static final int AskForVM = 0x402;
	private static final int RequestingVM = 0x403;
	private static final int StartingVM = 0x404;
	private static final int Disable = 0x405;
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
	
	
	public JavaPolicy1() {
		super();		
		onlyInstance = this;

		try {
			 fout = new FileOutputStream(new File("C:\\ctfan\\middlewareLog\\" + System.currentTimeMillis() + ".log"));
		} catch(Exception e) {
			System.err.println("Error while opening log file");
			e.printStackTrace();
		}		

		RefreshRoughSet();
		policyVMState = Normal;
		policyPublicVMState = Disable;
		policyPrivateVMState = Normal;
		System.out.println("=====Policy ready=====");
		
		CreateSwitchWindow();
		
	}
	
	private void CreateSwitchWindow(){
		final JFrame jf = new JFrame();
		
		jf.setTitle("Cloud Switch");
		jf.setSize(160,120);
		jf.setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);
		jf.setVisible(true);
		
		JButton button = new JButton("Switch on Private Cloud");
		jf.add(button);
		button.addActionListener(new ActionListener(){
			@Override
			public void actionPerformed(ActionEvent e) {
				jf.setVisible(false);
				policyPublicVMState = Normal;
//				policyPrivateVMState = Normal;
			}
		});
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
	public MigrationDecision GetMigrationDecision() {
		return null;
	}
	
	@Override
	public DispatchDecision GetNewJobDestination() {
		long startTime = System.nanoTime(); // predict start time
		System.out.println("===Asking New Job Destination===");
		
		// finding newly started VM
		if(policyVMState == StartingVM) {
			for(int i=0; i<m_runningClusterList.size(); i++) {
				if(m_runningClusterList.get(i).vmUUID == null) {
					ClusterNode cn = m_runningClusterList.get(i);
					clusterToStart.name = cn.name;
					clusterToStart.container = cn.container;
					clusterToStart.address = cn.address;
					m_runningClusterList.remove(i);
					m_runningClusterList.add(clusterToStart);
					clusterToStart = null;
					policyVMState = Normal;
					policyPublicVMState = (policyPublicVMState==Disable?Disable:Normal);
					policyPrivateVMState =(policyPrivateVMState==Disable?Disable:Normal);
					break;
				}
			}
		}
		
		// no job to dispatch
		if(m_waitingJobList.size()<=0)
			return null;
		
		if(this.m_runningClusterList.size()!=0) {
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
			

			int[]  vmType     = new int[m_runningClusterList.size()];
			for(int i=0; i<m_runningClusterList.size(); i++) {
				vmType[i] = m_runningClusterList.get(i).vmMaster.masterType;
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
				return new DispatchDecision(jn,m_runningClusterList.get(least));
			}
			else {
				if(policyVMState == Normal)
					policyVMState = AskForVM;				
			}			
		}
		else {
			if(policyVMState==Normal)
				policyVMState=AskForVM;
		}
		return null;			
	}
	
	public static Policy GetPolicy() {
		if(onlyInstance != null)
			return onlyInstance;
		else
			return new JavaPolicy1();
	}

	@Override
	public VMManagementDecision GetVMManagementDecision() {
		if(policyVMState == AskForVM) {
			policyVMState = RequestingVM;
			System.out.println("Ask For VM");
			if(m_availableClusterList.size()>0) {
				// find private VM
				if(policyPrivateVMState==Normal) {
					policyPrivateVMState = RequestingVM;
					for(int i=0; i<m_availableClusterList.size(); i++) {
						ClusterNode cn = m_availableClusterList.get(i);
						if(cn.vmMaster.masterType==VMMasterNode.PRIVATE) {
							policyVMState = policyPrivateVMState = StartingVM;
							clusterToStart = cn;
							return new VMManagementDecision(m_availableClusterList.remove(i),VMManagementDecision.START_VM);
						}
					}	
					policyPrivateVMState = Normal;
				}
					
				if(policyPublicVMState == Normal) {	
					policyPublicVMState = RequestingVM;
					for(int i=0; i<m_availableClusterList.size(); i++) {
						ClusterNode cn = m_availableClusterList.get(i);
						if(cn.vmMaster.masterType==VMMasterNode.PUBLIC) {
							policyVMState = policyPublicVMState = StartingVM;
							clusterToStart = cn;
							return new VMManagementDecision(m_availableClusterList.remove(i),VMManagementDecision.START_VM);
						}
					}					
					policyPublicVMState = Normal;
				}
			}
			policyVMState = Normal;
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
