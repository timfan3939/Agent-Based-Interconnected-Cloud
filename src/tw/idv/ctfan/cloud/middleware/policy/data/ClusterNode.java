package tw.idv.ctfan.cloud.middleware.policy.data;

import jade.core.ContainerID;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.xmlrpc.XmlRpcException;

import com.xensource.xenapi.Types.BadServerResponse;
import com.xensource.xenapi.Types.BootloaderFailed;
import com.xensource.xenapi.Types.LicenceRestriction;
import com.xensource.xenapi.Types.NoHostsAvailable;
import com.xensource.xenapi.Types.OperationNotAllowed;
import com.xensource.xenapi.Types.OtherOperationInProgress;
import com.xensource.xenapi.Types.UnknownBootloader;
import com.xensource.xenapi.Types.VmBadPowerState;
import com.xensource.xenapi.Types.VmHvmRequired;
import com.xensource.xenapi.Types.VmIsTemplate;
import com.xensource.xenapi.Types.XenAPIException;

import tw.idv.ctfan.cloud.middleware.Cluster.JobType;


public class ClusterNode implements Comparable<ClusterNode>
{
	// Agent Related Information
//	public String agentName;
//	public String agentAddress;
	public String agentID;
	public ContainerID agentContainer;
	public int    load;
	
	public String clusterName;
	public JobType jobType;	
	
	// System Related Information
	public long	  core;
	public long   memory;
	//public String siteIP;
	
	// Attributes about Clusters
	private HashMap<String,String> attributes = new HashMap<String, String>();
	static public HashMap<String, AttributeType> attributeType = new HashMap<String, AttributeType>();
	private ArrayList<VirtualMachineNode> machines = new ArrayList<VirtualMachineNode>();
	
	private boolean ReservedAttributeKey(String key) {
		if(key.equals("ClusterName")) return true;
		if(key.equals("Core")) return true;
		if(key.equals("Memory")) return true;
		
		return false;
	}
	
	private long GetContinuousReservedAttributeKey(String key) {
		if(key.equals("Core")) return this.core;
		if(key.equals("Memory")) return this.memory;
		
		return -1;
	}
	
	private String GetDiscreteReservedAttributeKey(String key) {
		if(key.equals("ClusterName")) return this.clusterName;
		return null;
	}
	
	
		

	// Other Related
	public boolean allowDispatch = true;

	public boolean AddDiscreteAttribute(String key, String value){
		if(!attributeType.containsKey(key)) {
			attributeType.put(key, AttributeType.Discrete);
		}
		else if(ReservedAttributeKey(key)) {
			return true;
		}		
		else if(attributeType.get(key)!=AttributeType.Discrete) {
			return true;
		}
		
		attributes.put(key, value);
		return false;
	}
	
	public boolean AddContinuousAttribute(String key, long value) {
		if(!attributeType.containsKey(key)) {
			attributeType.put(key, AttributeType.Continuous);
		}
		else if(ReservedAttributeKey(key)) {
			return true;
		}		
		else if(attributeType.get(key)!=AttributeType.Continuous) {
			return true;
		}
		
		attributes.put(key, Long.toString(value));
		return false;
	}
	
	public String GetDiscreteAttribute(String key) {
		if(!ReservedAttributeKey(key))
			return attributes.get(key);
		else
			return GetDiscreteReservedAttributeKey(key);
	}
	
	public long GetContinuousAttribute(String key) {
		if(!ReservedAttributeKey(key)) {
			if(attributeType.get(key)==AttributeType.Continuous)
				return Long.parseLong(attributes.get(key));
			else return -1;
		} else
			return GetContinuousReservedAttributeKey(key);
	}	
	
	public boolean AddMachine(VirtualMachineNode vmn) {
		if(!machines.contains(vmn)) {
			try {
				vmn.GetSpecInfo();
			} catch (Exception e) {
				e.printStackTrace();
				return true;
			}
			this.core += vmn.core;
			this.memory += vmn.memory;
			machines.add(vmn);
			return false;
		}
		return true;
	}
	
	public ArrayList<VirtualMachineNode> GetMachineList() {
		return machines;
	}
	
	public void StartCluster() throws BadServerResponse, VmBadPowerState, VmHvmRequired, VmIsTemplate, OtherOperationInProgress, OperationNotAllowed, BootloaderFailed, UnknownBootloader, NoHostsAvailable, LicenceRestriction, XenAPIException, XmlRpcException {
		System.out.println("Starting Cluster " + this.clusterName);
		for(VirtualMachineNode vmn : machines) {
			System.out.println("Starting VM " + vmn.vmNameLabel);
			vmn.StartVM();
		}
	}
	
	public ClusterNode(String clusterName, JobType jt) {
		this.clusterName = clusterName;
		
		this.load = 0;
		this.core = 0;
		this.memory = 0;
		
		this.jobType = jt;
	}
	
	
	public boolean compare(String agentID)
	{
		return (this.agentID == agentID);
			
	}	
	
	public String toString()
	{
		return this.agentID + " " + agentContainer + " " + load;
	}
	

	@Override
	public int compareTo(ClusterNode o) {
//		System.out.println(this.name + " Compare " + o.name);
		return (this.agentID==o.agentID?0:1);
	}
}