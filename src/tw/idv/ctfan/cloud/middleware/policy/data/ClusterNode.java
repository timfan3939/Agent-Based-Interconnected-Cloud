package tw.idv.ctfan.cloud.middleware.policy.data;

import java.util.ArrayList;
import java.util.HashMap;


public class ClusterNode implements Comparable<ClusterNode>
{
	// Agent Related Information
	public String name;
	public String address;
	public String container;
	public int    load;
	
	// System Related Information
	public long	  core;
	public long   memory;
	//public String siteIP;
	
	// Attributes about Clusters
	static public enum AttributeType {
		Continuous, Discrete
	}
	private HashMap<String,String> attributes = new HashMap<String, String>();
	static public HashMap<String, AttributeType> attributeType = new HashMap<String, AttributeType>();
	private ArrayList<VirtualMachineNode> machines = new ArrayList<VirtualMachineNode>();	
		

	// Other Related
	public boolean allowDispatch = true;	

	public boolean AddDiscreteAttribute(String key, String value){
		if(!attributeType.containsKey(key)) {
			attributeType.put(key, AttributeType.Discrete);
		} else if(attributeType.get(key)!=AttributeType.Discrete) {
			return true;
		}
		
		attributes.put(key, value);
		return false;
	}
	
	public boolean AddContinuousAttribute(String key, long value) {
		if(!attributeType.containsKey(key)) {
			attributeType.put(key, AttributeType.Continuous);
		} else if(attributeType.get(key)!=AttributeType.Continuous) {
			return true;
		}
		
		attributes.put(key, Long.toString(value));
		return false;
	}
	
	public String GetDiscreteAttribute(String key) {
		return attributes.get(key);
	}
	
	public long GetContinuousAttribute(String key) {
		if(attributeType.get(key)==AttributeType.Continuous)
			return Long.parseLong(attributes.get(key));
		return -1;
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
	
	public ClusterNode(String name, String container, String address)
	{
		this.name = name;
		this.container = container;
		this.address = address;
		this.load = 0;
		
		this.core = 0;
		this.memory = 0;
	}
	
	public boolean compare(String name, String container, String address)
	{
		if( this.name.compareTo(name)==0 &&
			this.container.compareTo(container)==0 &&
			this.address.compareTo(address)==0)
			return true;
		else
			return false;
			
	}	
	
	public String toString()
	{
		return name + " " + container + " " + address + " " + load;
	}
	

	@Override
	public int compareTo(ClusterNode o) {
//		System.out.println(this.name + " Compare " + o.name);
		return this.name.compareTo(o.name);
	}
}