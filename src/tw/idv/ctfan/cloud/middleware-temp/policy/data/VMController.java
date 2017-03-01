package tw.idv.ctfan.cloud.Middleware.policy.data;

import java.net.URL;

import com.xensource.xenapi.Connection;
import com.xensource.xenapi.Session;


public class VMController {
	
	public String IP;
	public String account;
	public String password;
	
	public Connection xenConnection;
	
	public enum VirtualMachineType {
		Private, Public
	}
	public VirtualMachineType masterType;
	
	public VMController(String IP, String account, String password, VirtualMachineType masterType) {
		this.IP = IP;
		this.account = account;
		this.password = password;
		this.masterType = masterType;
		
		try {
			xenConnection = new Connection(new URL("http://" + IP));
			Session.loginWithPassword(xenConnection, account, password, null);
				
		} catch(Exception e) {
			System.err.println("Problem while connection to server " + IP);
			System.err.println("Acc: " + account + " pw: " + password);
		}
	}
}
