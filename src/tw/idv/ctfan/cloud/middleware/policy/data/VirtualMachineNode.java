package tw.idv.ctfan.cloud.middleware.policy.data;

import org.apache.xmlrpc.XmlRpcException;

import com.xensource.xenapi.VM;
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

public class VirtualMachineNode {
	
	// System Related Information
	public long	  core;
	public long   memory;
	public long   CPURate;
	public String vmNameLabel;	
	public String vmUUID;
	//public String siteIP;
	public VMController vmController;
	
//	public VirtualMachineNode (String vmNameLabel) {
//		this.vmNameLabel = vmNameLabel;
//		vmController = null;
//	}
	
	public VirtualMachineNode (String uuid, VMController controller){
		vmUUID = uuid;
		vmController = controller;
		if(vmController==null) {
			System.err.println("VMController is null");
		}
	}
	
	public void CloseVM() throws BadServerResponse, VmBadPowerState, OtherOperationInProgress, OperationNotAllowed, VmIsTemplate, XenAPIException, XmlRpcException {
		if(vmController==null) return;
		
		VM vm = VM.getByUuid(vmController.xenConnection, vmUUID);
		vm.hardShutdown(vmController.xenConnection);
	}
	
	void StartVM() throws BadServerResponse, VmBadPowerState, VmHvmRequired, VmIsTemplate, OtherOperationInProgress, OperationNotAllowed, BootloaderFailed, UnknownBootloader, NoHostsAvailable, LicenceRestriction, XenAPIException, XmlRpcException {
		if(vmController==null) return;
		
		VM vm = VM.getByUuid(vmController.xenConnection, vmUUID);
		vm.start(vmController.xenConnection, false, false);
	}
	
	void GetSpecInfo() throws BadServerResponse, XenAPIException, XmlRpcException {
		if(vmController==null) return;
		
		VM vm = VM.getByUuid(vmController.xenConnection, vmUUID);
		vmNameLabel = vm.getNameLabel(vmController.xenConnection);
		core = vm.getVCPUsMax(vmController.xenConnection);
		memory = vm.getMemoryDynamicMax(vmController.xenConnection);
	}
}
