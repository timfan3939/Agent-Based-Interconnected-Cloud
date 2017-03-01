#!/usr/bin/env python3

import json
import math
import random

# taskworkflow
# |- taskname
# |- program
# |- task_id
# |- command
# \- _inE
#    \- _outE
# |- _outE
# |  |- _inV

def main():
	#item = EpigenmoicsWorkflow()
	item = MontageWorkflow()
	#item = LIGOWorkflow()
	#item = CyberShakeWorkflow()
	
	item.generate(4)
	item.generate(6)
	item.generate(8)
	item.generate(10)
	item.generate(12)
	
def tasknameGen(note, id):
	return 'task_{}_{:0>5}'.format(note, id)
	
class CyberShakeWorkflow:
	task_size = 5
	def gen_ExtractSGT(self, id, parallelism):
		tasks = []
		for i in range(2):
			task = {}
			task['taskname'] = tasknameGen('ExtractSGT', id)
			task['program'] = 'DummyTask.jar'
			task['task_id'] = id
			task['command'] = self.task_size
			task['_outE'] = []
			task['_inE'] = []
			
			tasks.append(task)
			id += 1
		
		self.ExtractSGT = tasks
		return id
	
	def gen_SeismogramSynthesis(self, id, parallelism):
		tasks = []
		for i in range(parallelism):
			task = {}
			task['taskname'] = tasknameGen('ExtractSGT', id)
			task['program'] = 'DummyTask.jar'
			task['task_id'] = id
			task['command'] = self.task_size
			task['_outE'] = []
			task['_inE'] = []
			
			tasks.append(task)
			id += 1
		
		self.SeismogramSynthesis = tasks
		return id
	
	def gen_ZipSeis(self, id, parallelism):
		tasks = []
		task = {}
		task['taskname'] = tasknameGen('ZipSeis', id)
		task['program'] = 'DummyTask.jar'
		task['task_id'] = id
		task['command'] = self.task_size
		task['_outE'] = []
		task['_inE'] = []
			
		tasks.append(task)
		id += 1
		
		self.ZipSeis = tasks
		return id
	
	def gen_PeakValCalcOkaya(self, id, parallelism):
		tasks = []
		for i in range(parallelism):
			task = {}
			task['taskname'] = tasknameGen('PeakValCalcOkaya', id)
			task['program'] = 'DummyTask.jar'
			task['task_id'] = id
			task['command'] = self.task_size
			task['_outE'] = []
			task['_inE'] = []
			
			tasks.append(task)
			id += 1
		
		self.PeakValCalcOkaya = tasks
		return id
	
	def gen_ZipPSA(self, id, parallelism):
		tasks = []
		task = {}
		task['taskname'] = tasknameGen('ZipPSA', id)
		task['program'] = 'DummyTask.jar'
		task['task_id'] = id
		task['command'] = self.task_size
		task['_outE'] = []
		task['_inE'] = []
			
		tasks.append(task)
		id += 1
		
		self.ZipPSA = tasks
		return id
	
	def connect_ExtractSGT_SeismogramSynthesis(self):
		a_tasks= self.ExtractSGT
		b_tasks = self.SeismogramSynthesis
		
		a_len = len(a_tasks)
		b_len = len(b_tasks)
		
		count = 0
		
		for i in range(b_len):
			a = a_tasks[i%a_len]
			b = b_tasks[i]
			a['_outE'].append( {'_inV':b['task_id']} )
			b['_inE'].append( {'_outV':a['task_id']} )	
		
		pass
	
	def connent_SeismogramSynthesis_ZipSeis(self):
		a_tasks = self.SeismogramSynthesis
		b_tasks = self.ZipSeis
		
		b = b_tasks[0]		
		for a in a_tasks:
			a['_outE'].append( {'_inV':b['task_id']} )
			b['_inE'].append( {'_outV':a['task_id']} )			
		pass
	
	def connect_SeismogramSynthesis_PeakValCalcOkaya(self):
		a_tasks = self.SeismogramSynthesis
		b_tasks = self.PeakValCalcOkaya
		
		for i in range( len(a_tasks) ):
			a = a_tasks[i]
			b = b_tasks[i]
			a['_outE'].append( {'_inV':b['task_id']} )
			b['_inE'].append( {'_outV':a['task_id']} )			
		pass
	
	def connect_PeakValCalcOkaya_ZipPSA(self):
		a_tasks = self.PeakValCalcOkaya
		b_tasks = self.ZipPSA
		
		b = b_tasks[0]		
		for a in a_tasks:
			a['_outE'].append( {'_inV':b['task_id']} )
			b['_inE'].append( {'_outV':a['task_id']} )			
		pass
	
	def generate(self, parallelism):
		id = 1
		
		id = self.gen_ExtractSGT(id, parallelism)
		id = self.gen_SeismogramSynthesis(id, parallelism)
		id = self.gen_ZipSeis(id, parallelism)
		id = self.gen_PeakValCalcOkaya(id, parallelism)
		id = self.gen_ZipPSA(id, parallelism)
				
		print('Total task generated:', id-1)
		
		self.connect_ExtractSGT_SeismogramSynthesis()
		self.connent_SeismogramSynthesis_ZipSeis()
		self.connect_SeismogramSynthesis_PeakValCalcOkaya()
		self.connect_PeakValCalcOkaya_ZipPSA()		
		
		self.workflow = {'taskworkflow':[]}
		self.workflow['taskworkflow'] += self.ExtractSGT
		self.workflow['taskworkflow'] += self.SeismogramSynthesis
		self.workflow['taskworkflow'] += self.ZipSeis
		self.workflow['taskworkflow'] += self.PeakValCalcOkaya
		self.workflow['taskworkflow'] += self.ZipPSA
		
		json.dumps(self.workflow, indent=4)
		filename = 'workflow CyberShake {} tasks size {} parallelism {}.json'.format(id-1, self.task_size, parallelism)
		json.dump(self.workflow, open(filename, encoding='utf8', mode='w'), indent=4)		
		
		pass
	
	pass
	
class LIGOWorkflow:
	task_size = 5
	def gen_TmpltBank(self, id, parallelism):
		tasks = []
		for i in range(parallelism):
			task = {}
			task['taskname'] = tasknameGen('TmpltBank', id)
			task['program'] = 'DummyTask.jar'
			task['task_id'] = id
			task['command'] = self.task_size
			task['_outE'] = []
			task['_inE'] = []
			
			tasks.append(task)
			id += 1
		
		self.TmpltBank = tasks
		
		return id
	
	def gen_Inspiral_phase1(self, id, parallelism):
		tasks = []
		for i in range(parallelism):
			task = {}
			task['taskname'] = tasknameGen('Inspiral_phase1', id)
			task['program'] = 'DummyTask.jar'
			task['task_id'] = id
			task['command'] = self.task_size
			task['_outE'] = []
			task['_inE'] = []
			
			tasks.append(task)
			id += 1
		
		self.Inspiral_phase1 = tasks
		
		return id
	
	def gen_Thinca_phase1(self, id, parallelism):
		tasks = []
		size = math.ceil(parallelism/5)
		for i in range(size):
			task = {}
			task['taskname'] = tasknameGen('Thinca_phase1', id)
			task['program'] = 'DummyTask.jar'
			task['task_id'] = id
			task['command'] = self.task_size
			task['_outE'] = []
			task['_inE'] = []
			
			tasks.append(task)
			id += 1
		
		self.Thinca_phase1 = tasks
		
		return id
	
	def gen_TrigBank(self, id, parallelism):
		tasks = []
		for i in range(parallelism):
			task = {}
			task['taskname'] = tasknameGen('TrigBank', id)
			task['program'] = 'DummyTask.jar'
			task['task_id'] = id
			task['command'] = self.task_size
			task['_outE'] = []
			task['_inE'] = []
			
			tasks.append(task)
			id += 1
		
		self.TrigBank = tasks
		
		return id
	
	def gen_Inspiral_phase2(self, id, parallelism):
		tasks = []
		for i in range(parallelism):
			task = {}
			task['taskname'] = tasknameGen('Inspiral_phase2', id)
			task['program'] = 'DummyTask.jar'
			task['task_id'] = id
			task['command'] = self.task_size
			task['_outE'] = []
			task['_inE'] = []
			
			tasks.append(task)
			id += 1
		
		self.Inspiral_phase2 = tasks
		
		return id
	
	def gen_Thinca_phase2(self, id, parallelism):
		tasks = []
		size = math.ceil(parallelism/5)
		for i in range(size):
			task = {}
			task['taskname'] = tasknameGen('Thinca_phase2', id)
			task['program'] = 'DummyTask.jar'
			task['task_id'] = id
			task['command'] = self.task_size
			task['_outE'] = []
			task['_inE'] = []
			
			tasks.append(task)
			id += 1
		
		self.Thinca_phase2 = tasks
		
		return id
		
	def connect_TmpltBank_Inspiral_phase1(self):		
		for i in range(len(self.TmpltBank)):
			a = self.TmpltBank[i]
			b = self.Inspiral_phase1[i]
			a['_outE'].append( {'_inV':b['task_id']} )
			b['_inE'].append( {'_outV':a['task_id']} )
		pass
	
	def connect_Inspiral_phase1_Thinca_phase1(self):
		for i in range( len(self.Inspiral_phase1) ):
			a = self.Inspiral_phase1[i]
			b = self.Thinca_phase1[math.floor(i/5)]
			a['_outE'].append( {'_inV':b['task_id']} )
			b['_inE'].append( {'_outV':a['task_id']} )
		pass
		
	def connect_Thinca_phase1_TrigBank(self):
		for i in range( len(self.TrigBank) ):
			b = self.TrigBank[i]
			a = self.Thinca_phase1[math.floor(i/5)]
			a['_outE'].append( {'_inV':b['task_id']} )
			b['_inE'].append( {'_outV':a['task_id']} )	
		pass
		
	def connect_TrigBank_Inspiral_phase2(self):		
		for i in range(len(self.TrigBank)):
			a = self.TrigBank[i]
			b = self.Inspiral_phase2[i]
			a['_outE'].append( {'_inV':b['task_id']} )
			b['_inE'].append( {'_outV':a['task_id']} )
		pass
		
	def connect_Inspiral_phase2_Thinca_phase2(self):
		for i in range( len(self.Inspiral_phase2) ):
			a = self.Inspiral_phase2[i]
			b = self.Thinca_phase2[math.floor(i/5)]
			a['_outE'].append( {'_inV':b['task_id']} )
			b['_inE'].append( {'_outV':a['task_id']} )
		pass
		
	def generate(self, parallelism):
		id = 1
		
		id = self.gen_TmpltBank(id, parallelism)
		id = self.gen_Inspiral_phase1(id, parallelism)
		id = self.gen_Thinca_phase1(id, parallelism)
		id = self.gen_TrigBank(id, parallelism)
		id = self.gen_Inspiral_phase2(id, parallelism)
		id = self.gen_Thinca_phase2(id, parallelism)
			
		print('Total task generated:', id-1)
		
		self.connect_TmpltBank_Inspiral_phase1()
		self.connect_Inspiral_phase1_Thinca_phase1()
		self.connect_Thinca_phase1_TrigBank()
		self.connect_TrigBank_Inspiral_phase2()
		self.connect_Inspiral_phase2_Thinca_phase2()
		
		# workflow generating
		self.workflow = {'taskworkflow':[]}
		self.workflow['taskworkflow'] += self.TmpltBank
		self.workflow['taskworkflow'] += self.Inspiral_phase1
		self.workflow['taskworkflow'] += self.Thinca_phase1
		self.workflow['taskworkflow'] += self.TrigBank
		self.workflow['taskworkflow'] += self.Inspiral_phase2
		self.workflow['taskworkflow'] += self.Thinca_phase2
		
		json.dumps(self.workflow, indent=4)
		filename = 'workflow LIGO {} tasks size {} parallelism {}.json'.format(id, self.task_size, parallelism)
		json.dump(self.workflow, open(filename, encoding='utf8', mode='w'), indent=4)		
	pass

class MontageWorkflow:
	task_size = 5
	def gen_mProjectPP(self, id, parallelism):
		tasks = []
		for i in range(parallelism):
			task = {}
			task['taskname'] = tasknameGen('mProjectPP', id)
			task['program'] = 'DummyTask.jar'
			task['task_id'] = id
			task['command'] = self.task_size
			task['_outE'] = []
			task['_inE'] = []
			
			tasks.append(task)
			id += 1
		
		self.mProjectPP = tasks
		
		return id
	
	def gen_mDiffFit(self, id, parallelism):
		totalTask = math.ceil((parallelism*(parallelism+1)/20))
		totalTask -= 1
		tasks = []
		for i in range(totalTask):
			task = {}
			task['taskname'] = tasknameGen('mDiffFit', id)
			task['program'] = 'DummyTask.jar'
			task['task_id'] = id
			task['command'] = self.task_size
			task['_outE'] = []
			task['_inE'] = []
			
			tasks.append(task)
			id += 1
		
		self.mDiffFit = tasks
		
		return id
	
	def gen_mConcatFit(self, id):
		task = {}
		task['taskname'] = tasknameGen('mConcatFit', id)
		task['program'] = 'DummyTask.jar'
		task['task_id'] = id
		task['command'] = self.task_size
		task['_outE'] = []
		task['_inE'] = []
		
		self.mConcatFit = [task]
		
		return id+1
	
	def gen_mBgModel(self, id):
		task = {}
		task['taskname'] = tasknameGen('mBgModel', id)
		task['program'] = 'DummyTask.jar'
		task['task_id'] = id
		task['command'] = self.task_size
		task['_outE'] = []
		task['_inE'] = []
		
		self.mBgModel = [task]
		
		return id+1
	
	def gen_mBackground(self, id, parallelism):
		tasks = []
		for i in range(parallelism):
			task = {}
			task['taskname'] = tasknameGen('mBackground', id)
			task['program'] = 'DummyTask.jar'
			task['task_id'] = id
			task['command'] = self.task_size
			task['_outE'] = []
			task['_inE'] = []
			
			tasks.append(task)
			id += 1
		
		self.mBackground = tasks
		
		return id
	
	def gen_mImgTbl(self, id):
		task = {}
		task['taskname'] = tasknameGen('mImgTbl', id)
		task['program'] = 'DummyTask.jar'
		task['task_id'] = id
		task['command'] = self.task_size
		task['_outE'] = []
		task['_inE'] = []
		
		self.mImgTbl = [task]
		
		return id+1
	
	def gen_mAdd(self, id):
		task = {}
		task['taskname'] = tasknameGen('mAdd', id)
		task['program'] = 'DummyTask.jar'
		task['task_id'] = id
		task['command'] = self.task_size
		task['_outE'] = []
		task['_inE'] = []
		
		self.mAdd = [task]
		
		return id+1
	
	def gen_mShrink(self, id):
		task = {}
		task['taskname'] = tasknameGen('mShrink', id)
		task['program'] = 'DummyTask.jar'
		task['task_id'] = id
		task['command'] = self.task_size
		task['_outE'] = []
		task['_inE'] = []
		
		self.mShrink = [task]
		
		return id+1
	
	def gen_mJPEG(self, id):
		task = {}
		task['taskname'] = tasknameGen('mJPEG', id)
		task['program'] = 'DummyTask.jar'
		task['task_id'] = id
		task['command'] = self.task_size
		task['_outE'] = []
		task['_inE'] = []
		
		self.mJPEG = [task]
		
		return id+1
	
	
	def connect_mProjectPP_mDiffFit(self):
		a = self.mProjectPP
		b = self.mDiffFit
		a_len = len(a)
		b_len = len(b)
		
		complicity = math.ceil(a_len/10)
		if complicity < 3:
			complicity = 3
		
		lines = []		
		for i in range(b_len):	
			notFound = True
			while notFound:
				current = set()
				for j in range(complicity):
					current.add(random.randrange(a_len))
				notFound = False
				for j in lines:
					if j <= current or current <= j:
						notFound = True
						print('duplicated')
				if len(current) < 2:
					notFound = True
				if not notFound:
					lines.append(current)
			
		for b_count in range(b_len):
			b_task = b[b_count]			
			
			for a_count in lines[b_count]:
				a_task = a[a_count]
				
				a_task['_outE'].append( {'_inV':b_task['task_id']} )
				b_task['_inE'].append( {'_outV':a_task['task_id']} )		
		pass
		
	def connect_mDiffFit_mConcatFit(self):
		b = self.mConcatFit[0]
		for a in self.mDiffFit:
			a['_outE'].append( {'_inV':b['task_id']} )
			b['_inE'].append( {'_outV':a['task_id']} )			
		pass
		
	def connect_mConcatFit_mBgModel(self):
		a = self.mConcatFit[0]
		b = self.mBgModel[0]	
		a['_outE'].append( {'_inV':b['task_id']} )
		b['_inE'].append( {'_outV':a['task_id']} )
		pass
		
	def connect_mBgModel_mBackground(self):
		a = self.mBgModel[0]
		for b in self.mBackground:
			a['_outE'].append( {'_inV':b['task_id']} )
			b['_inE'].append( {'_outV':a['task_id']} )
		pass
		
	def connect_mProjectPP_mBackground(self):
		for i in range( len(self.mProjectPP) ):
			a = self.mProjectPP[i]
			b = self.mBackground[i]
			a['_outE'].append( {'_inV':b['task_id']} )
			b['_inE'].append( {'_outV':a['task_id']} )
		pass
		
	def connect_mBackground_mImgTbl(self):
		b = self.mImgTbl[0]
		for a in self.mBackground:
			a['_outE'].append( {'_inV':b['task_id']} )
			b['_inE'].append( {'_outV':a['task_id']} )
		pass
		
	def connect_mImgTbl_mAdd(self):
		a = self.mImgTbl[0]
		b = self.mAdd[0]
		a['_outE'].append( {'_inV':b['task_id']} )
		b['_inE'].append( {'_outV':a['task_id']} )
		pass
		
	def connect_mAdd_mShrink(self):
		a = self.mAdd[0]
		b = self.mShrink[0]
		a['_outE'].append( {'_inV':b['task_id']} )
		b['_inE'].append( {'_outV':a['task_id']} )
		pass
		
	def connect_mShrink_mJPEG(self):
		a = self.mShrink[0]
		b = self.mJPEG[0]
		a['_outE'].append( {'_inV':b['task_id']} )
		b['_inE'].append( {'_outV':a['task_id']} )
		pass
		
	def generate(self, parallelism):
		id = 1
		
		# test node generating
		id = self.gen_mProjectPP(id, parallelism)
		id = self.gen_mDiffFit(id, parallelism)
		id = self.gen_mConcatFit(id)
		id = self.gen_mBgModel(id)
		id = self.gen_mBackground(id,parallelism)
		id = self.gen_mImgTbl(id)
		id = self.gen_mAdd(id)
		id = self.gen_mShrink(id)
		id = self.gen_mJPEG(id)
		
		print('Total task generated:', id-1)
		
		# task node connecting
		self.connect_mProjectPP_mDiffFit()
		self.connect_mDiffFit_mConcatFit()
		self.connect_mConcatFit_mBgModel()
		self.connect_mBgModel_mBackground()
		self.connect_mProjectPP_mBackground()
		self.connect_mBackground_mImgTbl()
		self.connect_mImgTbl_mAdd()
		self.connect_mAdd_mShrink()
		self.connect_mShrink_mJPEG()
		
		# workflow generating
		self.workflow = {'taskworkflow':[]}
		self.workflow['taskworkflow'] += self.mProjectPP
		self.workflow['taskworkflow'] += self.mDiffFit
		self.workflow['taskworkflow'] += self.mConcatFit
		self.workflow['taskworkflow'] += self.mBgModel
		self.workflow['taskworkflow'] += self.mBackground
		self.workflow['taskworkflow'] += self.mImgTbl
		self.workflow['taskworkflow'] += self.mAdd
		self.workflow['taskworkflow'] += self.mShrink
		self.workflow['taskworkflow'] += self.mJPEG
		
		json.dumps(self.workflow, indent=4)
		filename = 'workflow Montage {} tasks size {} parallelism {}.json'.format(id-1, self.task_size, parallelism)
		json.dump(self.workflow, open(filename, encoding='utf8', mode='w'), indent=4)		
		

class EpigenmoicsWorkflow:
	
	task_size = 5
	def gen_fastQSplit(self, id):
		task = {}
		task['taskname'] = tasknameGen('fastQSplit', id)
		task['program'] = 'DummyTask.jar'
		task['task_id'] = id
		task['command'] = self.task_size
		task['_outE'] = []
		task['_inE'] = []
		
		self.fastQSplit = [task]
		
		return id+1
	
	def connect_fastQSplit_filterContams(self):
		a = self.fastQSplit[0]
		for task in self.filterContams:
			a['_outE'].append( {'_inV':task['task_id']} )
			task['_inE'].append( {'_outV':a['task_id']} )
					
	def gen_filterContams(self, id, parallelism):
		tasks = []
		
		for i in range(parallelism):
			task = {}
			task['taskname'] = tasknameGen('filterContams', id)
			task['program'] = 'DummyTask.jar'
			task['task_id'] = id
			task['command'] = self.task_size
			task['_outE'] = []
			task['_inE'] = []
			
			tasks.append(task)
			id += 1
		
		self.filterContams = tasks
		return id
	
	def connect_filterContams_sol2sanger(self):
		for i in range( len(self.filterContams) ):
			a = self.filterContams[i]
			b = self.sol2sanger[i]
			a['_outE'].append( {'_inV':b['task_id']} )
			b['_inE'].append( {'_outV':a['task_id']} )
		
	def gen_sol2sanger(self, id, parallelism):
		tasks = []
		
		for i in range(parallelism):
			task = {}
			task['taskname'] = tasknameGen('sol2sanger', id)
			task['program'] = 'DummyTask.jar'
			task['task_id'] = id
			task['command'] = self.task_size
			task['_outE'] = []
			task['_inE'] = []
			
			tasks.append(task)
			id += 1
		
		self.sol2sanger = tasks
		return id
		
	def connect_sol2sanger_fastq2bfq(self):
		for i in range( len(self.sol2sanger) ):
			a = self.sol2sanger[i]
			b = self.fastq2bfq[i]
			a['_outE'].append( {'_inV':b['task_id']} )
			b['_inE'].append( {'_outV':a['task_id']} )	
		
	def gen_fastq2bfq(self, id, parallelism):
		tasks = []
		
		for i in range(parallelism):
			task = {}
			task['taskname'] = tasknameGen('fastq2bfq', id)
			task['program'] = 'DummyTask.jar'
			task['task_id'] = id
			task['command'] = self.task_size
			task['_outE'] = []
			task['_inE'] = []
			
			tasks.append(task)
			id += 1
		
		self.fastq2bfq = tasks
		return id
		
	def connect_fastq2bfq_map(self):
		for i in range( len(self.fastq2bfq) ):
			a = self.fastq2bfq[i]
			b = self.map[i]
			a['_outE'].append( {'_inV':b['task_id']} )
			b['_inE'].append( {'_outV':a['task_id']} )	
		
	def gen_map(self, id, parallelism):
		tasks = []
		
		for i in range(parallelism):
			task = {}
			task['taskname'] = tasknameGen('map', id)
			task['program'] = 'DummyTask.jar'
			task['task_id'] = id
			task['command'] = self.task_size
			task['_outE'] = []
			task['_inE'] = []
			
			tasks.append(task)
			id += 1
		
		self.map = tasks
		return id 
	
	def connect_map_mapMerge(self):
		b = self.mapMerge[0]
		for i in range( len(self.map) ):
			a = self.map[i]
			a['_outE'].append( {'_inV':b['task_id']} )
			b['_inE'].append( {'_outV':a['task_id']} )	
		
	def gen_mapMerge(self, id):
		task = {}
		task['taskname'] = tasknameGen('mapMerge', id)
		task['program'] = 'DummyTask.jar'
		task['task_id'] = id
		task['command'] = self.task_size
		task['_outE'] = []
		task['_inE'] = []
		
		self.mapMerge = [task]
		
		return id+1
		
	def connect_mapMerge_mapIndex(self):
		a = self.mapMerge[0]
		b = self.mapIndex[0]
		a['_outE'].append( {'_inV':b['task_id']} )
		b['_inE'].append( {'_outV':a['task_id']} )
	
	def gen_mapIndex(self, id):
		task = {}
		task['taskname'] = tasknameGen('mapIndex', id)
		task['program'] = 'DummyTask.jar'
		task['task_id'] = id
		task['command'] = self.task_size
		task['_outE'] = []
		task['_inE'] = []
		
		self.mapIndex = [task]
		
		return id+1
	
	def connect_mapIndex_pileup(self):
		a = self.mapIndex[0]
		b = self.pileup[0]
		a['_outE'].append( {'_inV':b['task_id']} )
		b['_inE'].append( {'_outV':a['task_id']} )
	
	def gen_pileup(self, id):
		task = {}
		task['taskname'] = tasknameGen('pileup', id)
		task['program'] = 'DummyTask.jar'
		task['task_id'] = id
		task['command'] = self.task_size
		task['_outE'] = []
		task['_inE'] = []
		
		self.pileup = [task]
		
		return id+1		
	
	def generate(self, num_parallel):
		id = 1
		
		# task node generating
		id = self.gen_fastQSplit(id)
		id = self.gen_filterContams(id, num_parallel)
		id = self.gen_sol2sanger(id, num_parallel)
		id = self.gen_fastq2bfq(id, num_parallel)
		id = self.gen_map(id, num_parallel)
		id = self.gen_mapMerge(id)
		id = self.gen_mapIndex(id)
		id = self.gen_pileup(id)
		
		print('Total task generated:', id-1)
		
		# task node connecting
		self.connect_fastQSplit_filterContams()
		self.connect_filterContams_sol2sanger()
		self.connect_sol2sanger_fastq2bfq()
		self.connect_fastq2bfq_map()
		self.connect_map_mapMerge()
		self.connect_mapMerge_mapIndex()
		self.connect_mapIndex_pileup()
		
		# workflow generating
		self.workflow = {'taskworkflow':[]}
		self.workflow['taskworkflow'] += self.fastQSplit
		self.workflow['taskworkflow'] += self.filterContams
		self.workflow['taskworkflow'] += self.sol2sanger
		self.workflow['taskworkflow'] += self.fastq2bfq
		self.workflow['taskworkflow'] += self.map
		self.workflow['taskworkflow'] += self.mapMerge
		self.workflow['taskworkflow'] += self.mapIndex
		self.workflow['taskworkflow'] += self.pileup
		
		json.dumps(self.workflow, indent=4)
		filename = 'workflow Epigenmoics {} tasks size {} parallelism {}.json'.format(id-1, self.task_size, num_parallel)
		json.dump(self.workflow, open(filename, encoding='utf8', mode='w'), indent=4)		
		
	
if __name__ == '__main__':
	main()