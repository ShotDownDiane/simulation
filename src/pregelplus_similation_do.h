#include "basic/pregel-dev.h"
#include <string>
#include <map>
#include <set>
#include <vector>
using namespace std;
typedef unsigned int UINT_32;
typedef unsigned char UINT_8;
#define ABSENT_ELEMENT -1
#define PRESENT_ELEMENT 1

struct Msg_pregel {
	VertexID id;
	UINT_32 simset;
	char label;
};

struct CCValue_pregel {
	char label;
	int id;
	int inDegree;
//	vector<Msg_pregel> inNeighbors;
	map<VertexID, Msg_pregel> inNeighbors;
	int outDegree;
//	vector<Msg_pregel> outNeighbors;
	map<VertexID, Msg_pregel> outNeighbors;

	vector<UINT_32> simsetStack;
};

ibinstream & operator<<(ibinstream & m, const Msg_pregel & msg) {
	m << msg.id;
	m << msg.simset;
}

obinstream & operator>>(obinstream & m, Msg_pregel & msg) {
	m >> msg.id;
	m >> msg.simset;
}

ibinstream & operator<<(ibinstream & m, const CCValue_pregel & v) {
	m << v.label;
	m << v.id;
	m << v.inDegree;
	m << v.inNeighbors;
	m << v.outDegree;
	m << v.outNeighbors;
	return m;
}

obinstream & operator>>(obinstream & m, CCValue_pregel & v) {
	m >> v.label;
	m >> v.id;
	m >> v.inDegree;
	m >> v.inNeighbors;
	m >> v.outDegree;
	m >> v.outNeighbors;
	return m;
}

bool o2oMatch(UINT_32 tobematch, vector<UINT_32> & simsets, vector<bool> & used,
		int depth = 1) {
	assert(simsets.size()==used.size());
	if (depth == 1) {
		int tobecount = 0;
		for (int i = 0; i < 32; i++)
			if (tobematch & 1 << i)
				tobecount++;
		int cancount = 0;
		UINT_32 canmatch = 0;
		for (int i = 0; i < simsets.size(); i++) {
			canmatch |= simsets[i];
			if (tobematch & simsets[i])
				cancount++;
		}
		if (cancount < tobecount) {
			return false;
		}
		if (tobematch & (~canmatch)) {
			return false;
		}
	}
	int i = 0;
	for (i = 0; i < 32; i++) {
		if (tobematch & 1 << i) {
			break;
		}
	}
	if (tobematch & 1 << i) { //fiind a match for i
		for (int j = 0; j < used.size(); j++) {
			if ((!used[j]) && ((1 << i) & simsets[j])) {
				used[j] = true;
				bool matched = o2oMatch(tobematch & (~((UINT_32) 1 << i)),
						simsets, used, depth + 1);
				used[j] = false;
				if (matched)
					return true;
			}
		}
		return false;
	} else {
		return true;
	}
}
class CCVertex_pregel: public Vertex<VertexID, CCValue_pregel, Msg_pregel> {
public:
	void broadcast(UINT_32 simset) {
		Msg_pregel msg;
		msg.id = value().id;
		msg.simset = simset;

		map<VertexID,Msg_pregel> & onbs = value().outNeighbors;
		for(map<VertexID,Msg_pregel>::iterator it=onbs.begin();it!=onbs.end();it++){
			send_message(it->first,msg);
		}
		msg.simset = msg.simset | 1 << 31;
		map<VertexID,Msg_pregel> & inbs = value().inNeighbors;
		for(map<VertexID,Msg_pregel>::iterator it=inbs.begin();it!=inbs.end();it++){
			send_message(it->first,msg);
		}

	}

	void Vnormalcompute(MessageContainer & messages) {
		vector<UINT_32> &simsetStack = value().simsetStack;
		if (mutated) {
			//first, simcountStack and simsetStack
			simsetStack.resize(edges.size());
			if (simsetStack.size() > 1) {
				simsetStack[simsetStack.size() - 1] =
						simsetStack[simsetStack.size() - 2];
			}

			UINT_32 & simset = simsetStack[simsetStack.size() - 1];

			vector<int> & partialSupp = partialSuppStack[partialSuppStack.size()
					- 1];

//			update the in and out neighbors' simsets only according to the label of the neighbors
			if (gspanMsg.fromlabel != -1) {
				for (map<VertexID, Msg_pregel>::iterator it =
						value().outNeighbors.begin();
						it != value().outNeighbors.end(); it++) {
					if (it->second.label == gspanMsg.fromlabel) {
						it->second.simset |= 1 << gspanMsg.fromid;
					} else {
						it->second.simset &= ~(1 << gspanMsg.fromid);
					}
				}
				for (map<VertexID, Msg_pregel>::iterator it =
						value().inNeighbors.begin();
						it != value().inNeighbors.end(); it++) {
					if (it->second.label == gspanMsg.fromlabel) {
						it->second.simset |= 1 << gspanMsg.fromid;
					} else {
						it->second.simset &= ~(1 << gspanMsg.fromid);
					}
				}
			}

			if (gspanMsg.tolabel != -1) {
				for (map<VertexID, Msg_pregel>::iterator it =
						value().outNeighbors.begin();
						it != value().outNeighbors.end(); it++) {
					if (it->second.label == gspanMsg.tolabel) {
						it->second.simset |= 1 << gspanMsg.toid;
					} else {
						it->second.simset &= ~(1 << gspanMsg.toid);
					}
				}
				for (map<VertexID, Msg_pregel>::iterator it =
						value().inNeighbors.begin();
						it != value().inNeighbors.end(); it++) {
					if (it->second.label == gspanMsg.tolabel) {
						it->second.simset |= 1 << gspanMsg.toid;
					} else {
						it->second.simset &= ~(1 << gspanMsg.toid);
					}
				}
			}

			//initial simset and increment the partialSupport
			if (gspanMsg.fromlabel != -1) {
				if (value().label == gspanMsg.fromlabel) {
					simset |= 1 << gspanMsg.fromid;
					partialSupp[gspanMsg.fromid]++;
				} else {
					simset &= ~(1 << gspanMsg.fromid);
				}
			}
			if (gspanMsg.tolabel != -1) {
				if (value().label == gspanMsg.tolabel) {
					simset |= 1 << gspanMsg.toid;
					partialSupp[gspanMsg.toid]++;
				} else {
					simset &= ~(1 < gspanMsg.toid);
				}
			}

			// setup the bitmap_msg
			bool updated = false;
			for (int i = 0; i < q.labels.size(); i++) {
				if (i != gspanMsg.fromid && i != gspanMsg.toid) //only vertex of new added edge is needed
					continue;

				if (!(simset & 1 << i)) // not match to this vertex, continue;
					continue;

				//testing if this vertex can match i any more.
				bool match = true;
				set<char> qnlabels;

				for (int j = 0; j < q.inEdges[i].size(); j++) {
					char l = q.labels[q.inEdges[i][j]];
					if (qnlabels.find(l) == qnlabels.end()) {
						qnlabels.insert(l);
					}
				}

				for (int j = 0; j < q.outEdges[i].size(); j++) {
					char l = q.labels[q.outEdges[i][j]];
					if (qnlabels.find(l) == qnlabels.end()) {
						qnlabels.insert(l);
					}
				}

				//for now, seperate the inNeighbor match and outNeighbor match
				for (set<char>::iterator it = qnlabels.begin();
						it != qnlabels.end(); it++) {
					//for each label, set up tobematch
					UINT_32 tobematch = 0;
					for (int j = 0; j < q.inEdges[i].size(); j++) {
						if (*it == q.labels[q.inEdges[i][j]]) {
							tobematch |= 1 << q.inEdges[i][j];
						}
					}
					//set up simsets and used vector
					vector<UINT_32> simsets;
					for (map<VertexID, Msg_pregel>::iterator it =
							value().outNeighbors.begin();
							it != value().outNeighbors.end(); it++) {
						if (it->second.simset & tobematch) {
							simsets.push_back(it->second.simset);
						}
					}
					vector<bool> used;
					used.resize(simsets.size(), false);
					bool matchin = o2oMatch(tobematch, simsets, used);

					if (!matchin) {
						match = false;
						break;
					}

					//set up tobematch
					tobematch = 0;
					for (int j = 0; j < q.outEdges[i].size(); j++) {
						if (*it == q.labels[q.outEdges[i][j]]) {
							tobematch |= 1 << q.outEdges[i][j];
						}
					}

					//set up simsets and used vector
					simsets.clear();
					for (map<VertexID, Msg_pregel>::iterator it =
							value().inNeighbors.begin();
							it != value().inNeighbors.end(); it++) {
						if (it->second.simset & tobematch) {
							simsets.push_back(it->second.simset);
						}
					}

					used.resize(0);
					used.resize(simsets.size(), false);
					bool matchout = o2oMatch(tobematch, simsets, used);
					if (!matchout) {
						match = false;
						break;
					}
				}

				if (!match) { //no match, update the simset
					simset &= ~(1 << i);
					partialSupp[i]--;
					updated = true;
				}
			}

			if (updated) {
				broadcast(simset);
			}

			vote_to_halt();
		} else {

			//alias here
			UINT_32 & simset = simsetStack[simsetStack.size() - 1];

			vector<int> &partialSupp = partialSuppStack[partialSuppStack.size()
					- 1];
			/*
			 * update the neighbors simset
			 * according to the recieved messages
			 */

			bool inNeighborUpdated = false;
			bool outNeighborUpdated = false;

			for (int i = 0; i < messages.size(); i++) {
				if (messages[i].simset & 1 << 31) {
					outNeighborUpdated = true;
					value().outNeighbors[messages[i].id] = messages[i];
				} else {
					inNeighborUpdated = true;
					value().inNeighbors[messages[i].id] = messages[i];
				}
			}
			/*
			 * update the simset and setup the message
			 * update the partialSupp if simset is updated
			 */

			// setup the bitmap_msg
			bool updated = false;
			for (int i = 0; i < q.labels.size(); i++) {

				if (!(simset & 1 << i)) // not match to this vertex, continue;
					continue;

				//testing if this vertex can match i any more.
				bool match = true;
				set<char> qnlabels;

				for (int j = 0; j < q.inEdges[i].size(); j++) {
					char l = q.labels[q.inEdges[i][j]];
					if (qnlabels.find(l) == qnlabels.end()) {
						qnlabels.insert(l);
					}
				}

				for (int j = 0; j < q.outEdges[i].size(); j++) {
					char l = q.labels[q.outEdges[i][j]];
					if (qnlabels.find(l) == qnlabels.end()) {
						qnlabels.insert(l);
					}
				}

				//for now, seperate the inNeighbor match and outNeighbor match
				for (set<char>::iterator it = qnlabels.begin();
						it != qnlabels.end(); it++) {
					UINT_32 tobematch = 0;
					vector<UINT_32> simsets;
					vector<bool> used;
					if (outNeighborUpdated) {
						//for each label, set up tobematch
						for (int j = 0; j < q.inEdges[i].size(); j++) {
							if (*it == q.labels[q.inEdges[i][j]]) {
								tobematch |= 1 << q.inEdges[i][j];
							}
						}
						//set up simsets and used vector
						for (map<VertexID, Msg_pregel>::iterator it =
								value().outNeighbors.begin();
								it != value().outNeighbors.end(); it++) {
							if (it->second.simset & tobematch) {
								simsets.push_back(it->second.simset);
							}
						}
						used.resize(simsets.size(), false);
						bool matchin = o2oMatch(tobematch, simsets, used);

						if (!matchin) {
							match = false;
							break;
						}
					}

					if (inNeighborUpdated) {
						//set up tobematch
						tobematch = 0;
						for (int j = 0; j < q.outEdges[i].size(); j++) {
							if (*it == q.labels[q.outEdges[i][j]]) {
								tobematch |= 1 << q.outEdges[i][j];
							}
						}

						//set up simsets and used vector
						simsets.clear();
						for (map<VertexID, Msg_pregel>::iterator it =
								value().inNeighbors.begin();
								it != value().inNeighbors.end(); it++) {
							if (it->second.simset & tobematch) {
								simsets.push_back(it->second.simset);
							}
						}

						used.resize(0);
						used.resize(simsets.size(), false);
						bool matchout = o2oMatch(tobematch, simsets, used);
						if (!matchout) {
							match = false;
							break;
						}
					}
				}

				if (!match) { //no match, update the simset
					simset &= ~(1 << i);
					partialSupp[i]--;
					updated = true;
				}
			}

			if (updated) {
				broadcast(simset);
			}

			vote_to_halt();
		}
	}

	void Vpreprocessing(MessageContainer & messages) {
		if (preprocessSuperstep == 1) {
			map<VertexID, Msg_pregel> & onbs = value().outNeighbors;
			Msg_pregel msg;
			msg.id = value().id;
			msg.simset = value().label;

			for (map<VertexID, Msg_pregel>::iterator it = onbs.begin();
					it != onbs.end(); it++) {
				send_message(it->first, it->second);
			}

			map<VertexID, Msg_pregel> & inbs = value().inNeighbors;
			msg.simset |= 1 << 31;
			for (map<VertexID, Msg_pregel>::iterator it = inbs.begin();
					it != inbs.end(); it++) {
				send_message(it->first, it->second);
			}

		} else if (preprocessSuperstep == 2) {
			//summarize the edge frequency in this partition
			for (MessageContainer::iterator it = messages.begin();
					it != messages.end(); ++it) {
				if (it->simset & 1 << 31) { //from outNeighbors
					it->simset &= ~(1 << 31);
					value().outNeighbors[it->id].label = it->simset;
				} else { //from inNeighbors
					value().inNeighbors[it->id].label = it->simset;
					edgeFrequent[value().label][it->simset]++;
				}
			}
		}
		vote_to_halt();
	}

	virtual void compute(MessageContainer & messages) {
		if (phase == preprocessing) {
			Vpreprocessing(messages);
		} else if (phase == normalcomputing) {
			Vnormalcompute(messages);
		}
	}
};
//=============================Aggregator==============================================================================
struct SimulationPartial {
	vector<int> matchcount;
};
struct SimulationFinal {
	vector<int> matchcount;
};
ibinstream & operator<<(ibinstream & m, const SimulationPartial & v) {
	m << v.matchcount;
	return m;
}
obinstream & operator>>(obinstream & m, SimulationPartial & v) {
	m >> v.matchcount;
	return m;
}
ibinstream & operator<<(ibinstream & m, const SimulationFinal & v) {
	m << v.matchcount;
	return m;
}
obinstream & operator>>(obinstream & m, SimulationFinal & v) {
	m >> v.matchcount;
	return m;
}

/* each worker holds an Aggregator
 */
class CCAggregator_pregel: public Aggregator<CCVertex_pregel, SimulationPartial,
		SimulationFinal> {
public:
	//initialized at each worker before each superstep
	virtual void init() {
	}
	//aggregate each computed vertex (after vertex compute)
	virtual void stepPartial(CCVertex_pregel* v) {
	}
	//call when sync_agg by each worker (not master)
	//the returned value is gathered by the master to aggregate all the partial values
	virtual SimulationPartial* finishPartial() {
		sum.matchcount = partialSuppStack[partialSuppStack.size() - 1]; //deep copy?
		return &sum;
	}
	//only called by the master, to agg worker_partial, each worker(not master) once
	virtual void stepFinal(SimulationPartial* part) {
		for (int i = 0; i < sum.matchcount.size(); i++)
			sum.matchcount[i] += part->matchcount[i];
	}
	//called by the master before broadcast aggregated value to workers,
	//the final aggregated value can be accessed by each worker in next super_step with: void* getAgg()
	virtual SimulationFinal* finishFinal() {
		supp = sum.matchcount[0]; //supp value of this round. can be broadcast or for other use!
		cout << "supVector:";
		for (int i = 0; i < sum.matchcount.size(); i++) {
			cout << sum.matchcount[i] << " ";
			supp = supp <= sum.matchcount[i] ? supp : sum.matchcount[i];
		}
		cout << endl;
		SimulationFinal * finalsum = (SimulationFinal*) getAgg();
		return finalsum;
	}
private:
	SimulationPartial sum;
};
//=====================================================================
class CCWorker_pregel: public Worker<CCVertex_pregel, CCAggregator_pregel> {
	char buf[100];

public:
	virtual CCVertex_pregel* toVertex(char* line) {
		/*
		 * format of the input graph:
		 * srcID	label outDegree N1 N2.... inDegree
		 */
		char * pch;
		pch = strtok(line, "\t"); //srcID
		CCVertex_pregel* v = new CCVertex_pregel;
		v->id = atoi(pch);
		v->value().id = v->id;
		pch = strtok(NULL, " "); //label
		char* label = pch;
#ifdef little
		v->value().label = label[0];
#else
		v->value().label = atoi(label);
#endif
		pch = strtok(NULL, " "); //outDegree
		v->value().outDegree = atoi(pch);
		Msg_pregel nb;
		for (int i = 0; i < v->value().outDegree; i++) {
			pch = strtok(NULL, " "); //neighbor
			nb.id = atoi(pch);
			v->value().outNeighbors[nb.id] = nb;
		}
		pch = strtok(NULL, " "); //inDegree
		v->value().inDegree = atoi(pch);
		for (int i = 0; i < v->value().inDegree; i++) {
			pch = strtok(NULL, " ");
			nb.id = atoi(pch);
			v->value().inNeighbors[nb.id] = nb;
		}
		return v;
	}

	virtual void toline(CCVertex_pregel* v, BufferedWriter & writer) {
//		sprintf(buf, "vid:%d\t can_similate:%d \n", v->value().id,
//				v->value().simset[0]);
		writer.write(buf);
	}
};
//=============================use no combiner==============================
class CCCombiner_pregel: public Combiner<Msg_pregel> {
public:
	virtual void combine(Msg_pregel & old, const Msg_pregel & new_msg) {
	}
};

void mine() {
	Worker<CCVertex_pregel, CCAggregator_pregel> * w = (Worker<CCVertex_pregel,
			CCAggregator_pregel>*) workercontext;
	w->looponsim();
}
void pregel_similation(string in_path, string out_path, bool use_combiner) {
//	init();
	WorkerParams param;
	param.input_path = in_path;
	param.output_path = out_path;
	param.force_write = true;
	param.native_dispatcher = false;
	CCWorker_pregel worker;
	workercontext = &worker;
	CCCombiner_pregel combiner;
	if (use_combiner)
		worker.setCombiner(&combiner);
	CCAggregator_pregel SimulationAggregator;
	worker.setAggregator(&SimulationAggregator);
	worker.run(param);
}
