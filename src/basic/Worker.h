#ifndef WORKER_H
#define WORKER_H

#include <vector>
#include <map>
#include "../utils/global.h"
#include "MessageBuffer.h"
#include <string>
#include "../utils/communication.h"
#include "../utils/ydhdfs.h"
#include "../utils/Combiner.h"
#include "../utils/Aggregator.h"
#include "../gSpan/gspan.h"
#include "SelfSimulation.h"
using namespace std;
//------------------------------------------------------------------------------------------------------------------------------------
enum Phase {
	preprocessing = 1, normalcomputing = 2, postloopsim=3
};
Phase phase = preprocessing;
//=================preprocessing=====================
map<int, map<int, int> > edgeFrequent;
map<int, map<int, int> > src_edgeFrequent;
map<int, map<int, int> > dst_edgeFrequent;
int preprocessSuperstep = 0;
vector<char> labelset_global;
int labelsetsize = 0;
int minsup = 0;
//=================Query graph=========================
struct Query_Graph {
	vector<char> labels;
	vector<vector<VertexID> > outEdges;
	vector<vector<VertexID> > inEdges;
};

struct Simmsg {
	int size;
	int fromid;
	char fromlabel;
	int toid;
	char tolabel;
};

ibinstream & operator<<(ibinstream & m, const Simmsg & msg) {
	m << msg.size << msg.fromid << msg.fromlabel << msg.toid << msg.tolabel;
	return m;
}
obinstream & operator>>(obinstream & m, Simmsg & msg) {
	m >> msg.size >> msg.fromid >> msg.fromlabel >> msg.toid >> msg.tolabel;
	return m;
}
void mine();

void * workercontext = 0;
Query_Graph q;
vector<Simmsg> edges;
vector<vector<int> > leastprematchcounts;
vector<vector<int> > leastsucmatchcounts;
Simmsg gspanMsg;
bool mutated = false;
//=================support metric====================
//vector<int> partialSupp;
vector<vector<int> > partialSuppStack;
int supp;

void processgspanMsg() {
	edges.resize(gspanMsg.size);
	edges[gspanMsg.size - 1] = gspanMsg;
	//construct graph
	q.labels.resize(std::max(gspanMsg.fromid, gspanMsg.toid) + 1);
	q.outEdges.clear();
	q.outEdges.resize(q.labels.size());
	for (int i = 0; i < edges.size(); ++i) {
		Simmsg &e = edges[i];
		q.labels[e.fromid] = e.fromlabel;
		q.labels[e.toid] = e.tolabel;
		q.outEdges[e.fromid].push_back(e.toid);
	}
	q.inEdges.clear();
	q.inEdges.resize(q.outEdges.size());
	for (int i = 0; i < q.outEdges.size(); i++) {
		for (int j = 0; j < q.outEdges[i].size(); j++) {
			q.inEdges[q.outEdges[i][j]].push_back(i);
		}
	}

	//add constraint to the children numbers of specific label, the children number must noless than the pattern
	vector<vector<vector<int> > > leastmatchcounts = leastDualMatchCount(
			q.labels, q.outEdges, q.inEdges);
	leastprematchcounts = leastmatchcounts[0];
	leastsucmatchcounts = leastmatchcounts[1];

	//used to tell the new add vertexes.
	if (edges.size() > 1) {
		Simmsg &e = edges[edges.size() - 2];
		if (gspanMsg.fromid <= std::max(e.fromid, e.toid)) {
			gspanMsg.fromlabel = -1;
		}
		if (gspanMsg.toid <= std::max(e.fromid, e.toid)) {
			gspanMsg.tolabel = -1;
		}
	}

	//process partialSuppStack
	partialSuppStack.resize(edges.size());
	if (partialSuppStack.size() > 1) {
		partialSuppStack[partialSuppStack.size() - 1] =
				partialSuppStack[partialSuppStack.size() - 2];
	}
	partialSuppStack[partialSuppStack.size() - 1].resize(q.labels.size(), 0);
	if (gspanMsg.fromlabel != -1)
		partialSuppStack[partialSuppStack.size() - 1][gspanMsg.fromid] = 0;
	if (gspanMsg.tolabel != -1)
		partialSuppStack[partialSuppStack.size() - 1][gspanMsg.toid] = 0;
}
int curSupp() {
	return supp;
}

//===========frequent extension constrain==================
//maintained by gspanImpl, tell which vertexes is on rmpath
vector<int> RMVertexes;

//variables distributed on each worker
map<int, map<int, int> > ext_e_src_freq;//srcid -> dstid -> freq
map<int, map<int, int> > ext_e_dst_freq;//srcid -> dstid -> freq
map<int, map<char, map<char, int> > > ext_v_src_freq;//RMpathvertexid -> lalbel -> src(direction) -> freq
map<int, map<char, map<char, int> > > ext_v_dst_freq;//RMpathvertexid -> lalbel -> src(direction) -> freq

//variables maintained by master
vector<map<int, map<int, int> > > ext_e_freq_stack; //stack -> srcid -> dstid -> freq
vector<map<int, map<char, map<char, int> > > > ext_v_freq_stack; //stack -> RMpathvertexid -> lalbel -> src(direction) -> freq
void postsiminit(){
	ext_e_src_freq.clear();
	ext_e_dst_freq.clear();
	ext_v_src_freq.clear();
	ext_v_dst_freq.clear();
	//exchange RMVertexes
	if(get_worker_id()==MASTER_RANK){
		masterBcast(RMVertexes);
	}else{
		slaveBcast(RMVertexes);
	}

	for(int i=0;i<RMVertexes.size();i++){
		for(int j=0;j<RMVertexes.size();j++){
			ext_e_src_freq[RMVertexes[i]][RMVertexes[j]]=0;
			ext_e_dst_freq[RMVertexes[i]][RMVertexes[j]]=0;
		}
	}
	for(int i=0;i<RMVertexes.size();i++){
		for(int j=0;j<labelset_global.size();j++){
			ext_v_src_freq[RMVertexes[i]][labelset_global[j]]['l']=0;
			ext_v_src_freq[RMVertexes[i]][labelset_global[j]]['r']=0;
			ext_v_dst_freq[RMVertexes[i]][labelset_global[j]]['l']=0;
			ext_v_dst_freq[RMVertexes[i]][labelset_global[j]]['r']=0;
		}
	}
}
//executed after each super step
void postsimmessageprocess() {
	const int mpi_msg_size=RMVertexes.size()*RMVertexes.size()*2+RMVertexes.size()*labelsetsize*2*2;
	int send[mpi_msg_size];
	int recv[mpi_msg_size];

	for(int i=0;i<mpi_msg_size;i++){
		send[i]=0;
		recv[i]=0;
	}
	//prepare ext_e_src_freq to be send
	int offset=0;
	for(int i=0;i<RMVertexes.size();i++){
		if(ext_e_src_freq.find(RMVertexes[i])==ext_e_src_freq.end())continue;

		for(int j=0;j<RMVertexes.size();j++){
			if(i==j)continue;
			if(ext_e_src_freq[RMVertexes[i]].find(RMVertexes[j])==ext_e_src_freq[RMVertexes[i]].end())continue;

			send[offset+RMVertexes.size()*i+j]=ext_e_src_freq[RMVertexes[i]][RMVertexes[j]];
		}
	}

	//prepare ext_e_dst_freq to be send
	offset=RMVertexes.size()*RMVertexes.size();
	for(int i=0;i<RMVertexes.size();i++){
		if(ext_e_dst_freq.find(RMVertexes[i])==ext_e_dst_freq.end())continue;

		for(int j=0;j<RMVertexes.size();j++){
			if(i==j)continue;
			if(ext_e_dst_freq[RMVertexes[i]].find(RMVertexes[j])==ext_e_dst_freq[RMVertexes[i]].end())continue;

			send[offset+RMVertexes.size()*i+j]=ext_e_dst_freq[RMVertexes[i]][RMVertexes[j]];
		}
	}
	//prepare ext_v_src_freq
	vector<char> src_pos;
	src_pos.push_back('l');
	src_pos.push_back('r');
	offset=RMVertexes.size()*RMVertexes.size()*2;
	for(int i=0;i<RMVertexes.size();i++){
		if(ext_v_src_freq.find(RMVertexes[i])==ext_v_src_freq.end())continue;
		for(int j=0;j<labelset_global.size();j++){
			if(ext_v_src_freq[RMVertexes[i]].find(labelset_global[j])==ext_v_src_freq[RMVertexes[i]].end())continue;
			for(int k=0;k<src_pos.size();k++){
				if(ext_v_src_freq[RMVertexes[i]][labelset_global[j]].find(src_pos[k])==ext_v_src_freq[RMVertexes[i]][labelset_global[j]].end())continue;

				send[offset+src_pos.size()*labelset_global.size()*i+src_pos.size()*j+k]=ext_v_src_freq[RMVertexes[i]][labelset_global[j]][src_pos[k]];
			}
		}
	}
	//prepare ext_v_dst_freq
	offset=RMVertexes.size()*RMVertexes.size()*2+RMVertexes.size()*labelset_global.size()*src_pos.size();
	for(int i=0;i<RMVertexes.size();i++){
		if(ext_v_dst_freq.find(RMVertexes[i])==ext_v_dst_freq.end())continue;
		for(int j=0;j<labelset_global.size();j++){
			if(ext_v_dst_freq[RMVertexes[i]].find(labelset_global[j])==ext_v_dst_freq[RMVertexes[i]].end())continue;
			for(int k=0;k<src_pos.size();k++){
				if(ext_v_dst_freq[RMVertexes[i]][labelset_global[j]].find(src_pos[k])==ext_v_dst_freq[RMVertexes[i]][labelset_global[j]].end())continue;

				send[offset+src_pos.size()*labelset_global.size()*i+src_pos.size()*j+k]=ext_v_dst_freq[RMVertexes[i]][labelset_global[j]][src_pos[k]];
			}
		}
	}

	MPI_Reduce(send, recv, mpi_msg_size,MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);

	if(get_worker_id()==MASTER_RANK){
		ext_e_freq_stack.resize(edges.size());
		ext_v_freq_stack.resize(edges.size());
		map<int, map<int, int> > & ext_e_freq=ext_e_freq_stack[ext_e_freq_stack.size()-1];
		map<int, map<char, map<char, int> > > & ext_v_freq=ext_v_freq_stack[ext_v_freq_stack.size()-1];

		//ext_e_freq
		int offset1=0;
		int offset2=RMVertexes.size()*RMVertexes.size();
		for(int i=0;i<RMVertexes.size();i++){
			for(int j=0;j<RMVertexes.size();j++){
				ext_e_freq[RMVertexes[i]][RMVertexes[j]]=min(recv[offset1+RMVertexes.size()*i+j],recv[offset2+RMVertexes.size()*i+j]);
			}
		}
		//ext_v_freq
		offset1=RMVertexes.size()*RMVertexes.size()*2;
		offset2=RMVertexes.size()*RMVertexes.size()*2+RMVertexes.size()*labelsetsize*2;
		for(int i=0;i<RMVertexes.size();i++){
			for(int j=0;j<labelset_global.size();j++){
				for(int k=0;k<src_pos.size();k++){
					ext_v_freq[RMVertexes[i]][labelset_global[j]][src_pos[k]]=
						min(
							recv[offset1+src_pos.size()*labelset_global.size()*i+src_pos.size()*j+k],
							recv[offset2+src_pos.size()*labelset_global.size()*i+src_pos.size()*j+k]
						);
				}
			}
		}

		{//debugzjh
			for(map<int,map<int,int> >::iterator it=ext_e_freq.begin();it!=ext_e_freq.end();it++){
			}
		}

	}
}

//----------------------------------------------------------------------------------------------------------------------------------------
template<class VertexT, class AggregatorT = DummyAgg> //user-defined VertexT
class Worker {
	typedef vector<VertexT*> VertexContainer;
	typedef typename VertexContainer::iterator VertexIter;

	typedef typename VertexT::KeyType KeyT;
	typedef typename VertexT::MessageType MessageT;
	typedef typename VertexT::HashType HashT;

	typedef MessageBuffer<VertexT> MessageBufT;
	typedef typename MessageBufT::MessageContainerT MessageContainerT;
	typedef typename MessageBufT::Map Map;
	typedef typename MessageBufT::MapIter MapIter;

	typedef typename AggregatorT::PartialType PartialT;
	typedef typename AggregatorT::FinalType FinalT;

public:
	Worker() {
		//init_workers();//put to run.cpp
		message_buffer = new MessageBuffer<VertexT>;
		global_message_buffer = message_buffer;
		active_count = 0;
		combiner = NULL;
		global_combiner = NULL;
		aggregator = NULL;
		global_aggregator = NULL;
		global_agg = NULL;
	}

	void setCombiner(Combiner<MessageT>* cb) {
		combiner = cb;
		global_combiner = cb;
	}

	void setAggregator(AggregatorT* ag) {
		aggregator = ag;
		global_aggregator = ag;
		global_agg = new FinalT;
	}

	virtual ~Worker() {
		for (int i = 0; i < vertexes.size(); i++)
			delete vertexes[i];
		delete message_buffer;
		if (getAgg() != NULL)
			delete (FinalT*) global_agg;
		//worker_finalize();//put to run.cpp
		worker_barrier(); //newly added for ease of multi-job programming in run.cpp
	}

	//==================================
	//sub-functions
	void sync_graph() {
		//ResetTimer(4);
		//set send buffer
		vector<VertexContainer> _loaded_parts(_num_workers);
		for (int i = 0; i < vertexes.size(); i++) {
			VertexT* v = vertexes[i];
			_loaded_parts[hash(v->id)].push_back(v);
		}
		//exchange vertices to add
		all_to_all(_loaded_parts);

		//delete sent vertices
		for (int i = 0; i < vertexes.size(); i++) {
			VertexT* v = vertexes[i];
			if (hash(v->id) != _my_rank)
				delete v;
		}
		vertexes.clear();
		//collect vertices to add
		for (int i = 0; i < _num_workers; i++) {
			vertexes.insert(vertexes.end(), _loaded_parts[i].begin(),
					_loaded_parts[i].end());
		}
		_loaded_parts.clear();
		//StopTimer(4);
		//PrintTimer("Reduce Time",4);
	}
	;

	/*
	 * compute those vertex who is active or activated by msg.
	 */
	void active_compute() {
		active_count = 0;
		MessageBufT* mbuf = (MessageBufT*) get_message_buffer();
		vector<MessageContainerT>& v_msgbufs = mbuf->get_v_msg_bufs();
		for (int i = 0; i < vertexes.size(); i++) {
			if (v_msgbufs[i].size() == 0) {
				if (vertexes[i]->is_active()) {
					vertexes[i]->compute(v_msgbufs[i]);
					AggregatorT* agg = (AggregatorT*) get_aggregator();
					if (agg != NULL)
						agg->stepPartial(vertexes[i]);
					if (vertexes[i]->is_active())
						active_count++;
				}
			} else {
				vertexes[i]->activate();
				vertexes[i]->compute(v_msgbufs[i]);
				v_msgbufs[i].clear(); //clear used msgs
				AggregatorT* agg = (AggregatorT*) get_aggregator();
				if (agg != NULL)
					agg->stepPartial(vertexes[i]);
				if (vertexes[i]->is_active())
					active_count++;
			}
		}
	}

	/*
	 * compute all vertex no matter if it is active.
	 */
	void all_compute() {
		active_count = 0;
		MessageBufT* mbuf = (MessageBufT*) get_message_buffer();
		vector<MessageContainerT>& v_msgbufs = mbuf->get_v_msg_bufs();
		for (int i = 0; i < vertexes.size(); i++) {
			vertexes[i]->activate();
			vertexes[i]->compute(v_msgbufs[i]);
			v_msgbufs[i].clear(); //clear used msgs
			AggregatorT* agg = (AggregatorT*) get_aggregator();
			if (agg != NULL)
				agg->stepPartial(vertexes[i]);
			if (vertexes[i]->is_active())
				active_count++;
		}
	}

	inline void add_vertex(VertexT* vertex) {
		vertexes.push_back(vertex);
		if (vertex->is_active())
			active_count++;
	}

	void agg_sync() {
		AggregatorT* agg = (AggregatorT*) get_aggregator();
		if (agg != NULL) {
			if (_my_rank != MASTER_RANK) { //send partialT to aggregator
				//gathering PartialT
				PartialT* part = agg->finishPartial();
				//------------------------ strategy choosing BEGIN ------------------------
				StartTimer(COMMUNICATION_TIMER);
				StartTimer(SERIALIZATION_TIMER);
				ibinstream m;
				m << part;
				int sendcount = m.size();
				StopTimer(SERIALIZATION_TIMER);
				int total = all_sum(sendcount);
				StopTimer(COMMUNICATION_TIMER);
				//------------------------ strategy choosing END ------------------------
				if (total <= AGGSWITCH)
					slaveGather(*part);
				else {
					send_ibinstream(m, MASTER_RANK);
				}
				//scattering FinalT
				slaveBcast(*((FinalT*) global_agg));
			} else {
				agg->finishPartial();
				/*
				 * two strategy according to the size of partial result:
				 * 1,if the size<=AGGSWITCH, gather all at once then reduce the result
				 * 2,else gather one partial result and aggregate it one by one to save space.
				 */
				//------------------------ strategy choosing BEGIN ------------------------
				int total = all_sum(0);
				//------------------------ strategy choosing END ------------------------
				//gathering PartialT
				if (total <= AGGSWITCH) {
					vector<PartialT*> parts(_num_workers);
					masterGather(parts);
					for (int i = 0; i < _num_workers; i++) {
						if (i != MASTER_RANK) {
							PartialT* part = parts[i];
							agg->stepFinal(part);
							delete part;
						}
					}
				} else {
					for (int i = 0; i < _num_workers; i++) {
						if (i != MASTER_RANK) {
							obinstream um = recv_obinstream(i);
							PartialT* part;
							um >> part;
							agg->stepFinal(part);
							delete part;
						}
					}
				}
				//scattering FinalT
				FinalT* final = agg->finishFinal();
				//cannot set "global_agg=final" since MASTER_RANK works as a slave, and agg->finishFinal() may change
				*((FinalT*) global_agg) = *final; //deep copy
				masterBcast(*((FinalT*) global_agg));
			}
		}
	}

	//user-defined graphLoader ==============================
	virtual VertexT* toVertex(char* line) = 0; //this is what user specifies!!!!!!

	void load_vertex(VertexT* v) { //called by load_graph
		add_vertex(v);
	}

	void load_graph(const char* inpath) {
		hdfsFS fs = getHdfsFS();
		hdfsFile in = getRHandle(inpath, fs);
		LineReader reader(fs, in);
		while (true) {
			reader.readLine();
			if (!reader.eof())
				load_vertex(toVertex(reader.getLine()));
			else
				break;
		}
		hdfsCloseFile(fs, in);
		hdfsDisconnect(fs);
		//cout<<"Worker "<<_my_rank<<": \""<<inpath<<"\" loaded"<<endl;//DEBUG !!!!!!!!!!
	}
	//=======================================================

	//user-defined graphDumper ==============================
	virtual void toline(VertexT* v, BufferedWriter& writer) = 0; //this is what user specifies!!!!!!

	void preprocess() {
		if (get_worker_id() == MASTER_RANK)
			ST("Enter Preprocess\n");

		//distribute label set;
		if(get_worker_id()==MASTER_RANK){
			masterBcast(labelset_global);
		}else{
			slaveBcast(labelset_global);
		}
		labelsetsize=labelset_global.size();

		if (get_worker_id() == MASTER_RANK)
			setBit(WAKE_ALL_ORBIT);
		preprocessSuperstep = 0;
		while (true) {
			preprocessSuperstep++;

			//===================
			char bits_bor = all_bor(global_bor_bitmap);
			if (getBit(FORCE_TERMINATE_ORBIT, bits_bor) == 1)
				break;
			get_vnum() = all_sum(vertexes.size());
			int wakeAll = getBit(WAKE_ALL_ORBIT, bits_bor);
			if (wakeAll == 0) {
				active_vnum() = all_sum(active_count);
				if (active_vnum() == 0
						&& getBit(HAS_MSG_ORBIT, bits_bor) == 0) {
					break; //all_halt AND no_msg
				}
			} else
				active_vnum() = get_vnum();
			clearBits();
			if (wakeAll == 1) {
				all_compute();
			} else {
				active_compute();
			}
			message_buffer->combine();
			master_sum_LL(message_buffer->get_total_msg());
			master_sum_LL(message_buffer->get_total_vadd());
			vector<VertexT*>& to_add = message_buffer->sync_messages();
			for (int i = 0; i < to_add.size(); i++)
				add_vertex(to_add[i]);
			to_add.clear();
			//===================
			worker_barrier();
			// preprocess step
			if (preprocessSuperstep == 2) {
				setBit(WAKE_ALL_ORBIT);
				int edgefrequentarray[labelsetsize * labelsetsize];
				int result[labelsetsize * labelsetsize];
				//>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
				for (int i = 0; i < labelsetsize * labelsetsize; i++)
					edgefrequentarray[i] = 0;

				for (map<int, map<int, int> >::iterator src =
						src_edgeFrequent.begin(); src != src_edgeFrequent.end();
						++src) {
					for (map<int, int>::iterator dst = src->second.begin();
							dst != src->second.end(); ++dst) {
#ifdef little
						edgefrequentarray[(src->first - 'a') * labelsetsize
								+ dst->first - 'a'] = dst->second;
#else
						edgefrequentarray[(src->first - 1) * labelsetsize + dst->first
						- 1] = dst->second;
#endif
					}
				}

				MPI_Allreduce(edgefrequentarray, result,
						labelsetsize * labelsetsize, MPI_INT, MPI_SUM,
						MPI_COMM_WORLD);

				for (int src = 0; src < labelsetsize; src++) {
					for (int dst = 0; dst < labelsetsize; dst++) {
#ifdef little
						src_edgeFrequent[src + 'a'][dst + 'a'] = result[src
								* labelsetsize + dst];
//						if (get_worker_id() == MASTER_RANK)
//							ST("src<%c,%c> occurs %d times\n", src + 'a',
//									dst + 'a',
//									src_edgeFrequent[src + 'a'][dst + 'a']);
#else
						src_edgeFrequent[src+1][dst+1]=result[src*labelsetsize+dst];
#endif
					}
				}
				//-----------------------------------------------------------------
				for (int i = 0; i < labelsetsize * labelsetsize; i++)
					edgefrequentarray[i] = 0;

				for (map<int, map<int, int> >::iterator src =
						dst_edgeFrequent.begin(); src != dst_edgeFrequent.end();
						++src) {
					for (map<int, int>::iterator dst = src->second.begin();
							dst != src->second.end(); ++dst) {
#ifdef little
						edgefrequentarray[(src->first - 'a') * labelsetsize
								+ dst->first - 'a'] = dst->second;
#else
						edgefrequentarray[(src->first - 1) * labelsetsize + dst->first
						- 1] = dst->second;
#endif
					}
				}

				MPI_Allreduce(edgefrequentarray, result,
						labelsetsize * labelsetsize, MPI_INT, MPI_SUM,
						MPI_COMM_WORLD);

				for (int src = 0; src < labelsetsize; src++) {
					for (int dst = 0; dst < labelsetsize; dst++) {
#ifdef little
						dst_edgeFrequent[src + 'a'][dst + 'a'] = result[src
								* labelsetsize + dst];
//						if (get_worker_id() == MASTER_RANK)
//							ST("dst<%c,%c> occurs %d times\n", src + 'a',
//									dst + 'a',
//									dst_edgeFrequent[src + 'a'][dst + 'a']);
#else
						dst_edgeFrequent[src+1][dst+1]=result[src*labelsetsize+dst];
#endif
					}
				}
				//<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
				assert(src_edgeFrequent.size()==dst_edgeFrequent.size());
				for (map<int, map<int, int> >::iterator src =
						src_edgeFrequent.begin(); src != src_edgeFrequent.end();
						src++) {
					assert(
							src_edgeFrequent[src->first].size()==dst_edgeFrequent[src->first].size());
					for (map<int, int>::iterator dst = src->second.begin();
							dst != src->second.end(); dst++) {
						edgeFrequent[src->first][dst->first] = min(
								src_edgeFrequent[src->first][dst->first],
								dst_edgeFrequent[src->first][dst->first]);

#ifdef little
						if (get_worker_id() == MASTER_RANK)
							ST("<%c,%c> occurs %d times\n", src->first,
									dst->first,
									edgeFrequent[src->first][dst->first]);
#else
						if (get_worker_id()==MASTER_RANK && edgeFrequent[src->first][dst->first] >= minsup)
//						if (get_worker_id()==MASTER_RANK)
						ST("<%d,%d> occurs %d times\n", src->first, dst->first,edgeFrequent[src->first][dst->first]);
#endif
					}
				}

			} else if (preprocessSuperstep == 3) {
				//delete vertexes with low frequency
				vector<VertexT*> vertexes_tmp = vertexes;
				vertexes.clear();

				for (typename vector<VertexT*>::iterator it =
						vertexes_tmp.begin(); it != vertexes_tmp.end(); it++) {
					if ((*it)->has_neighbor()) {
						vertexes.push_back(*it);
					} else {
						delete *it;
					}
				}
				//message buffer need to be reinit
				message_buffer->reinit(vertexes);
			}
		}

		if (get_worker_id() == MASTER_RANK)
			ST("Leave Preprocess\n");
	}

	void dump_partition(const char* outpath) {
		hdfsFS fs = getHdfsFS();
		BufferedWriter* writer = new BufferedWriter(outpath, fs, _my_rank);

		for (VertexIter it = vertexes.begin(); it != vertexes.end(); it++) {
			writer->check();
			toline(*it, *writer);
		}
		delete writer;
		hdfsDisconnect(fs);
	}
	//=======================================================
	void postsim_process(){
		phase = postloopsim;
		postsiminit();
		all_compute();
		postsimmessageprocess();
		phase = normalcomputing;
	}
	// run the worker
	void looponsim() {
		long long step_msg_num;
		long long step_vadd_num;
		if (get_worker_id() == MASTER_RANK) {
			printf(
					"\n====================enter loopsim======================\n");

			setBit(Query_Mutated);
		}
		setBit(WAKE_ALL_ORBIT);
		while (true) {
//			worker_barrier();
			global_step_num++;
			ResetTimer(4);
			//===================
			char bits_bor = all_bor(global_bor_bitmap);
			if (getBit(FORCE_TERMINATE_ORBIT, bits_bor) == 1)
				break;
			if (getBit(Query_Mutated, bits_bor) == 1) {
				if (get_worker_id() == MASTER_RANK) {
					masterBcast(gspanMsg);
#ifdef little
//					ST("M:(size,s,sl,d,dl)=(%d,%d,%c,%d,%c)\n", gspanMsg.size,
//							gspanMsg.fromid, gspanMsg.fromlabel, gspanMsg.toid,
//							gspanMsg.tolabel);
#else
					ST("M:(size,s,sl,d,dl)=(%d,%d,%d,%d,%d)\n", gspanMsg.size,
							gspanMsg.fromid, gspanMsg.fromlabel, gspanMsg.toid,
							gspanMsg.tolabel);
#endif
				} else {
					slaveBcast(gspanMsg);
//					ST("S:(size,s,sl,d,dl)=(%d,%d,%c,%d,%c)\n", gspanMsg.size,
//							gspanMsg.fromid, gspanMsg.fromlabel, gspanMsg.toid,
//							gspanMsg.tolabel);
				}
				processgspanMsg();
				if (get_worker_id() == MASTER_RANK) {
					printf("gspanMSG: (%d:%d, %d,%d)\n", gspanMsg.fromid,
							gspanMsg.fromlabel, gspanMsg.toid,
							gspanMsg.tolabel);
				}
				mutated = true;
			} else {
				mutated = false;
			}
			get_vnum() = all_sum(vertexes.size());
			int wakeAll = getBit(WAKE_ALL_ORBIT, bits_bor);
			if (wakeAll == 0) {
				active_vnum() = all_sum(active_count);
				if (active_vnum() == 0
						&& getBit(HAS_MSG_ORBIT, bits_bor) == 0) {
					postsim_process();
					if (get_worker_id() == MASTER_RANK)
						break; //all_halt AND no_msg
					else
						continue;
				}
			} else
				active_vnum() = get_vnum();
			//===================
			AggregatorT* agg = (AggregatorT*) get_aggregator();
			if (agg != NULL)
				agg->init();
			//==============================================================================
			clearBits();

			if (wakeAll == 1) {
				all_compute();
			} else {
				active_compute();
			}
			message_buffer->combine();
			step_msg_num = master_sum_LL(message_buffer->get_total_msg());
			step_vadd_num = master_sum_LL(message_buffer->get_total_vadd());
			if (_my_rank == MASTER_RANK) {
				global_msg_num += step_msg_num;
				global_vadd_num += step_vadd_num;
			}
			vector<VertexT*>& to_add = message_buffer->sync_messages();
			agg_sync();
			for (int i = 0; i < to_add.size(); i++)
				add_vertex(to_add[i]);
			to_add.clear();
			//===================
			worker_barrier();
			StopTimer(4);
			if (_my_rank == MASTER_RANK) {
				cout << "Superstep " << global_step_num
						<< " done. Time elapsed: " << get_timer(4) << " seconds"
						<< endl;
				cout << "#msgs: " << step_msg_num << ", #vadd: "
						<< step_vadd_num << endl;
				printf(
						"--------------------------------SuperStep %d end------------------------------------\n",
						step_num());
			}
		}
		if (get_worker_id() == MASTER_RANK)
			printf(
					"\n====================leave loopsim======================\n");
	}

	void run(const WorkerParams& params) {

		//check path + init
		if (_my_rank == MASTER_RANK) {
			if (dirCheck(params.input_path.c_str(), params.output_path.c_str(),
					_my_rank == MASTER_RANK, params.force_write) == -1)
				exit(-1);
		}
		init_timers();

		//dispatch splits
		ResetTimer(WORKER_TIMER);
		vector<vector<string> >* arrangement;
		if (_my_rank == MASTER_RANK) {
			arrangement =
					params.native_dispatcher ?
							dispatchLocality(params.input_path.c_str()) :
							dispatchRan(params.input_path.c_str());
			//reportAssignment(arrangement);//DEBUG !!!!!!!!!!
			masterScatter(*arrangement);
			vector<string>& assignedSplits = (*arrangement)[0];
			//reading assigned splits (map)
			for (vector<string>::iterator it = assignedSplits.begin();
					it != assignedSplits.end(); it++)
				load_graph(it->c_str());
			delete arrangement;
		} else {
			vector<string> assignedSplits;
			slaveScatter(assignedSplits);
			//reading assigned splits (map)
			for (vector<string>::iterator it = assignedSplits.begin();
					it != assignedSplits.end(); it++)
				load_graph(it->c_str());
		}

		//send vertices according to hash_id (reduce)
		sync_graph();

		message_buffer->init(vertexes);
		//barrier for data loading
		worker_barrier(); //@@@@@@@@@@@@@
		StopTimer(WORKER_TIMER);
		PrintTimer("Load Time", WORKER_TIMER);
		//finished loading graph.================================
		init_timers();
		ResetTimer(WORKER_TIMER);
		//supersteps
		global_step_num = 0;
		//==============================loop start here=========================================
		GSPAN::gSpan gspan; //initialize gspan and the label set
#ifdef little
		minsup = 2;
#else
		minsup = 30;
#endif
		phase = preprocessing;
		preprocess();
		phase = normalcomputing;
		if (get_worker_id() != MASTER_RANK) {
			looponsim();
		} else {
			ST("loop start\n");
			StartTimer(GSPAN_TIMER);
#ifdef little
			gspan.run(minsup, 1, 3, false, false, true);
#else
			gspan.run(minsup, 1, 3, false, false, true);
#endif
			StopTimer(GSPAN_TIMER);
//			test();
//			terminate = true;
			setBit(FORCE_TERMINATE_ORBIT);
			looponsim();
			ST("loop end\n");

		}
		//==============================loop end here===========================================
		worker_barrier();
		StopTimer(WORKER_TIMER);
		PrintTimer("Communication Time", COMMUNICATION_TIMER);
		PrintTimer("- Serialization Time", SERIALIZATION_TIMER);
		PrintTimer("- Transfer Time", TRANSFER_TIMER);
		PrintTimer("Total Computational Time", WORKER_TIMER);
		PrintTimer("Total gspan time", GSPAN_TIMER);
		if (_my_rank == MASTER_RANK) {
			cout << "Total #msgs=" << global_msg_num << ", Total #vadd="
					<< global_vadd_num << endl;
			printf("total #supersteps=%d\n", step_num());
		}
		// dump graph
		ResetTimer(WORKER_TIMER);
		dump_partition(params.output_path.c_str());
		StopTimer(WORKER_TIMER);
		PrintTimer("Dump Time", WORKER_TIMER);

	}

	int getworkervertexnumber() {
		return vertexes.size();
	}

	void test() {
//		gspanMsg.size = 1;
//		if (dfs.src == 'l') {
//			gspanMsg.fromid = dfs.from;
//			gspanMsg.fromlabel = dfs.fromlabel;
//			gspanMsg.toid = dfs.to;
//			gspanMsg.tolabel = dfs.tolabel;
//		} else if (dfs.src == 'r') {
//			gspanMsg.fromid = dfs.to;
//			gspanMsg.fromlabel = dfs.tolabel;
//			gspanMsg.toid = dfs.from;
//			gspanMsg.tolabel = dfs.fromlabel;
//		} else
//			assert(false);
//		mine();
	}

	VertexContainer& getAllVertexes() {
		return vertexes;
	}

private:
	HashT hash;
	VertexContainer vertexes;
	int active_count;

	MessageBuffer<VertexT>* message_buffer;
	Combiner<MessageT>* combiner;
	AggregatorT* aggregator;
	//-------------add by zjh---------------------

	long long global_msg_num = 0;
	long long global_vadd_num = 0;
	bool terminate = false;

};

#endif
