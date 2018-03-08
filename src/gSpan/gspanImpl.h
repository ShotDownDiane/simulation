/*
 $Id: gspan.cpp,v 1.8 2004/05/21 09:27:17 taku-ku Exp $;

 Copyright (C) 2004 Taku Kudo, All rights reserved.
 This is free software with ABSOLUTELY NO WARRANTY.

 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation; either version 2 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.
 
 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA
 02111-1307, USA
 */


//#include "gspan.h"
#include "../basic/Worker.h"
#include <iterator>
#include <fstream>
#include <stdlib.h>
#include <unistd.h>

namespace GSPAN {
char * resultfile=NULL;
gSpan::gSpan(void) {
#ifdef SIMULATION
#ifdef little
	labelset.push_back('a');
	labelset.push_back('b');
	labelset.push_back('c');
	labelset.push_back('d');
	labelset_global.push_back('a');
	labelset_global.push_back('b');
	labelset_global.push_back('c');
	labelset_global.push_back('d');
#else
	for (char i = 1; i <= 100; i++){
		labelset.push_back(i);
		labelset_global.push_back(i);
	}
#endif
	labelsetsize = labelset.size();
#endif
}
void gSpan::outputmp4(Projected_map4 mp, std::string prefix, std::string suffix,
		std::string indent) {
	for (Projected_iterator4 it = mp.begin(); it != mp.end(); ++it) {
		char first[12];
		sprintf(first, "%d", it->first);
		outputmp3(it->second, prefix + first + ",", "]", indent);
	}
}
void gSpan::outputmp3(Projected_map3 mp, std::string prefix, std::string suffix,
		std::string indent) {
	for (Projected_iterator3 it = mp.begin(); it != mp.end(); ++it) {
		char first[12];
		sprintf(first, "%d", it->first);
		outputmp2(it->second, prefix + first + ",", "]", indent);
	}
}
void gSpan::outputmp2(Projected_map2 mp, std::string prefix, std::string suffix,
		std::string indent) {
	for (Projected_iterator2 it = mp.begin(); it != mp.end(); ++it) {
		char first[12];
		sprintf(first, "%d", it->first);
		outputmp1(it->second, prefix + first + ",", "]", indent);
	}
}
void gSpan::outputmp1(Projected_map1 mp, std::string prefix, std::string suffix,
		std::string indent) {
	for (Projected_iterator1 it = mp.begin(); it != mp.end(); ++it) {
		char first[12];
		sprintf(first, "%c", it->first);
		outputProjected(it->second, prefix + first, "]", indent);
	}
}
void gSpan::outputProjected(Projected match, std::string prefix,
		std::string suffix, std::string indent) {
	*os << prefix << "]=";
	for (Projected::iterator it = match.begin(); it != match.end(); ++it) {
		*os << it->id << ":(" << it->edge->from << "," << it->edge->to << "),";
	}
	*os << std::endl;
}

void gSpan::outputdfscode(DFSCode &code) {
	for (DFSCode::iterator it = code.begin(); it != code.end(); ++it) {
#ifdef little
		printf("(%d,%d,%c,%d,%c,%c)", it->from, it->to, it->fromlabel,
				it->elabel, it->tolabel, it->src);
#else
		printf("(%d,%d,%d,%d,%d,%c)", it->from, it->to, it->fromlabel,
				it->elabel, it->tolabel, it->src);
#endif
	}
	printf("\n");
}

#ifndef SIMULATION
std::istream &gSpan::read(std::istream &is) {
	Graph g(directed);
	while (true) {
		g.read(is);
		if (g.empty())
		break;
		TRANS.push_back(g);
//		TRACE("trans#%d\n", TRANS.size());
	}
	return is;
}

std::map<unsigned int, unsigned int> gSpan::support_counts(
		Projected &projected) {
	std::map<unsigned int, unsigned int> counts;

	for (Projected::iterator cur = projected.begin(); cur != projected.end();
			++cur) {
		counts[cur->id] += 1;
	}

	return (counts);
}

unsigned int gSpan::support(Projected &projected) {
	unsigned int oid = 0xffffffff;
	unsigned int size = 0;

	for (Projected::iterator cur = projected.begin(); cur != projected.end();
			++cur) {
		if (oid != cur->id) {
			++size;
		}
		oid = cur->id;
	}

	return size;
}
/* Special report function for single node graphs.
 */
void gSpan::report_single(Graph &g, unsigned int sup) {
//	TRACE("maxpat_max=%u,maxpat_min=%u,g.size()=%lu\n", maxpat_max, maxpat_min,
//			g.size());
	if (maxpat_max > maxpat_min && g.size() > maxpat_max)
	return;
	if (maxpat_min > 0 && g.size() < maxpat_min)
	return;

	if (enc == false) {
#ifndef DIRECTED
		if (where == false)
#endif
		*os << "t # " << ID << " * " << sup;
		*os << '\n';

		g.write(*os);
		*os << '\n';
	} else {
		std::cerr << "report_single not implemented for non-Matlab calls"
		<< std::endl;
	}
	ID++;
}
#endif



void gSpan::report(Projected &projected, unsigned int sup) {

	/* Filter too small/too large graphs.
	 */
	if (maxpat_max > maxpat_min && DFS_CODE.nodeCount() > maxpat_max)
		return;
	if (maxpat_min > 0 && DFS_CODE.nodeCount() < maxpat_min)
		return;
#ifndef DIRECTED
	if (where) {
		*os << "<pattern>\n";
		*os << "<id>" << ID << "</id>\n";
		*os << "<support>" << sup << "</support>\n";
		*os << "<what>";
	}
#endif

	if (!enc) {
		Graph g(directed);
		DFS_CODE.toGraph(g);
#ifndef DIRECTED
		if (!where)
#endif
		*os << "t # " << ID << " * " << sup;

		*os << '\n';
		g.write(*os);
	} else {
		if (!where)
			*os << '<' << ID << ">    " << sup << " [";

		DFS_CODE.write(*os);
		if (!where)
			*os << ']';
	}

	if (where) {
		*os << "Occurrent In Trans:";

		for (Projected::iterator cur = projected.begin();
				cur != projected.end(); ++cur) {
			if (cur != projected.begin())
				*os << ' ';
			*os << cur->id;
			printf("(%d,%d)", cur->edge->from, cur->edge->to);
		}
#ifdef DIRECTED
#else
		*os << "</where>\n</pattern>";
#endif
	}
#ifdef DIRECTED
	*os << "\n\n";
#else
	*os << '\n';
#endif
	++ID;
}


void gSpan::store(Projected &projected, unsigned int sup) {

	if(resultfile){
		/* Filter too small/too large graphs.
		 */
		if (maxpat_max > maxpat_min && DFS_CODE.nodeCount() > maxpat_max)
			return;
		if (maxpat_min > 0 && DFS_CODE.nodeCount() < maxpat_min)
			return;

		std::ofstream ofile;
		ofile.open(resultfile,std::ios::app);
		Graph g(directed);
		DFS_CODE.toGraph(g);
		ofile << "t # " << ID << " * " << sup;

		ofile<< '\n';
		g.write(ofile);


		ofile << "\n\n";

		ID;
	}
}
/* Recursive subgraph mining function (similar to subprocedure 1
 * Subgraph_Mining in [Yan2002]).
 */
#ifdef SIMULATION
void gSpan::project(Projected &projected) {

	static int ismintestcount = 0, exceedstestcount = 0, supptestcount = 0,
			supppasscount = 0;
	assert(projected.size()==1);

#ifdef SIMULATION
	//filter if new added edge below frequent
	DFS& dfstmp = DFS_CODE.back();
	if (dfstmp.src == 'l') {
		if (edgeFrequent[dfstmp.fromlabel][dfstmp.tolabel] < minsup){
			return;
		}
	} else if (dfstmp.src == 'r') {
//		if(dfstmp.from==30&&dfstmp.tolabel)
		if (edgeFrequent[dfstmp.tolabel][dfstmp.fromlabel] < minsup){
			return;
		}
	} else {
		assert(false);
	}
#endif

	/* Check if the pattern is frequent enough.
	 *
	 * TODO: here we need to invoke the simulation algorithm to calculate the support value!
	 */
//	unsigned int sup = 2;
//	if (sup < minsup) {
//		return;
//	}
	/* In case we have a valid upper bound and our graph already exceeds it,
	 * return.  Note: we do not check for equality as the DFS exploration may
	 * still add edges within an existing subgraph, without increasing the
	 * number of nodes.
	 */
	exceedstestcount++;
	if (maxpat_max > maxpat_min && DFS_CODE.nodeCount() > maxpat_max) {
		return;
	}
	/* The minimal DFS code check is more expensive than the support check,
	 * hence it is done now, after checking the support.
	 *
	 * the inverse occasion in distributed graph simulation, support count is more expensive!
	 */
	ismintestcount++;
	if (is_min() == false) {
		//      *os  << "NOT MIN [";  DFS_CODE.write (*os);  *os << "]" << std::endl;
		return;
	}


#ifdef SIMULATION
	//maintain the RMVertexes vector
	RMPath rmpath=DFS_CODE.buildRMPath();
	RMVertexes.clear();
	for(RMPath::reverse_iterator it=rmpath.rbegin();it!=rmpath.rend();it++){
		RMVertexes.push_back(DFS_CODE[*it].from);
	}
	if(rmpath.size()>0){
		RMVertexes.push_back(DFS_CODE[rmpath[0]].to);
	}

	//maintain gspanMsg
	supptestcount++;
	static int reported = 0;
	reported++;
	gspanMsg.size = DFS_CODE.size();
	DFS &dfs = DFS_CODE[DFS_CODE.size() - 1];
	if (dfs.src == 'l') {
		gspanMsg.fromid = dfs.from;
		gspanMsg.fromlabel = dfs.fromlabel;
		gspanMsg.toid = dfs.to;
		gspanMsg.tolabel = dfs.tolabel;
	} else if (dfs.src == 'r') {
		gspanMsg.fromid = dfs.to;
		gspanMsg.fromlabel = dfs.tolabel;
		gspanMsg.toid = dfs.from;
		gspanMsg.tolabel = dfs.fromlabel;
	} else
		assert(false);

	static double gspanstarttime = get_current_time();
	static double gspanstoptime = 0;
	static double gspanacctime = 0;
	static double simstarttime = 0;
	static double simstoptime = 0;
	static double simacctime = 0;

	gspanstoptime = get_current_time();

	simstarttime = gspanstoptime;

	StopTimer(GSPAN_TIMER);
	ST("report DFS_CODE start\n");
	outputdfscode(DFS_CODE);
	ST("report DFS_CODE end\n");
	mine();
	StartTimer(GSPAN_TIMER);

	simstoptime = get_current_time();
	gspanacctime += gspanstoptime - gspanstarttime;
	simacctime += simstoptime - simstarttime;

	printf("%f %f %f %f (exceeds,ismin,supp,suppY,suppV)=(%d,%d,%d,%d,%d)\n",
			gspanstoptime - gspanstarttime, simstoptime - simstarttime,
			gspanacctime, simacctime, exceedstestcount, ismintestcount,
			supptestcount, supppasscount, curSupp());

	gspanstarttime = simstoptime;

	int supp = curSupp();


	if (supp < minsup) {
		return;
	}
	supppasscount++;
	report(projected, supp);
	store(projected, supp);

#else
	report(projected, sup);
#endif


	/* We just outputted a frequent subgraph.  As it is frequent enough, so
	 * might be its (n+1)-extension-graphs, hence we enumerate them all.
	 */

	//const RMPath rmpath = DFS_CODE.buildRMPath(); //build by backward traverse.
	int minlabel = DFS_CODE[0].fromlabel; //root vertex label, minlabel of dfscode
	int maxtoc = DFS_CODE[rmpath[0]].to; //max vertexID of dfs code.

	//===============================================backward==========================================
	for (int i = (rmpath.size() - 1); i >= 0; --i) {
		int from = DFS_CODE[rmpath[i]].from;
		int to = DFS_CODE[rmpath[0]].to;

		//TODO: here needs to pruning some result.
		//needs to filter out some edges to reduce search space.

		//before adding the backward edge, we have to test if it exists
		bool exist = false;
		for (vector<Edge>::size_type i = 0; i < currentGraph[to].edge.size();
				i++) {
			assert(currentGraph[to].edge[i].from==to);
			if (currentGraph[to].edge[i].to == from) {
				exist = true;
			}
		}
		Projected p;
//		TRACE("exists? %d\n",exist);
//		TRACE("Report currentGraph\n");
//		currentGraph.write(*os);
//		TRACE("Report currentGraph end\n");
		if (!exist) {
			if (DFS_CODE[rmpath[i]].tolabel > DFS_CODE[rmpath[0]].tolabel)
				continue;


			DFS_CODE.push(to, from, DFS_CODE[rmpath[0]].tolabel, 0,
					DFS_CODE[rmpath[i]].fromlabel, 'l');

			currentGraph[to].push(to, from, 0);

			p.push(0, 0, &projected[0]);
			project(p);

			currentGraph[to].pop();
			DFS_CODE.pop();
		}

		exist = false;
		for (int i = 0; i < currentGraph[from].edge.size(); i++) {
			assert(currentGraph[from].edge[i].from==from);
			if (currentGraph[from].edge[i].to == to) {
				exist = true;
			}
		}
//		TRACE("exists? %d\n",exist);
//		TRACE("Report currentGraph\n");
//		currentGraph.write(*os);
//		TRACE("Report currentGraph end\n");
		if (!exist) {
			if (DFS_CODE[rmpath[i]].tolabel > DFS_CODE[rmpath[0]].tolabel)
				continue;
			if (DFS_CODE[rmpath[i]].tolabel == DFS_CODE[rmpath[0]].tolabel
					&& DFS_CODE[rmpath[i]].src == 'r')
				continue;

			DFS_CODE.push(to, from, DFS_CODE[rmpath[0]].tolabel, 0,
					DFS_CODE[rmpath[i]].fromlabel, 'r');

			currentGraph[from].push(from, to, 0);

			p.clear();
			p.push(0, 0, &projected[0]);
			project(p);

			currentGraph[from].pop();
			DFS_CODE.pop();
		}
	}
	//=================================================pure forward============================================
	currentGraph.resize(maxtoc + 2);
	for (int i = 0; i < labelset.size(); i++) {
		if (labelset[i] < minlabel)
			continue;
		currentGraph[maxtoc + 1].label = labelset[i];

		Projected p;

		DFS_CODE.push(maxtoc, maxtoc + 1, DFS_CODE[rmpath[0]].tolabel, 0,
				labelset[i], 'l');

		currentGraph[maxtoc].push(maxtoc, maxtoc + 1, 0);

		p.push(0, 0, &projected[0]);
		project(p);

		currentGraph[maxtoc].pop();
		DFS_CODE.pop();
		//-------------------------------------------------------------

		DFS_CODE.push(maxtoc, maxtoc + 1, DFS_CODE[rmpath[0]].tolabel, 0,
				labelset[i], 'r');

		currentGraph[maxtoc + 1].push(maxtoc + 1, maxtoc, 0);

		p.clear();
		p.push(0, 0, &projected[0]);
		project(p);

		currentGraph[maxtoc + 1].pop();
		DFS_CODE.pop();
	}

	//===================================backtracked forward===================================
	for (int i = (rmpath.size() - 1); i >= 0; --i) {
		int from = DFS_CODE[rmpath[i]].from;
		for (int j = 0; j < labelset.size(); j++) {
			currentGraph[maxtoc + 1].label = labelset[j];
			if (labelset[j] < minlabel)
				continue;

			if (DFS_CODE[rmpath[i]].tolabel > labelset[j])
				continue;

			Projected p;
			if (DFS_CODE[rmpath[i]].tolabel < labelset[j]
					|| DFS_CODE[rmpath[i]].src == 'l') {

				DFS_CODE.push(from, maxtoc + 1, DFS_CODE[rmpath[i]].fromlabel,
						0, labelset[j], 'l');

				currentGraph[from].push(from, maxtoc + 1, 0);
				p.push(0, 0, &projected[0]);
				project(p);

				currentGraph[from].pop();
				DFS_CODE.pop();
			}

			DFS_CODE.push(from, maxtoc + 1, DFS_CODE[rmpath[i]].fromlabel, 0,
					labelset[j], 'r');

			currentGraph[maxtoc + 1].push(maxtoc + 1, from, 0);

			p.clear();
			p.push(0, 0, &projected[0]);
			project(p);

			currentGraph[maxtoc + 1].pop();
			DFS_CODE.pop();
		}
	}
	currentGraph.resize(maxtoc + 1);
	assert(currentGraph.size()==(maxtoc+1));
	return;
}
#else

void gSpan::project(Projected &projected) {


	/* Check if the pattern is frequent enough.
	 */
	unsigned int sup = support(projected);
	if (sup < minsup) {
		return;
	}

	/* The minimal DFS code check is more expensive than the support check,
	 * hence it is done now, after checking the support.
	 */
	if (is_min() == false) {
		//      *os  << "NOT MIN [";  DFS_CODE.write (*os);  *os << "]" << std::endl;
		return;
	}

	// Output the frequent substructure
	report(projected, sup);

	/* In case we have a valid upper bound and our graph already exceeds it,
	 * return.  Note: we do not check for equality as the DFS exploration may
	 * still add edges within an existing subgraph, without increasing the
	 * number of nodes.
	 */
	if (maxpat_max > maxpat_min && DFS_CODE.nodeCount() > maxpat_max) {
		return;
	}

	/* We just outputted a frequent subgraph.  As it is frequent enough, so
	 * might be its (n+1)-extension-graphs, hence we enumerate them all.
	 */
	const RMPath &rmpath = DFS_CODE.buildRMPath(); //build by backward traverse.
	int minlabel = DFS_CODE[0].fromlabel;//root vertex label, minlabel of dfscode
	int maxtoc = DFS_CODE[rmpath[0]].to;//max vertexID of dfs code.

#ifdef DIRECTED
	Projected_map4 new_fwd_root; //(fromVid,elabel,toVlabel,src)
	Projected_map3 new_bck_root;//(toVid,elabel,src)
#else

	Projected_map3 new_fwd_root; //(fromVid,elabel,toVlabel)
	Projected_map2 new_bck_root;//(toVid,elabel)==>PDFS
#endif

	EdgeList edges;

	/* Enumerate all possible one edge extensions of the current substructure.
	 */
	for (unsigned int n = 0; n < projected.size(); ++n) {

		unsigned int id = projected[n].id; //transID
		PDFS *cur = &projected[n];//
		History history(TRANS[id], cur);

		// backward
#ifdef DIRECTED
		for (int i = (int) rmpath.size() - 1; i >= 0; --i) {
			//if(DFS_CODE[rmpath[i]].src==)
			int from, to;
			if (DFS_CODE[rmpath[i]].src == 'l') {
				from = history[rmpath[i]]->from;
			} else if (DFS_CODE[rmpath[i]].src == 'r') {
				from = history[rmpath[i]]->to;
			} else
			assert(false);
			if (DFS_CODE[rmpath[0]].src == 'l') {
				to = history[rmpath[0]]->to;
			} else if (DFS_CODE[rmpath[0]].src == 'r') {
				to = history[rmpath[0]]->from;
			} else
			assert(false);

			Edge *e = get_backward(TRANS[id], history[rmpath[i]],
					history[rmpath[0]], history, from, to);
			if (e)
			if (e->from == from)
			new_bck_root[DFS_CODE[rmpath[i]].from][e->elabel]['r'].push(
					id, e, cur);
			else if (e->from == to)
			new_bck_root[DFS_CODE[rmpath[i]].from][e->elabel]['l'].push(
					id, e, cur);
		}
#else
		for (int i = (int) rmpath.size() - 1; i >= 1; --i) {
			Edge *e = get_backward(TRANS[id], history[rmpath[i]],
					history[rmpath[0]], history);
			if (e)
			new_bck_root[DFS_CODE[rmpath[i]].from][e->elabel].push(id, e,
					cur);
		}
#endif

		// pure forward
		// FIXME: here we pass a too large e->to (== history[rmpath[0]]->to
		// into get_forward_pure, such that the assertion fails.
		//
		// The problem is:
		// history[rmpath[0]]->to > TRANS[id].size()
#ifdef DIRECTED

		int v;
		if (DFS_CODE[rmpath[0]].src == 'l') {
			v = history[rmpath[0]]->to;
		} else if (DFS_CODE[rmpath[0]].src == 'r') {
			v = history[rmpath[0]]->from;
		} else
		assert(false);

		if (get_forward_pure(TRANS[id], v, minlabel, history, edges))
		for (EdgeList::iterator it = edges.begin(); it != edges.end(); ++it)
		if ((*it)->from == v)
		new_fwd_root[maxtoc][(*it)->elabel][TRANS[id][(*it)->to].label]['l'].push(
				id, *it, cur);
		else if ((*it)->to == v)
		new_fwd_root[maxtoc][(*it)->elabel][TRANS[id][(*it)->from].label]['r'].push(
				id, *it, cur);
		else
		assert(false);
#else
		if (get_forward_pure(TRANS[id], history[rmpath[0]], minlabel, history,
						edges))
		for (EdgeList::iterator it = edges.begin(); it != edges.end(); ++it)
		new_fwd_root[maxtoc][(*it)->elabel][TRANS[id][(*it)->to].label].push(
				id, *it, cur);
#endif

		// backtracked forward
#ifdef DIRECTED
		for (int i = 0; i < (int) rmpath.size(); ++i) {

			int v1, v2;
			if (DFS_CODE[rmpath[i]].src == 'l') {
				v1 = history[rmpath[i]]->from;
				v2 = history[rmpath[i]]->to;
			} else if (DFS_CODE[rmpath[i]].src == 'r') {
				v1 = history[rmpath[i]]->to;
				v2 = history[rmpath[i]]->from;
			} else
			assert(false);

			if (get_forward_rmpath(TRANS[id], v1, v2, history[rmpath[i]],
							minlabel, history, edges))
			for (EdgeList::iterator it = edges.begin(); it != edges.end();
					++it)
			if ((*it)->from == v1)
			new_fwd_root[DFS_CODE[rmpath[i]].from][(*it)->elabel][TRANS[id][(*it)->to].label]['l'].push(
					id, *it, cur);
			else if ((*it)->to == v1)
			new_fwd_root[DFS_CODE[rmpath[i]].from][(*it)->elabel][TRANS[id][(*it)->from].label]['r'].push(
					id, *it, cur);
			else
			assert(false);
		}
#else
		for (int i = 0; i < (int) rmpath.size(); ++i)
		if (get_forward_rmpath(TRANS[id], history[rmpath[i]], minlabel,
						history, edges))
		for (EdgeList::iterator it = edges.begin(); it != edges.end();
				++it)
		new_fwd_root[DFS_CODE[rmpath[i]].from][(*it)->elabel][TRANS[id][(*it)->to].label].push(
				id, *it, cur);
#endif
	}

	/* Test all extended substructures.
	 *
	 *
	 */

	/* Attention: how could the vertex label be -1, and why ?
	 *
	 * in the DFS_CODE.push operations, sometime the label of vertex is -1, the label -1 indicates
	 * this vertex has been included in the previous DFS_code and the label can be obtained by
	 * traverse the DFS_code
	 *
	 */
	// backward
#ifdef DIRECTED
	for (Projected_iterator3 to = new_bck_root.begin();
			to != new_bck_root.end(); ++to) {
		for (Projected_iterator2 elabel = to->second.begin();
				elabel != to->second.end(); ++elabel) {
			for (Projected_iterator1 src = elabel->second.begin();
					src != elabel->second.end(); ++src) {
				DFS_CODE.push(maxtoc, to->first, -1, elabel->first, -1,
						src->first);
				project(src->second);
				DFS_CODE.pop();
			}
		}
	}
#else
	for (Projected_iterator2 to = new_bck_root.begin();
			to != new_bck_root.end(); ++to) {
		for (Projected_iterator1 elabel = to->second.begin();
				elabel != to->second.end(); ++elabel) {
			DFS_CODE.push(maxtoc, to->first, -1, elabel->first, -1);
			project(elabel->second);
			DFS_CODE.pop();
		}
	}
#endif

	// forward
#ifdef DIRECTED
	for (Projected_riterator4 from = new_fwd_root.rbegin();
			from != new_fwd_root.rend(); ++from) {
		for (Projected_iterator3 elabel = from->second.begin();
				elabel != from->second.end(); ++elabel) {
			for (Projected_iterator2 tolabel = elabel->second.begin();
					tolabel != elabel->second.end(); ++tolabel) {
				for (Projected_iterator1 src = tolabel->second.begin();
						src != tolabel->second.end(); ++src) {
					DFS_CODE.push(from->first, maxtoc + 1, -1, elabel->first,
							tolabel->first, src->first);
					project(src->second);
					DFS_CODE.pop();
				}
			}
		}
	}
#else
	for (Projected_riterator3 from = new_fwd_root.rbegin();
			from != new_fwd_root.rend(); ++from) {
		for (Projected_iterator2 elabel = from->second.begin();
				elabel != from->second.end(); ++elabel) {
			for (Projected_iterator1 tolabel = elabel->second.begin();
					tolabel != elabel->second.end(); ++tolabel) {
				DFS_CODE.push(from->first, maxtoc + 1, -1, elabel->first,
						tolabel->first);
				project(tolabel->second);
				DFS_CODE.pop();
			}
		}
	}
#endif

	return;
}
#endif
#ifdef SIMULATION

void gSpan::run(unsigned int _minsup, unsigned int _maxpat_min,
		unsigned int _maxpat_max, bool _enc, bool _where, bool _directed) {
	/*
	 *
	 * parameters
	 * minsup
	 * maxpat_min, minimum nodes number
	 * maxpat_max, maximum nodes number
	 *
	 *
	 *
	 */
	os = &std::cout;
	ID = 0;
	minsup = _minsup;
	maxpat_min = _maxpat_min;
	maxpat_max = _maxpat_max;
	enc = _enc;
	where = _where;
	directed = _directed;

	TRACE(
			"Call parameter:\n  (_minsup=%u,_maxpat_min=%u,_maxpat_max=%u,_enc=%d,_where=%d,_directed=%d)\n",
			minsup, maxpat_min, maxpat_max, enc, where, directed);

	run_intern();
}
#else
void gSpan::run(std::istream &is, std::ostream &_os, unsigned int _minsup,
		unsigned int _maxpat_min, unsigned int _maxpat_max, bool _enc,
		bool _where, bool _directed) {
	os = &_os;
	ID = 0;
	minsup = _minsup;
	maxpat_min = _maxpat_min;
	maxpat_max = _maxpat_max;
	enc = _enc;
	where = _where;
	directed = _directed;

	TRACE(
			"Call parameter:\n  (_minsup=%u,_maxpat_min=%u,_maxpat_max=%u,_enc=%d,_where=%d,_directed=%d)\n",
			minsup, maxpat_min, maxpat_max, enc, where, directed);

	read(is);
	run_intern();
}
#endif

#ifdef SIMULATION
void gSpan::run_intern(void) {

	/*
	 * for hierarchy:(svlabel,dvlabel,src)
	 */

	for (unsigned int s = 0; s < labelset.size(); s++) {
		for (unsigned int d = 0; d < labelset.size(); d++) {
			currentGraph.resize(2);
			assert(currentGraph.size()==2);
			if (labelset[s] <= labelset[d]) {

				DFS_CODE.push(0, 1, labelset[s], 0, labelset[d], 'l');

				currentGraph[0].label = labelset[s];
				currentGraph[1].label = labelset[d];
				currentGraph[0].push(0, 1, 0);

				Projected p;
				p.push(0, 0, 0);
				assert(currentGraph.size()==2);
				project(p);

				currentGraph[0].pop();
				DFS_CODE.pop();
			} else {

				DFS_CODE.push(0, 1, labelset[d], 0, labelset[s], 'r');

				currentGraph[0].label = labelset[d];
				currentGraph[1].label = labelset[s];
				currentGraph[1].push(1, 0, 0);

				Projected p;
				p.push(0, 0, 0);
				assert(currentGraph.size()==2);
				project(p);

				currentGraph[1].pop();
				DFS_CODE.pop();
			}
		}
	}

}
#else

void gSpan::run_intern(void) {

	//single node pattern
	if (maxpat_min <= 1) {
		for (unsigned int id = 0; id < TRANS.size(); ++id) {
			for (unsigned int nid = 0; nid < TRANS[id].size(); ++nid) {
				if (singleVertex[id][TRANS[id][nid].label] == 0) {
					// number of graphs it appears in
					singleVertexLabel[TRANS[id][nid].label] += 1;
				}
				singleVertex[id][TRANS[id][nid].label] += 1; //(transID,vlabel)
			}
		}

		for (std::map<unsigned int, unsigned int>::iterator it =
				singleVertexLabel.begin(); it != singleVertexLabel.end();
				++it) {
			if ((*it).second < minsup)
			continue;

			unsigned int frequent_label = (*it).first;

			/* Found a frequent node label, report it.
			 */
			Graph g(directed);
			g.resize(1);
			g[0].label = frequent_label;

			report_single(g, (*it).second);
		}
	}

#ifdef DIRECTED

	Projected_map3 root;
	Projected_map4 root4; // (v1label,elabel,v2label,direction)==>PDFS(transID,edge*,0)

	EdgeList edges;

	/*
	 * for hierarchy: transactionsID --> srcVertexId --> ajacentVertexes
	 *
	 * K,V datastructure
	 *(svlabel,elabel,dvlabel)==>array(transID,edge*,prev*)
	 */
	for (unsigned int id = 0; id < TRANS.size(); ++id) {
		Graph &g = TRANS[id];
		for (unsigned int from = 0; from < g.size(); ++from) {
			if (get_forward_root(g, g[from], edges)) {
				for (EdgeList::iterator it = edges.begin(); it != edges.end();
						++it)
				if (g[from].label <= g[(*it)->to].label) //should be less or equal, because l<r
				root4[g[from].label][(*it)->elabel][g[(*it)->to].label]['l'].push(
						id, *it, 0);
				else
				root4[g[(*it)->to].label][(*it)->elabel][g[from].label]['r'].push(
						id, *it, 0);
			}
		}
	}
	/*
	 *
	 * for hierarchy
	 * 		svlabel -->  elabel --> dvlabel --> src
	 *
	 */
	for (Projected_iterator4 fromlabel = root4.begin();
			fromlabel != root4.end(); ++fromlabel) {
		for (Projected_iterator3 elabel = fromlabel->second.begin();
				elabel != fromlabel->second.end(); ++elabel) {
			for (Projected_iterator2 tolabel = elabel->second.begin();
					tolabel != elabel->second.end(); ++tolabel) {
				for (Projected_iterator1 src = tolabel->second.begin();
						src != tolabel->second.end(); src++) {
					DFS_CODE.push(0, 1, fromlabel->first, elabel->first,
							tolabel->first, src->first);
					project(src->second);
					DFS_CODE.pop();
				}
			}
		}
	}
#else
	//(svlabel,elabel,dvlabel)==>PDFS(transID,edge*,0)

	Projected_map3 root;

	EdgeList edges;

	/*
	 * for hierarchy: transactionsID --> srcVertexId --> ajacentVertexes
	 *
	 * K,V datastructure
	 *(svlabel,elabel,dvlabel)==>array(transID,edge*,prev*)
	 */
	for (unsigned int id = 0; id < TRANS.size(); ++id) {
		Graph &g = TRANS[id];
		for (unsigned int from = 0; from < g.size(); ++from) {
			if (get_forward_root(g, g[from], edges)) {
				for (EdgeList::iterator it = edges.begin(); it != edges.end();
						++it)
				root[g[from].label][(*it)->elabel][g[(*it)->to].label].push(
						id, *it, 0);
			}
		}
	}

	/*
	 *
	 * for hierarchy
	 * 		svlabel -->  elabel --> dvlabel
	 *
	 */
	for (Projected_iterator3 fromlabel = root.begin(); fromlabel != root.end();
			++fromlabel) {
		for (Projected_iterator2 elabel = fromlabel->second.begin();
				elabel != fromlabel->second.end(); ++elabel) {
			for (Projected_iterator1 tolabel = elabel->second.begin();
					tolabel != elabel->second.end(); ++tolabel) {
				/* Build the initial two-node graph.  It will be grown
				 * recursively within project.
				 */
				DFS_CODE.push(0, 1, fromlabel->first, elabel->first,
						tolabel->first);
				project(tolabel->second);
				DFS_CODE.pop();
			}
		}
	}
#endif

}
#endif

}





/*
 *
../src/basic/../gSpan/../basic/Worker.h(391) rank#0:<a,a> occurs 0 times

../src/basic/../gSpan/../basic/Worker.h(391) rank#0:<a,b> occurs 2 times

../src/basic/../gSpan/../basic/Worker.h(391) rank#0:<a,c> occurs 2 times

../src/basic/../gSpan/../basic/Worker.h(391) rank#0:<a,d> occurs 0 times

../src/basic/../gSpan/../basic/Worker.h(391) rank#0:<b,a> occurs 0 times

../src/basic/../gSpan/../basic/Worker.h(391) rank#0:<b,b> occurs 0 times

../src/basic/../gSpan/../basic/Worker.h(391) rank#0:<b,c> occurs 2 times

../src/basic/../gSpan/../basic/Worker.h(391) rank#0:<b,d> occurs 0 times

../src/basic/../gSpan/../basic/Worker.h(391) rank#0:<c,a> occurs 0 times

../src/basic/../gSpan/../basic/Worker.h(391) rank#0:<c,b> occurs 0 times

../src/basic/../gSpan/../basic/Worker.h(391) rank#0:<c,c> occurs 1 times

../src/basic/../gSpan/../basic/Worker.h(391) rank#0:<c,d> occurs 2 times

../src/basic/../gSpan/../basic/Worker.h(391) rank#0:<d,a> occurs 1 times

../src/basic/../gSpan/../basic/Worker.h(391) rank#0:<d,b> occurs 2 times

../src/basic/../gSpan/../basic/Worker.h(391) rank#0:<d,c> occurs 2 times

../src/basic/../gSpan/../basic/Worker.h(391) rank#0:<d,d> occurs 0 times




 */
