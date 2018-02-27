/*
 $Id: gspan.h,v 1.6 2004/05/21 05:50:13 taku-ku Exp $;

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
#include <iostream>
#include <map>
#include <vector>
#include <set>
#include <algorithm>
#include <stdio.h>
#include <string>
#include <assert.h>

//#define TRACE (printf("%s(%d)<%s>:\t",__FILE__, __LINE__, __FUNCTION__), printf)
#define TRACE (printf("%s(%d):",__FILE__, __LINE__), printf)
#define ST (printf("%s(%d) rank#%d:",__FILE__,__LINE__,_my_rank),printf)
//#define ST (printf("%s(%d) rank#%d:",__FILE__, __LINE__,_my_rank),printf)

/*
 * macro DIRECTED is used to decide if the edge is directed in the gspan code
 */
#define DIRECTED 1

/*
 * macro SIMULATION defines the whole project is a distributed mining project, if the Simulation is
 * not defined, the project would degrade downto a gSpan project
 *
 */
#define SIMULATION 1

/*
 *the macro little defines if the code is run on the cluster or on the development environment
 */
//#define little 1

#define debug
#ifdef debug
//#define debug1 //20180131


#endif



#define maxx(a,b)    (((a) > (b)) ? (a) : (b))
#define minn(a,b)    (((a) < (b)) ? (a) : (b))

namespace GSPAN {

template<class T> inline void _swap(T &x, T &y) {
	T z = x;
	x = y;
	y = z;
}

struct Edge {
	int from;
	int to;
	int elabel;
	unsigned int id;
	Edge() :
			from(0), to(0), elabel(0), id(0) {
	}
	;
};

class Vertex {
public:
	typedef std::vector<Edge>::iterator edge_iterator;

	int label;
	std::vector<Edge> edge;

	void push(int from, int to, int elabel) {
		edge.resize(edge.size() + 1);
		edge[edge.size() - 1].from = from;
		edge[edge.size() - 1].to = to;
		edge[edge.size() - 1].elabel = elabel;
		return;
	}

	void pop() {
		edge.resize(edge.size() - 1);
	}
};

class Graph: public std::vector<Vertex> {
private:
	unsigned int edge_size_;
public:
	typedef std::vector<Vertex>::iterator vertex_iterator;
	bool directed;

	Graph(bool _directed) {
		directed = _directed;
	}
	;

	unsigned int edge_size() {
		return edge_size_;
	}

	unsigned int vertex_size() {
		return (unsigned int) size();
	}

	void buildEdge();
	// read the graph, undirected edge is represented as two opposite edge
	std::istream &read(std::istream &);
	std::ostream &write(std::ostream &); // write
	void check(void);
#ifdef DIRECTED
	Graph() :
			edge_size_(0), directed(true) {
	}
	;
#else
	Graph() :
			edge_size_(0), directed(false) {
	}
	;
#endif
};

class DFS {
public:
	int from;
	int to;
	int fromlabel;
	int elabel;
	int tolabel;
#ifdef DIRECTED
	/*
	 * from->to is denoted as 'l'
	 * to->from is denoted as 'r'
	 *
	 * and l<r
	 *
	 * src denote on which side(left side or right side) the src V is
	 */
	char src;
#endif

	friend bool operator ==(const DFS &d1, const DFS &d2) {
#ifdef DIRECTED
		return (d1.from == d2.from && d1.to == d2.to
				&& d1.fromlabel == d2.fromlabel && d1.elabel == d2.elabel
				&& d1.tolabel == d2.tolabel && d1.src == d2.src);
#else
		return (d1.from == d2.from && d1.to == d2.to
				&& d1.fromlabel == d2.fromlabel && d1.elabel == d2.elabel
				&& d1.tolabel == d2.tolabel);
#endif
	}

	friend bool operator !=(const DFS &d1, const DFS &d2) {
		return (!(d1 == d2));
	}

#ifdef DIRECTED

#else
	DFS() :
	from(0), to(0), fromlabel(0), elabel(0), tolabel(0) {
	}
	;
#endif
};
typedef std::vector<int> RMPath;

struct DFSCode: public std::vector<DFS> {
private:
	RMPath rmpath;
public:
#ifdef SIMULATION
	const RMPath buildRMPath();
#else
	const RMPath& buildRMPath();
#endif

	/* Convert current DFS code into a graph.
	 */
	bool toGraph(Graph &);

	/* Clear current DFS code and build code from the given graph.
	 */
	void fromGraph(Graph &g);

	/* Return number of nodes in the graph.
	 */
	unsigned int nodeCount(void);
#ifdef DIRECTED
	void push(int from, int to, int fromlabel, int elabel, int tolabel,
			char direction) {
		resize(size() + 1);
		DFS &d = (*this)[size() - 1];

		d.from = from;
		d.to = to;
		d.fromlabel = fromlabel;
		d.elabel = elabel;
		d.tolabel = tolabel;
		d.src = direction;
	}
#else
	void push(int from, int to, int fromlabel, int elabel, int tolabel) {
		resize(size() + 1);
		DFS &d = (*this)[size() - 1];

		d.from = from;
		d.to = to;
		d.fromlabel = fromlabel;
		d.elabel = elabel;
		d.tolabel = tolabel;
	}
#endif
	void pop() {
		resize(size() - 1);
	}
	std::ostream &write(std::ostream &); // write
};

/**
 *
 * id is the transaction graph matched by the edge,
 * *edge is the matched edge of this transaction
 *
 *
 * PDFS form a linked list of match of the edges in DFSCode
 */
struct PDFS {
	unsigned int id; // ID of the original input graph
	Edge *edge;
	PDFS *prev;
	PDFS() :
			id(0), edge(0), prev(0) {
	}
	;
};

/*
 * History is merely a tool class, there are three fields in it:
 * 	vector<Edge*>, vector<bool> edge and vector<bool> vertex.
 *
 * 	vector<Edge*> contains all edges in the occurrence of current pattern(in the DFSCode order)
 * 	vector<bool> edge: indicates whether a edge is in the occurrence (true if included)
 * 	vector<bool> vertex: similar to vector<bool> edge
 *
 */
class History: public std::vector<Edge*> {
private:
	std::vector<int> edge;
	std::vector<int> vertex;

public:
	bool hasEdge(unsigned int id) {
		return (bool) edge[id];
	}
	bool hasVertex(unsigned int id) {
		return (bool) vertex[id];
	}
	void build(Graph &, PDFS *);
	History() {
	}
	;
	History(Graph& g, PDFS *p) {
		build(g, p);
	}
};

/**
 *
 * the occurrence of current DFSCode
 * is contained in a vector.
 *
 */
class Projected: public std::vector<PDFS> {
public:
	void push(int id, Edge *edge, PDFS *prev) {
		resize(size() + 1);
		PDFS &d = (*this)[size() - 1];
		d.id = id;
		d.edge = edge;
		d.prev = prev;
	}
};

/*  class FrequentSet {
 private:
 std::vector <unsigned> frequent1;
 std::map <unsigned, std::set<int> > frequent2;

 public:
 void push (unsigned int);                 // set single item
 void push (unsigned int, unsigned int);   // set two nodes
 }; */

typedef std::vector<Edge*> EdgeList;

#ifdef DIRECTED
bool get_forward_pure(Graph&, int, int, History&, EdgeList &);
Edge *get_backward(Graph&, Edge *, Edge *, History&, int, int);
bool get_forward_rmpath(Graph&, int v1, int v2, Edge *, int, History&,
		EdgeList &);
#else
bool get_forward_pure(Graph&, Edge *, int, History&, EdgeList &);
Edge *get_backward(Graph&, Edge *, Edge *, History&);
bool get_forward_rmpath(Graph&, Edge *, int, History&, EdgeList &);
#endif

bool get_forward_root(Graph&, Vertex&, EdgeList &);

class gSpan {

private:
#ifdef DIRECTED
	typedef std::map<int,
			std::map<int, std::map<int, std::map<char, Projected> > > > Projected_map4;
	typedef std::map<int, std::map<int, std::map<char, Projected> > > Projected_map3;
	typedef std::map<int, std::map<char, Projected> > Projected_map2;
	typedef std::map<char, Projected> Projected_map1;

	typedef std::map<int,
			std::map<int, std::map<int, std::map<char, Projected> > > >::iterator Projected_iterator4;
	typedef std::map<int, std::map<int, std::map<char, Projected> > >::iterator Projected_iterator3;
	typedef std::map<int, std::map<char, Projected> >::iterator Projected_iterator2;
	typedef std::map<char, Projected>::iterator Projected_iterator1;

	typedef std::map<int,
			std::map<int, std::map<int, std::map<char, Projected> > > >::reverse_iterator Projected_riterator4;

	void outputmp4(Projected_map4, std::string prefix = "[",
			std::string suffix = "]", std::string indent = "");
	void outputmp3(Projected_map3, std::string prefix = "[",
			std::string suffix = "]", std::string indent = "");
	void outputmp2(Projected_map2, std::string prefix = "[",
			std::string suffix = "]", std::string indent = "");
	void outputmp1(Projected_map1, std::string prefix = "[",
			std::string suffix = "]", std::string indent = "");
	void outputProjected(Projected, std::string prefix = "[",
			std::string suffix = "]", std::string indent = "");

	void outputdfscode(DFSCode&);

#else

	typedef std::map<int, std::map<int, std::map<int, Projected> > > Projected_map3;
	typedef std::map<int, std::map<int, Projected> > Projected_map2;
	typedef std::map<int, Projected> Projected_map1;

	typedef std::map<int, std::map<int, std::map<int, Projected> > >::iterator Projected_iterator3;
	typedef std::map<int, std::map<int, Projected> >::iterator Projected_iterator2;
	typedef std::map<int, Projected>::iterator Projected_iterator1;

	typedef std::map<int, std::map<int, std::map<int, Projected> > >::reverse_iterator Projected_riterator3;
#endif

	std::vector<Graph> TRANS;
	std::vector<char> labelset;
	DFSCode DFS_CODE;
	DFSCode DFS_CODE_IS_MIN;
	Graph GRAPH_IS_MIN;

#ifdef SIMULATION
	Graph currentGraph; //save the current graph of dfscode.
#endif

	unsigned int ID;
	unsigned int minsup;
	unsigned int maxpat_min; // lower bound on node count
	unsigned int maxpat_max; // upper bound on node count
	bool where;
	bool enc;
	bool directed;
	std::ostream* os;

	/* Singular vertex handling stuff
	 * [graph][vertexlabel] = count.
	 */
	std::map<unsigned int, std::map<unsigned int, unsigned int> > singleVertex;
	std::map<unsigned int, unsigned int> singleVertexLabel;

	std::vector<Graph> bestGraphs;
	std::vector<double> bestGraphsY;
	std::vector<double> bestGraphsGain;
	std::vector<std::map<unsigned int, unsigned int> > bestGraphsCounts;

	/* Transparent pointers for gain function and gain bound.
	 */
	double (gSpan::*gain)(Projected &projected, double y);
	double (gSpan::*gainbound)(Projected &projected);

	void report_single(Graph &g, unsigned int sup);

	bool is_min();
	bool project_is_min(Projected &);

	std::map<unsigned int, unsigned int> support_counts(Projected &projected);
	unsigned int support(Projected&);
	void project(Projected &);
	void report(Projected &, unsigned int);
	void store(Projected &, unsigned int);

	std::istream &read(std::istream &);

	void run_intern(void);

public:
	gSpan(void);
#ifdef SIMULATION
	void run(unsigned int _minsup, unsigned int _maxpat_min,
			unsigned int _maxpat_max, bool _enc, bool _where, bool _directed);
#else
	void run(std::istream &is, std::ostream &_os, unsigned int _minsup,
			unsigned int _maxpat_min, unsigned int _maxpat_max, bool _enc,
			bool _where, bool _directed);
#endif
};
}
;

