/*for scc algorithm
 */
//#include "pregel_app_hashmin.h"
//
//int main(int argc, char* argv[]){
//	init_workers();
//	pregel_hashmin("/toyFolder", "/toyOutput", true);
//	worker_finalize();
//	return 0;
//}

/*for sssp algorithm
 */
#include "pregelplus_similation_do.h"


#ifdef SIMULATION
int main(int argc, char* argv[]) {
#else
	int main1(int argc, char* argv[]) {
#endif
	init_workers();

	if(get_worker_id() == MASTER_RANK){
		if(argc>1){
			GSPAN::resultfile=argv[1];
		}
	}

#ifdef little
	pregel_similation("/simgraph3", "/simresult", false);
#else
	pregel_similation("/simgraph5", "/simresult", false);
#endif
	worker_finalize();
	return 0;
}
