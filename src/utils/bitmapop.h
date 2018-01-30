#ifndef BITMAPOP
#define BIGMAPOP
#include <string>
namespace zjh {
std::string bitmap2string(unsigned int a) {
	std::string s;
	for (int i = 31; i >= 0; i--) {
		if(a & 1<<i){
			s+="1";
		}else{
			s+="0";
		}
		if (i % 4 == 0)
			s+=" ";
	}
	return s;
}
}
#endif
