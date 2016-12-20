#ifndef __READER_H__
#define __READER_H__

//#include "TDFAPIStruct.h"

#ifdef __cplusplus
extern "C" {
#endif

   void* trace(const char* addr, const char* port, const char* username, const char* password);
   void close_handler(void* handler);

#ifdef __cplusplus
}
#endif

#endif