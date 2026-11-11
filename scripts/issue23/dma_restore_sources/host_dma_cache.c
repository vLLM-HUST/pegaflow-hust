/* Driver-owned pinned host memory: A/B/B/A with supported DMA pre-registration. */
#define _GNU_SOURCE
#include <acl/acl.h>
#include <ascend_hal.h>
#include <dlfcn.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
static uint64_t ns(void){struct timespec t;clock_gettime(CLOCK_MONOTONIC,&t);return (uint64_t)t.tv_sec*1000000000ULL+t.tv_nsec;}
#define CHECK(x) do{int e=(x);if(e){fprintf(stderr,"%s=%d\n",#x,e);return 1;}}while(0)
int main(int argc,char**argv){
 alarm(240);int device=argc>1?atoi(argv[1]):1;size_t n=15456,piece=262144,bytes=n*piece;
 void*allocation=NULL,*host=NULL,*alias=NULL,*dest=NULL;aclrtStream stream=NULL;
 CHECK(aclInit(NULL));CHECK(aclrtSetDevice(device));CHECK(aclrtMallocHost(&allocation,bytes+4096));host=(void*)(((uintptr_t)allocation+4095)&~(uintptr_t)4095);memset(host,0x5a,bytes);
 CHECK(aclrtMalloc(&dest,bytes,ACL_MEM_MALLOC_HUGE_FIRST));CHECK(aclrtCreateStream(&stream));
 typedef drvError_t(*Reg)(void*,UINT64,UINT32,UINT32,void**);typedef drvError_t(*Unreg)(void*,UINT32,UINT32);
 Reg reg=(Reg)dlsym(RTLD_DEFAULT,"halHostRegister");Unreg unreg=(Unreg)dlsym(RTLD_DEFAULT,"halHostUnregisterEx");if(!reg||!unreg)return 3;
 int registered=0;
 for(int trial=0;trial<4;trial++){
  if(trial==1){uint64_t a=ns();int e=reg(host,bytes,HOST_MEM_MAP_DMA,device,&alias);printf("{\"event\":\"dma_register\",\"result\":%d,\"wall_ns\":%llu}\n",e,(unsigned long long)(ns()-a));fflush(stdout);if(e)return 4;registered=1;}
  if(trial==3){uint64_t a=ns();int e=unreg(host,device,HOST_MEM_MAP_DMA);printf("{\"event\":\"dma_unregister\",\"result\":%d,\"wall_ns\":%llu}\n",e,(unsigned long long)(ns()-a));fflush(stdout);if(e)return 5;registered=0;}
  for(int j=0;j<8;j++){size_t block=(size_t)j*(n-1)/7;CHECK(aclrtMemsetAsync((char*)dest+block*piece,8,0,8,stream));}CHECK(aclrtSynchronizeStream(stream));
  uint64_t start=ns();printf("{\"event\":\"copy_begin\",\"trial\":%d,\"registered\":%d,\"start_ns\":%llu}\n",trial,registered,(unsigned long long)start);fflush(stdout);
  for(size_t i=0;i<n;i++)CHECK(aclrtMemcpyAsync((char*)dest+i*piece,piece,(char*)host+i*piece,piece,ACL_MEMCPY_HOST_TO_DEVICE,stream));
  uint64_t submitted=ns();CHECK(aclrtSynchronizeStream(stream));uint64_t end=ns();
  for(int j=0;j<8;j++){size_t block=(size_t)j*(n-1)/7;uint64_t value=0;CHECK(aclrtMemcpy(&value,8,(char*)dest+block*piece,8,ACL_MEMCPY_DEVICE_TO_HOST));if(value!=0x5a5a5a5a5a5a5a5aULL){fprintf(stderr,"copy verification failed\n");return 6;}}
  printf("{\"event\":\"copy\",\"trial\":%d,\"registered\":%d,\"descriptors\":%zu,\"bytes\":%zu,\"verified_tags\":8,\"start_ns\":%llu,\"end_ns\":%llu,\"api_ns\":%llu,\"sync_ns\":%llu,\"wall_ns\":%llu}\n",trial,registered,n,bytes,(unsigned long long)start,(unsigned long long)end,(unsigned long long)(submitted-start),(unsigned long long)(end-submitted),(unsigned long long)(end-start));fflush(stdout);
 }
 CHECK(aclrtDestroyStream(stream));CHECK(aclrtFree(dest));CHECK(aclrtFreeHost(allocation));CHECK(aclrtResetDevice(device));CHECK(aclFinalize());puts("{\"event\":\"completed\",\"cases\":4}");return 0;
}
