/* Fixed immutable-source diagnosis only. Own pinned shadow with DMA pre-registration.
 * Policy29 copies the source pool once and redirects H2D source pointers.
 * Policy0 releases the shadow after the preceding load has completed.
 * The finite probe must serialize loads and preserve the source pool contents. */
#define _GNU_SOURCE
#include <acl/acl_rt.h>
#include <ascend_hal.h>
#include <dlfcn.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
static uintptr_t source_base;static size_t source_bytes;static void*allocation,*shadow;static int device=-1,fd=-1;static int dma_registered;
static pthread_mutex_t mutex=PTHREAD_MUTEX_INITIALIZER;
static uint64_t stamp(void){struct timespec t;clock_gettime(CLOCK_MONOTONIC,&t);return(uint64_t)t.tv_sec*1000000000ULL+t.tv_nsec;}
static void*symbol(const char*n){void*p=dlsym(RTLD_NEXT,n);if(!p)_Exit(125);return p;}
static void emit(const char*event,uint64_t a,uint64_t b,int error){if(fd<0)return;char line[500];int n=snprintf(line,sizeof(line),"{\"event\":\"%s\",\"start_ns\":%llu,\"end_ns\":%llu,\"bytes\":%zu,\"error\":%d}\n",event,(unsigned long long)a,(unsigned long long)b,source_bytes,error);write(fd,line,n);}
__attribute__((constructor))static void init(void){const char*p=getenv("NATIVE_COPY_TRACE_LOG");if(p)fd=open(p,O_WRONLY|O_CREAT|O_APPEND,0600);}
aclError aclrtHostRegister(void*ptr,uint64_t bytes,aclrtHostRegisterType type,void**alias){
 typedef aclError(*Fn)(void*,uint64_t,aclrtHostRegisterType,void**);Fn fn=(Fn)symbol("aclrtHostRegister");aclError e=fn(ptr,bytes,type,alias);
 if(!e&&bytes>=4UL*1024*1024*1024){source_base=(uintptr_t)ptr;source_bytes=bytes;}return e;
}
static int activate(int want_dma){
 if(!getenv("CAUSAL_IMMUTABLE_SOURCE"))return 100000;
 typedef aclError(*Allocate)(void**,size_t);typedef aclError(*GetDevice)(int32_t*);typedef drvError_t(*Register)(void*,UINT64,UINT32,UINT32,void**);typedef drvError_t(*Unreg)(void*,UINT32,UINT32);
 if(!shadow){uint64_t a=stamp();int e=((GetDevice)symbol("aclrtGetDevice"))(&device);if(e)return e;e=((Allocate)symbol("aclrtMallocHost"))(&allocation,source_bytes+4096);if(e)return e;shadow=(void*)(((uintptr_t)allocation+4095)&~(uintptr_t)4095);emit("shadow_allocate",a,stamp(),0);a=stamp();memcpy(shadow,(void*)source_base,source_bytes);emit("shadow_seed_copy",a,stamp(),0);}
 if(want_dma&&!dma_registered){void*unused=NULL;uint64_t a=stamp();int e=((Register)symbol("halHostRegister"))(shadow,source_bytes,HOST_MEM_MAP_DMA,device,&unused);emit("shadow_dma_register",a,stamp(),e);if(e)return e;dma_registered=1;}
 if(!want_dma&&dma_registered){uint64_t a=stamp();int e=((Unreg)symbol("halHostUnregisterEx"))(shadow,device,HOST_MEM_MAP_DMA);emit("shadow_dma_unregister",a,stamp(),e);if(e)return e;dma_registered=0;}
 return 0;
}
static int deactivate(void){
 if(!shadow)return 0;typedef aclError(*Free)(void*);
 int e=activate(0);if(e)return e;uint64_t a=stamp();e=((Free)symbol("aclrtFreeHost"))(allocation);emit("shadow_free",a,stamp(),e);if(!e){shadow=NULL;allocation=NULL;}return e;
}
aclError aclrtMemcpyBatchAsync(void**dst,size_t*maximum,void**src,size_t*sizes,size_t count,aclrtMemcpyBatchAttr*attr,size_t*indices,size_t nattr,size_t*failed,aclrtStream stream){
 typedef aclError(*Fn)(void**,size_t*,void**,size_t*,size_t,aclrtMemcpyBatchAttr*,size_t*,size_t,size_t*,aclrtStream);Fn fn=(Fn)symbol("aclrtMemcpyBatchAsync");
 int h2d=attr&&indices&&nattr==1&&indices[0]==0&&attr[0].srcLoc.type==ACL_MEM_LOCATION_TYPE_HOST&&attr[0].dstLoc.type==ACL_MEM_LOCATION_TYPE_DEVICE;if(!h2d)return fn(dst,maximum,src,sizes,count,attr,indices,nattr,failed,stream);
 int mode=0;const char*path=getenv("NATIVE_COPY_POLICY");if(path){int f=open(path,O_RDONLY);if(f>=0){read(f,&mode,sizeof(mode));close(f);}}
 pthread_mutex_lock(&mutex);int e=0;if(mode==0){e=deactivate();pthread_mutex_unlock(&mutex);if(e)return e;return fn(dst,maximum,src,sizes,count,attr,indices,nattr,failed,stream);}
 if((mode!=29&&mode!=30)||!source_base||!source_bytes){pthread_mutex_unlock(&mutex);return 100000;}
 e=activate(mode==29);if(e){pthread_mutex_unlock(&mutex);return e;}
 void**redirect=malloc(count*sizeof(*redirect));if(!redirect){pthread_mutex_unlock(&mutex);return 100000;}
 for(size_t i=0;i<count;i++){uintptr_t p=(uintptr_t)src[i];if(p<source_base||sizes[i]>source_bytes||p-source_base>source_bytes-sizes[i]){free(redirect);pthread_mutex_unlock(&mutex);return 100000;}redirect[i]=(char*)shadow+(p-source_base);}
 e=fn(dst,maximum,redirect,sizes,count,attr,indices,nattr,failed,stream);free(redirect);pthread_mutex_unlock(&mutex);return e;
}
