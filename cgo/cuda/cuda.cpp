/* 
 * Copyright 2021 Matrix Origin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define MO_CL_CUDA

#include <math.h>

extern "C" 
{
#include "../xcall.h"
#include "../bitmap.h"
}

#include <string.h>
#include <cstring>
#include <iostream>
#include <unistd.h>
#include <libgen.h>

// CUDA headers.
#include <cuda_runtime_api.h>
#include <cuda.h>
#include <helper_cuda_drvapi.h>
#include <helper_functions.h>
#include <builtin_types.h>

#define FATBIN_FILE "mocl_kernel64.fatbin"

using namespace std;

#ifndef TEST_RUN
#define MOCL_CHECK_RET(_x, _code, _errBuf) if (true) { \
    CUresult cuErr = (_x);                                              \
    if (cuErr != CUDA_SUCCESS) {                                        \
        const char *_errStr = NULL;                                     \
        cuGetErrorString(cuErr, &_errStr);                              \
        int nbErr = snprintf((char *) _errBuf+1 , 255,                  \
                            "CUDA error %d: %s", cuErr, _errStr);       \
        if (nbErr > 255) {                                              \
            nbErr = 255;                                                \
        }                                                               \
        _errBuf[0] = (uint8_t) nbErr;                                   \
        return _code;                                                    \
    }                                                                   \
} else 
#else
#define MOCL_CHECK_RET(x, code, errBuf) checkCudaErrors(x)
#endif

typedef struct CudaEnv {
    CUdevice cuDevice;
    CUcontext cuContext;
    CUmodule cuModule;
    CUfunction l2distance_f32; 
    CUfunction l2distance_f64;
    CUfunction l2distance_f32_const;
    CUfunction l2distance_f64_const;
} CudaEnv;

int32_t CudaEnv_init(CudaEnv *env, uint8_t *errBuf) {
    MOCL_CHECK_RET(cuInit(0), -1000, errBuf);
    env->cuDevice = findCudaDeviceDRV(0, NULL);
#if (CUDART_VERSION < 13000)
    MOCL_CHECK_RET(cuCtxCreate(&env->cuContext, 0, env->cuDevice), -1001, errBuf);
#else
    MOCL_CHECK_RET(cuCtxCreate(&env->cuContext, nullptr, 0, env->cuDevice), -1001, errBuf);
#endif

#ifdef __linux__
    char mobin_path[PATH_MAX];
    ssize_t nb = readlink("/proc/self/exe", mobin_path, PATH_MAX);
    if (nb == -1) {
        int nbErr = snprintf((char *) errBuf+1, 255, "readlink error %d", errno);
        errBuf[0] = (uint8_t) nbErr;
        return -1;
    }
    string fatbin_path = string(dirname(mobin_path)) + "/" + FATBIN_FILE;
#else
    // Not implemented yet.
    return -1;
#endif
    MOCL_CHECK_RET(cuModuleLoad(&env->cuModule, fatbin_path.c_str()), -1002, errBuf);
    MOCL_CHECK_RET(cuModuleGetFunction(&env->l2distance_f32, env->cuModule, "l2distance_f32"), -1003, errBuf);
    MOCL_CHECK_RET(cuModuleGetFunction(&env->l2distance_f64, env->cuModule, "l2distance_f64"), -1004, errBuf);
    MOCL_CHECK_RET(cuModuleGetFunction(&env->l2distance_f32_const, env->cuModule, "l2distance_f32_const"), -1005, errBuf);
    MOCL_CHECK_RET(cuModuleGetFunction(&env->l2distance_f64_const, env->cuModule, "l2distance_f64_const"), -1006, errBuf);
    return 0;
} 

CudaEnv *newCudaEnv(uint8_t *errBuf) {
    CudaEnv *env = (CudaEnv *) malloc(sizeof(CudaEnv));

    env->cuDevice = 0;
    env->cuContext = 0;
    env->cuModule = 0;
    env->l2distance_f32 = 0;
    env->l2distance_f64 = 0;
    env->l2distance_f32_const = 0;
    env->l2distance_f64_const = 0;
    if (CudaEnv_init(env, errBuf) != 0) {
        free(env);
        return NULL;
    }
    return env;
}

static uint8_t gErrBuf[256]; 
static CudaEnv *gCudaEnv = newCudaEnv(gErrBuf);

int32_t cuda_l2distance_impl(uint8_t *errBuf, double *pres, int n, int vecsz, bool sq,
        varlena_t *p1, uint8_t *area1, bool isconst1,
        varlena_t *p2, uint8_t *area2, bool isconst2,
        int nbits
        ) {

    if (gCudaEnv == NULL) {
        memcpy(errBuf, gErrBuf, 256);
        return -1111;
    }

    // Allocate device memory.
    CUdeviceptr d_pres, d_p1, d_p2, d_a1, d_a2;
    ptrlen_t c1, c2;

    MOCL_CHECK_RET(cuCtxSetCurrent(gCudaEnv->cuContext), -1999, errBuf); ;   

    MOCL_CHECK_RET(cuMemAlloc(&d_pres, n * sizeof(double)), -2000, errBuf);
    if (isconst1) {
        varlena_get_ptrlen(p1, area1, &c1);
        MOCL_CHECK_RET(cuMemAlloc(&d_a1, vecsz), -2001, errBuf);
        MOCL_CHECK_RET(cuMemcpyHtoD(d_a1, c1.ptr, vecsz), -2002, errBuf);
    } else {
        MOCL_CHECK_RET(cuMemAlloc(&d_p1, n * VARLENA_SZ), -2003, errBuf);
        MOCL_CHECK_RET(cuMemAlloc(&d_a1, n * vecsz), -2003, errBuf);
        MOCL_CHECK_RET(cuMemcpyHtoD(d_p1, (void *)p1, n * VARLENA_SZ), -2003, errBuf);
        MOCL_CHECK_RET(cuMemcpyHtoD(d_a1, (void *)area1, n * vecsz), -2003, errBuf);
    }

    if (isconst2) {
        varlena_get_ptrlen(p2, area2, &c2);
        MOCL_CHECK_RET(cuMemAlloc(&d_a2, vecsz), -2004, errBuf);
        MOCL_CHECK_RET(cuMemcpyHtoD(d_a2, c2.ptr, vecsz), -2004, errBuf);
    } else {
        MOCL_CHECK_RET(cuMemAlloc(&d_p2, n * VARLENA_SZ), -2005, errBuf);
        MOCL_CHECK_RET(cuMemAlloc(&d_a2, n * vecsz), -2005, errBuf);
        MOCL_CHECK_RET(cuMemcpyHtoD(d_p2, (void *)p2, n * VARLENA_SZ), -2005, errBuf);
        MOCL_CHECK_RET(cuMemcpyHtoD(d_a2, (void *)area2, n * vecsz), -2005, errBuf);
    }

    int threadsPerBlock = CUDA_THREADS_PER_BLOCK;   
    int blocksPerGrid = (n + threadsPerBlock - 1) / threadsPerBlock;

    if (nbits == 32) {
        if (isconst1) {
            // swap p1 and p2, leave const the last arg.
            void *args[] = {&d_pres, &n, &vecsz, &sq, &d_p2, &d_a2, &d_a1};
            MOCL_CHECK_RET(cuLaunchKernel(gCudaEnv->l2distance_f32_const, blocksPerGrid, 1, 1, threadsPerBlock, 1, 1, 0, NULL, args, NULL), -2009, errBuf);
        } else if (isconst2) {
            void *args[] = {&d_pres, &n, &vecsz, &sq, &d_p1, &d_a1, &d_a2};
            MOCL_CHECK_RET(cuLaunchKernel(gCudaEnv->l2distance_f32_const, blocksPerGrid, 1, 1, threadsPerBlock, 1, 1, 0, NULL, args, NULL), -2010, errBuf);
        } else {
            void *args[] = {&d_pres, &n, &vecsz, &sq, &d_p1, &d_a1, &d_p2, &d_a2};
            MOCL_CHECK_RET(cuLaunchKernel(gCudaEnv->l2distance_f32, blocksPerGrid, 1, 1, threadsPerBlock, 1, 1, 0, NULL, args, NULL), -2011, errBuf);
        } 
    } else {
        if (isconst1) {
            // swap p1 and p2, leave const the last arg.
            void *args[] = {&d_pres, &n, &vecsz, &sq, &d_p2, &d_a2, &d_a1};
            MOCL_CHECK_RET(cuLaunchKernel(gCudaEnv->l2distance_f64_const, blocksPerGrid, 1, 1, threadsPerBlock, 1, 1, 0, NULL, args, NULL), -2009, errBuf);
        } else if (isconst2) {
            void *args[] = {&d_pres, &n, &vecsz, &sq, &d_p1, &d_a1, &d_a2};
            MOCL_CHECK_RET(cuLaunchKernel(gCudaEnv->l2distance_f64_const, blocksPerGrid, 1, 1, threadsPerBlock, 1, 1, 0, NULL, args, NULL), -2010, errBuf);
        } else {
            void *args[] = {&d_pres, &n, &vecsz, &sq, &d_p1, &d_a1, &d_p2, &d_a2};
            MOCL_CHECK_RET(cuLaunchKernel(gCudaEnv->l2distance_f64, blocksPerGrid, 1, 1, threadsPerBlock, 1, 1, 0, NULL, args, NULL), -2011, errBuf);
        } 
    }

    MOCL_CHECK_RET(cuMemcpyDtoH(pres, d_pres, n * sizeof(double)), -2015, errBuf);

    MOCL_CHECK_RET(cuMemFree(d_pres), -2016, errBuf);
    MOCL_CHECK_RET(cuMemFree(d_p1), -2017, errBuf);
    MOCL_CHECK_RET(cuMemFree(d_p2), -2018, errBuf);
    MOCL_CHECK_RET(cuMemFree(d_a1), -2017, errBuf);
    MOCL_CHECK_RET(cuMemFree(d_a2), -2018, errBuf);

    return 0;
}

extern "C"
int32_t cuda_l2distance_f32(uint8_t *errBuf, double *pres, int n, int vecsz, bool sq,
        varlena_t *p1, uint8_t *area1, bool isconst1,
        varlena_t *p2, uint8_t *area2, bool isconst2
        ) {
    return cuda_l2distance_impl(errBuf,
        pres, n, vecsz, sq, p1, area1, isconst1, p2, area2, isconst2, 32);
}

extern "C"
int32_t cuda_l2distance_f64(uint8_t *errBuf, double *pres, int n, int vecsz, bool sq,
        varlena_t *p1, uint8_t *area1, bool isconst1,
        varlena_t *p2, uint8_t *area2, bool isconst2
        ) {
    return cuda_l2distance_impl(errBuf,
        pres, n, vecsz, sq, p1, area1, isconst1, p2, area2, isconst2, 64);
}

#ifdef TEST_RUN
int main(int argc, char **argv) {
    double *pres;
    varlena_t *p1, *p2;
    uint8_t *area1, *area2;

    int n = 8192;
    int vecsize = 128 * sizeof(float);
    int32_t ret;
    uint8_t errBuf[256];

    pres = (double *) malloc(n * sizeof(double));
    p1 = (varlena_t *) malloc(n * sizeof(varlena_t));
    p2 = (varlena_t *) malloc(n * sizeof(varlena_t));
    area1 = (uint8_t *) malloc(n * vecsize);
    area2 = (uint8_t *) malloc(n * vecsize);

    for (int i = 0; i < n; i++) {
        for (int j = 0; j < 128; j++) {
            ((float *) area1)[i*128+j] = i + j;
            ((float *) area2)[i*128+j] = i + j + 1;
        }

        uint32_t *pp1 = (uint32_t *) &p1[i].bs[0];
        uint32_t *pp2 = (uint32_t *) &p2[i].bs[0];

        pp1[0] = 0xffffffff;
        pp2[0] = 0xffffffff;
        pp1[1] = i*vecsize;
        pp2[1] = i*vecsize;
        pp1[2] = vecsize;
        pp2[2] = vecsize;
    }

    for (int loop = 0; loop < 100; loop++) {
        ret = cuda_l2distance_f32(errBuf, pres, n, vecsize, false, p1, area1, false, p2, area2, false);
        if (ret != 0) {
            cout << "Error: " << ret << endl << string((char *) errBuf+1, errBuf[0]) << endl;
        } else {
            cout << "Loop: " << loop << endl;
            for (int i = 0; i < 2; i++) {
                cout << pres[i] << endl;
            }
        }
    }
}
#endif  

