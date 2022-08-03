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

#include <stddef.h>
#include <stdio.h>
#include <limits.h>
#include "../mo.h"

int test_addi32() {
    int32_t r[8192];
    int32_t a[8192];
    int32_t b[8192];
    for (int i = 0; i < 8192; i++) {
        a[i] = i;
        b[i] = 1;
    }

    int32_t rc = SignedInt_VecAdd(r, a, b, 8192, NULL, 0, 4);
    return rc;
}

int test_adddec64() {
    int64_t r[8192];
    int64_t a[8192];
    int64_t b[8192];
    for (int i = 0; i < 8192; i++) {
        a[i] = -i;
        b[i] = i;
    }

    int32_t rc = Decimal64_VecAdd(r, a, b, 8192, NULL, 0);
    return rc;
}


int test_subdec64() {
    int64_t r[8192];
    int64_t a[8192];
    int64_t b[8192];
    for (int i = 0; i < 8192; i++) {
            a[i] = i;
            b[i] = i*3;
    }
    int32_t rc = Decimal64_VecSub(r, a, b, 8192, NULL, 0);
    return rc;
}

int test_divedec64(){
    int64_t r[10];
    int64_t a[10];
    int64_t b[10];
    for (int i = 0; i < 10; i++) {
        a[i] = (i+1)*1024;
        b[i] = 2;
    }
    int32_t rc = Decimal64_VecDiv(r, a, b, 10, NULL, 0);

    for (int i = 0; i < 10; i++) {
        printf("%ld / %ld = ", a[i], b[i]);
        printf("r[%d]=%ld \n", i, r[i]);
    }

    return rc;
}

int main() {
//    test_addi32();
     //test_subdec64();
     //test_divedec64();
     test_adddec64();
     return 0;
}
