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

int main() {
    test_addi32();
}
