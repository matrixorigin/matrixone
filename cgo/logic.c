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

#include "mo_impl.h"

/*
   Logical and operator truth table
    A     |  B     |  A AND B
    ------|--------|----------
    true  | true   |  true
    true  | false  |  false
    true  | null   |  null
    false | true   |  false
    false | false  |  false
    false | null   |  false
    null  | true   |  null
    null  | false  |  false
    null  | null   |  null
*/
int32_t Logic_VecAnd(void *r, void *a, void  *b, uint64_t n, uint64_t *anulls, uint64_t *bnulls, uint64_t *rnulls, int32_t flag) {
    bool *rt = (bool *) r;
    bool *at = (bool *) a;
    bool *bt = (bool *) b;
    if ((flag & LEFT_IS_SCALAR) != 0) {
        for (uint64_t i = 0; i < n; i++) {
            rt[i] = at[0] && bt[i];
        }
        if (rnulls != NULL && !at[0]) {
            for (uint64_t i = 0; i < n; i++) {
                if (Bitmap_Contains(rnulls, i)) {
                    Bitmap_Remove(rnulls, i);
                }
            }
        }
    } else if ((flag & RIGHT_IS_SCALAR) != 0) {
        for (uint64_t i = 0; i < n; i++) {
            rt[i] = at[i] && bt[0];
        }
        if (rnulls != NULL && !bt[0]) {
            for (uint64_t i = 0; i < n; i++) {
                if (Bitmap_Contains(rnulls, i)) {
                    Bitmap_Remove(rnulls, i);
                }
            }
        }
    } else {
        for (uint64_t i = 0; i < n; i++) {
            rt[i] = at[i] && bt[i];
        }
        if (anulls != NULL && bnulls != NULL) {
            for (uint64_t i = 0; i < n; i++) {
                if (Bitmap_Contains(anulls, i) && !Bitmap_Contains(bnulls, i)) {
                    if (!bt[i]) {
                        Bitmap_Remove(rnulls, i);
                    }
                }

                if (Bitmap_Contains(bnulls, i) && !Bitmap_Contains(anulls, i)) {
                    if (!at[i]) {
                        Bitmap_Remove(rnulls, i);
                    }
                }
            }
        } else if (anulls != NULL) {
            for (uint64_t i = 0; i < n; i++) {
                if (!bt[i] && Bitmap_Contains(anulls, i)) {
                    Bitmap_Remove(rnulls, i);
                }
            }
        } else if (bnulls != NULL) {
            for (uint64_t i = 0; i < n; i++) {
                if (!at[i] && Bitmap_Contains(bnulls, i)) {
                    Bitmap_Remove(rnulls, i);
                }
            }
        }
    }
    return RC_SUCCESS;
}

/*
 Logical or operator truth table
    A     |  B     |  A OR B
    ------|--------|----------
    true  | true   |  true
    true  | false  |  true
    true  | null   |  true
    false | true   |  true
    false | false  |  false
    false | null   |  null
    null  | true   |  true
    null  | false  |  null
    null  | null   |  null
*/
int32_t Logic_VecOr(void *r, void *a, void  *b, uint64_t n, uint64_t *anulls, uint64_t *bnulls, uint64_t *rnulls, int32_t flag) {
    bool *rt = (bool *) r;
    bool *at = (bool *) a;
    bool *bt = (bool *) b;
    if ((flag & LEFT_IS_SCALAR) != 0) {
        for (uint64_t i = 0; i < n; i++) {
            rt[i] = at[0] || bt[i];
        }
        if (rnulls != NULL && at[0]) {
            for (uint64_t i = 0; i < n; i++) {
                if (Bitmap_Contains(rnulls, i)) {
                    Bitmap_Remove(rnulls, i);
                }
            }
        }
    } else if ((flag & RIGHT_IS_SCALAR) != 0) {
        for (uint64_t i = 0; i < n; i++) {
            rt[i] = at[i] || bt[0];
        }
        if (rnulls != NULL && bt[0]) {
            for (uint64_t i = 0; i < n; i++) {
                if (Bitmap_Contains(rnulls, i)) {
                    Bitmap_Remove(rnulls, i);
                }
            }
        }
    } else {
        for (uint64_t i = 0; i < n; i++) {
            rt[i] = at[i] || bt[i];
        }
        if (anulls != NULL && bnulls != NULL) {
            for (uint64_t i = 0; i < n; i++) {
                if (Bitmap_Contains(anulls, i) &&  !Bitmap_Contains(bnulls, i)) {
                    if (bt[i]) {
                        Bitmap_Remove(rnulls, i);
                    }
                }

                if (Bitmap_Contains(bnulls, i) &&  !Bitmap_Contains(anulls, i)) {
                    if (at[i]) {
                        Bitmap_Remove(rnulls, i);
                    }
                }
            }
        } else if (anulls != NULL) {
            for (uint64_t i = 0; i < n; i++) {
                if (bt[i] && Bitmap_Contains(anulls, i)) {
                    Bitmap_Remove(rnulls, i);
                }
            }
        } else if (bnulls != NULL) {
            for (uint64_t i = 0; i < n; i++) {
                if (at[i] && Bitmap_Contains(bnulls, i)) {
                    Bitmap_Remove(rnulls, i);
                }
            }
        }
    }
    return RC_SUCCESS;
}

/*
 Logical exclusive or truth table
    A     |  B     |  A XOR B
    ------|--------|----------
    true  | true   |  false
    true  | false  |  true
    true  | null   |  null
    false | true   |  true
    false | false  |  false
    false | null   |  null
    null  | true   |  true
    null  | false  |  null
    null  | null   |  null
*/
int32_t Logic_VecXor(void *r, void *a, void  *b, uint64_t n, uint64_t *nulls, int32_t flag) {
    bool *rt = (bool *) r;
    bool *at = (bool *) a;
    bool *bt = (bool *) b;
    if ((flag & LEFT_IS_SCALAR) != 0) {
        for (uint64_t i = 0; i < n; i++) {
            rt[i] = (at[0] || bt[i]) && !(at[0] && bt[i]);
        }
    } else if ((flag & RIGHT_IS_SCALAR) != 0) {
        for (uint64_t i = 0; i < n; i++) {
            rt[i] = (at[i] || bt[0]) && !(at[i] && bt[0]);
        }
    } else {
        for (uint64_t i = 0; i < n; i++) {
            rt[i] = (at[i] || bt[i]) && !(at[i] && bt[i]);
        }
    }
    return RC_SUCCESS;
}

/*
    Logical not truth table
    P     |   NOT P
    ----------------
    true  |   false
    false |   true
    null  |   null
*/
int32_t Logic_VecNot(void *r, void *a, uint64_t n, uint64_t *nulls, int32_t flag) {
    bool *rt = (bool *) r;
    bool *at = (bool *) a;
    if ((flag & LEFT_IS_SCALAR) != 0) {
        rt[0] = !at[0];
    } else {
        for (uint64_t i = 0; i < n; i++) {
            rt[i] = !at[i];
        }
    }
    return RC_SUCCESS;
}