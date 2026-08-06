// Copyright 2021 - 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <dlfcn.h>
#include <limits.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef int32_t UErrorCode;
typedef uint16_t UChar;
static const UChar empty_uchar = 0;
typedef struct URegularExpression URegularExpression;
typedef struct {
    int32_t line;
    int32_t offset;
    UChar preContext[16];
    UChar postContext[16];
} UParseError;

typedef URegularExpression *(*fn_open)(const UChar *, int32_t, uint32_t, UParseError *, UErrorCode *);
typedef void (*fn_close)(URegularExpression *);
typedef void (*fn_set_text)(URegularExpression *, const UChar *, int32_t, UErrorCode *);
typedef int8_t (*fn_find)(URegularExpression *, int32_t, UErrorCode *);
typedef int8_t (*fn_find_next)(URegularExpression *, UErrorCode *);
typedef int32_t (*fn_start_end)(URegularExpression *, int32_t, UErrorCode *);
typedef void (*fn_set_limit)(URegularExpression *, int32_t, UErrorCode *);
typedef int32_t (*fn_append_replacement)(
    URegularExpression *, const UChar *, int32_t, UChar **, int32_t *, UErrorCode *);
typedef int32_t (*fn_append_tail)(URegularExpression *, UChar **, int32_t *, UErrorCode *);

static struct {
    void *library;
    fn_open open;
    fn_close close;
    fn_set_text set_text;
    fn_find find;
    fn_find_next find_next;
    fn_start_end start;
    fn_start_end end;
    fn_set_limit set_time_limit;
    fn_set_limit set_stack_limit;
    fn_append_replacement append_replacement;
    fn_append_tail append_tail;
    int available;
} icu;

static pthread_once_t icu_once = PTHREAD_ONCE_INIT;

static void *load_symbol(const char *name) {
    void *symbol = dlsym(icu.library, name);
    if (symbol != NULL) {
        return symbol;
    }
    char versioned[96];
    for (int version = 99; version >= 40; --version) {
        snprintf(versioned, sizeof(versioned), "%s_%d", name, version);
        symbol = dlsym(icu.library, versioned);
        if (symbol != NULL) {
            return symbol;
        }
    }
    return NULL;
}

static void initialize_icu(void) {
    const char *libraries[] = {
        "libicui18n.so",
        "libicui18n.dylib",
        "/opt/homebrew/opt/icu4c/lib/libicui18n.dylib",
        "/usr/local/opt/icu4c/lib/libicui18n.dylib",
        "/usr/lib/libicucore.dylib",
    };
    for (size_t i = 0; i < sizeof(libraries) / sizeof(libraries[0]) && icu.library == NULL; ++i) {
        icu.library = dlopen(libraries[i], RTLD_NOW | RTLD_LOCAL);
    }
    for (int version = 99; version >= 40 && icu.library == NULL; --version) {
        char library[64];
        snprintf(library, sizeof(library), "libicui18n.so.%d", version);
        icu.library = dlopen(library, RTLD_NOW | RTLD_LOCAL);
    }
    if (icu.library == NULL) {
        return;
    }

#define LOAD(field, symbol)                                      \
    do {                                                         \
        *(void **)(&icu.field) = load_symbol(symbol);            \
        if (icu.field == NULL) {                                 \
            dlclose(icu.library);                                \
            icu.library = NULL;                                  \
            return;                                              \
        }                                                        \
    } while (0)

    LOAD(open, "uregex_open");
    LOAD(close, "uregex_close");
    LOAD(set_text, "uregex_setText");
    LOAD(find, "uregex_find");
    LOAD(find_next, "uregex_findNext");
    LOAD(start, "uregex_start");
    LOAD(end, "uregex_end");
    LOAD(set_time_limit, "uregex_setTimeLimit");
    LOAD(set_stack_limit, "uregex_setStackLimit");
    LOAD(append_replacement, "uregex_appendReplacement");
    LOAD(append_tail, "uregex_appendTail");
#undef LOAD

    icu.available = 1;
}

static int load_icu(void) {
    if (pthread_once(&icu_once, initialize_icu) != 0) {
        return 0;
    }
    return icu.available;
}

typedef struct mo_icu_regex {
    URegularExpression *regex;
    UChar *pattern;
    UChar *subject;
    int32_t subject_len;
} mo_icu_regex;

mo_icu_regex *mo_icu_regex_open(
    const UChar *pattern,
    int32_t pattern_len,
    uint32_t flags,
    int32_t time_limit,
    int32_t stack_limit,
    UErrorCode *status,
    int32_t *line,
    int32_t *offset) {
    if (!load_icu()) {
        *status = -1;
        return NULL;
    }
    UParseError parse_error;
    memset(&parse_error, 0, sizeof(parse_error));
    mo_icu_regex *result = (mo_icu_regex *)calloc(1, sizeof(mo_icu_regex));
    if (result == NULL) {
        *status = 7; // U_MEMORY_ALLOCATION_ERROR
        return NULL;
    }
    *status = 0;
    if (pattern_len > 0) {
        result->pattern = (UChar *)malloc((size_t)pattern_len * sizeof(UChar));
        if (result->pattern == NULL) {
            free(result);
            *status = 7;
            return NULL;
        }
        memcpy(result->pattern, pattern, (size_t)pattern_len * sizeof(UChar));
    }
    result->regex = icu.open(result->pattern, pattern_len, flags, &parse_error, status);
    *line = parse_error.line;
    *offset = parse_error.offset;
    if (*status > 0 || result->regex == NULL) {
        free(result->pattern);
        free(result);
        return NULL;
    }
    icu.set_time_limit(result->regex, time_limit, status);
    if (*status <= 0) {
        icu.set_stack_limit(result->regex, stack_limit, status);
    }
    if (*status > 0) {
        icu.close(result->regex);
        free(result->pattern);
        free(result);
        return NULL;
    }
    return result;
}

void mo_icu_regex_close(mo_icu_regex *regex) {
    if (regex == NULL) {
        return;
    }
    if (regex->regex != NULL) {
        icu.close(regex->regex);
    }
    free(regex->pattern);
    free(regex->subject);
    free(regex);
}

int mo_icu_regex_set_text(
    mo_icu_regex *regex, const UChar *subject, int32_t subject_len, UErrorCode *status) {
    free(regex->subject);
    regex->subject = NULL;
    regex->subject_len = subject_len;
    if (subject_len > 0) {
        regex->subject = (UChar *)malloc((size_t)subject_len * sizeof(UChar));
        if (regex->subject == NULL) {
            *status = 7;
            return 0;
        }
        memcpy(regex->subject, subject, (size_t)subject_len * sizeof(UChar));
    }
    *status = 0;
    const UChar *text = regex->subject == NULL ? &empty_uchar : regex->subject;
    icu.set_text(regex->regex, text, subject_len, status);
    return *status <= 0;
}

int mo_icu_regex_find(
    mo_icu_regex *regex,
    int32_t start,
    int32_t occurrence,
    int32_t *match_start,
    int32_t *match_end,
    UErrorCode *status) {
    *status = 0;
    int found = icu.find(regex->regex, start, status) != 0;
    for (int32_t i = 1; i < occurrence && found && *status <= 0; ++i) {
        found = icu.find_next(regex->regex, status) != 0;
    }
    if (*status > 0 || !found) {
        return found;
    }
    *match_start = icu.start(regex->regex, 0, status);
    if (*status <= 0) {
        *match_end = icu.end(regex->regex, 0, status);
    }
    return *status <= 0;
}

static int grow_buffer(UChar **buffer, int32_t *capacity, int32_t required) {
    if (*buffer != NULL && required <= *capacity) {
        return 1;
    }
    int32_t next = *capacity == 0 ? 64 : *capacity;
    while (next < required && next <= INT32_MAX / 2) {
        next *= 2;
    }
    if (next < required) {
        next = required;
    }
    UChar *grown = (UChar *)realloc(*buffer, (size_t)next * sizeof(UChar));
    if (grown == NULL) {
        return 0;
    }
    *buffer = grown;
    *capacity = next;
    return 1;
}

int mo_icu_regex_replace(
    mo_icu_regex *regex,
    const UChar *replacement,
    int32_t replacement_len,
    int32_t start,
    int32_t occurrence,
    UChar **output,
    int32_t *output_len,
    UErrorCode *status) {
    *output = NULL;
    *output_len = 0;
    *status = 0;
    if (replacement == NULL) {
        replacement = &empty_uchar;
    }
    int found = icu.find(regex->regex, start, status) != 0;
    for (int32_t i = 1; i < occurrence && found && *status <= 0; ++i) {
        found = icu.find_next(regex->regex, status) != 0;
    }
    if (*status > 0) {
        return 0;
    }
    if (!found) {
        if (!grow_buffer(output, output_len, regex->subject_len)) {
            *status = 7;
            return 0;
        }
        memcpy(*output, regex->subject, (size_t)regex->subject_len * sizeof(UChar));
        *output_len = regex->subject_len;
        return 1;
    }

    int64_t estimate = (int64_t)regex->subject_len + replacement_len + 32;
    int32_t capacity = estimate > INT32_MAX ? INT32_MAX : (int32_t)estimate;
    if (capacity < 64) {
        capacity = 64;
    }

    for (;;) {
        int32_t allocated = 0;
        if (!grow_buffer(output, &allocated, capacity)) {
            *status = 7;
            return 0;
        }
        *status = 0;
        int32_t prefix_end = start;
        found = icu.find(regex->regex, start, status) != 0;
        for (int32_t i = 1; i < occurrence && found && *status <= 0; ++i) {
            prefix_end = icu.end(regex->regex, 0, status);
            if (*status > 0) {
                return 0;
            }
            found = icu.find_next(regex->regex, status) != 0;
        }
        if (*status > 0) {
            return 0;
        }
        int32_t used = prefix_end;
        if (prefix_end > 0) {
            memcpy(*output, regex->subject, (size_t)prefix_end * sizeof(UChar));
        }

        int overflow = 0;
        int32_t required_capacity = capacity;
        do {
            UChar *destination = *output + used;
            int32_t remaining = capacity - used;
            int32_t required = icu.append_replacement(
                regex->regex, replacement, replacement_len, &destination, &remaining, status);
            if (*status == 15) { // U_BUFFER_OVERFLOW_ERROR
                overflow = 1;
                required_capacity = used > INT32_MAX - required ? INT32_MAX : used + required;
                *status = 0;
                break;
            }
            if (*status > 0) {
                return 0;
            }
            used += required;
            if (occurrence != 0) {
                break;
            }
            found = icu.find_next(regex->regex, status) != 0;
        } while (found && *status <= 0);
        if (*status > 0) {
            return 0;
        }

        if (!overflow) {
            UChar *destination = *output + used;
            int32_t remaining = capacity - used;
            int32_t required = icu.append_tail(regex->regex, &destination, &remaining, status);
            if (*status == 15) {
                overflow = 1;
                required_capacity = used > INT32_MAX - required ? INT32_MAX : used + required;
                *status = 0;
            } else if (*status > 0) {
                return 0;
            } else {
                *output_len = used + required;
                return 1;
            }
        }

        free(*output);
        *output = NULL;
        if (capacity == INT32_MAX) {
            *status = 15;
            return 0;
        }
        int32_t next = capacity > INT32_MAX / 2 ? INT32_MAX : capacity * 2;
        if (next < required_capacity) {
            next = required_capacity;
        }
        capacity = next;
    }
}

void mo_icu_regex_free(void *pointer) {
    free(pointer);
}
