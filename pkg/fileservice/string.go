// Copyright 2024 Matrix Origin
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

package fileservice

var (
	str_s3fs_read                         = internString("S3FS.Read")
	str_read_return                       = internString("Read return")
	str_ioMerger_Merge_begin              = internString("ioMerger.Merge begin")
	str_ioMerger_Merge_end                = internString("ioMerger.Merge end")
	str_ioMerger_Merge_initiate           = internString("ioMerger.Merge initiate")
	str_ioMerger_Merge_wait               = internString("ioMerger.Merge wait")
	str_read_vector_Caches_begin          = internString("read vector.Caches begin")
	str_read_vector_Caches_end            = internString("read vector.Caches end")
	str_update_vector_Caches_begin        = internString("update vector.Caches begin")
	str_update_vector_Caches_end          = internString("update vector.Caches end")
	str_read_memory_cache_Caches_begin    = internString("read memory cache begin")
	str_read_memory_cache_Caches_end      = internString("read memory cache end")
	str_update_memory_cache_Caches_begin  = internString("update memory cache begin")
	str_update_memory_cache_Caches_end    = internString("update memory cache end")
	str_read_disk_cache_Caches_begin      = internString("read disk cache begin")
	str_read_disk_cache_Caches_end        = internString("read disk cache end")
	str_update_disk_cache_Caches_begin    = internString("update disk cache begin")
	str_update_disk_cache_Caches_end      = internString("update disk cache end")
	str_read_remote_cache_Caches_begin    = internString("read remote cache begin")
	str_read_remote_cache_Caches_end      = internString("read remote cache end")
	str_get_reader_begin                  = internString("getReader begin")
	str_get_reader_end                    = internString("getReader end")
	str_reader_close                      = internString("reader close")
	str_get_content_begin                 = internString("getContent begin")
	str_get_content_end                   = internString("getContent end")
	str_io_readall_begin                  = internString("io.ReadAll begin")
	str_io_readall_end                    = internString("io.ReadAll end")
	str_get_data_begin                    = internString("getData begin")
	str_get_data_end                      = internString("getData end")
	str_write_writerforread_begin         = internString("write WriterForRead begin")
	str_write_writerforread_end           = internString("write WriterForRead end")
	str_io_copybuffer_begin               = internString("io.CopyBuffer begin")
	str_io_copybuffer_end                 = internString("io.CopyBuffer end")
	str_disk_cache_setfile_begin          = internString("disk cache SetFile begin")
	str_disk_cache_setfile_end            = internString("disk cache SetFile end")
	str_retryable_reader_new_reader_begin = internString("retryable reader new reader begin")
	str_retryable_reader_new_reader_end   = internString("retryable reader new reader end")
	str_awssdkv2_get_object_begin         = internString("AwsSDKv2 GetObject begin")
	str_awssdkv2_get_object_end           = internString("AwsSDKv2 GetObject end")
	str_set_cache_data_begin              = internString("setCacheData begin")
	str_set_cache_data_end                = internString("setCacheData end")
	str_to_cache_data_begin               = internString("ToCacheData begin")
	str_to_cache_data_end                 = internString("ToCacheData end")
)

type stringRef struct {
	ptr *string
}

func (s stringRef) String() string {
	return *s.ptr
}

func internString(str string) stringRef {
	return stringRef{
		ptr: &str,
	}
}
