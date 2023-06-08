// Copyright 2022 Matrix Origin
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

import (
	"context"

	alicredentials "github.com/aliyun/credentials-go/credentials"
	"github.com/aws/aws-sdk-go-v2/aws"
	tencentcommon "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
)

func newAliyunCredentialsProvider() aws.CredentialsProvider {
	return aws.CredentialsProviderFunc(
		func(_ context.Context) (cs aws.Credentials, err error) {
			aliCredential, err := alicredentials.NewCredential(nil)
			if err != nil {
				return
			}
			accessKeyID, err := aliCredential.GetAccessKeyId()
			if err != nil {
				return
			}
			cs.AccessKeyID = *accessKeyID
			secretAccessKey, err := aliCredential.GetAccessKeySecret()
			if err != nil {
				return
			}
			cs.SecretAccessKey = *secretAccessKey
			return
		},
	)
}

func newTencentCloudCredentialsProvider() aws.CredentialsProvider {
	return aws.CredentialsProviderFunc(
		func(_ context.Context) (cs aws.Credentials, err error) {
			provider := tencentcommon.DefaultProviderChain()
			credentials, err := provider.GetCredential()
			if err != nil {
				return
			}
			cs.AccessKeyID = credentials.GetSecretId()
			cs.SecretAccessKey = credentials.GetSecretKey()
			return
		},
	)
}
