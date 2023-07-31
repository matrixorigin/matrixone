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
	"os"

	alicredentials "github.com/aliyun/credentials-go/credentials"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	tencentcommon "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	"go.uber.org/zap"
)

func getCredentialsProvider(
	ctx context.Context,
	endpoint string,
	region string,
	apiKey string,
	apiSecret string,
	sessionToken string,
	roleARN string,
	externalID string,
) (
	ret aws.CredentialsProvider,
) {

	// cache
	defer func() {
		if ret == nil {
			return
		}
		ret = aws.NewCredentialsCache(ret)
	}()

	// aliyun TODO
	//if strings.Contains(endpoint, "aliyuncs.com") {
	//	provider := newAliyunCredentialsProvider(roleARN, externalID)
	//	_, err := provider.Retrieve(ctx)
	//	if err == nil {
	//		return provider
	//	}
	//	logutil.Info("skipping bad aliyun credential provider",
	//		zap.Any("error", err),
	//	)
	//}

	// qcloud TODO
	//if strings.Contains(endpoint, "myqcloud.com") ||
	//	strings.Contains(endpoint, "tencentcos.cn") {
	//	provider := newTencentCloudCredentialsProvider(roleARN, externalID)
	//	_, err := provider.Retrieve(ctx)
	//	if err == nil {
	//		return provider
	//	}
	//	logutil.Info("skipping bad qcloud credential provider",
	//		zap.Any("error", err),
	//	)
	//}

	// aws role arn
	if roleARN != "" {
		if provider := func() aws.CredentialsProvider {
			loadConfigOptions := []func(*config.LoadOptions) error{
				config.WithLogger(logutil.GetS3Logger()),
				config.WithClientLogMode(
					aws.LogSigning |
						aws.LogRetries |
						aws.LogRequest |
						aws.LogResponse |
						aws.LogDeprecatedUsage |
						aws.LogRequestEventMessage |
						aws.LogResponseEventMessage,
				),
			}
			awsConfig, err := config.LoadDefaultConfig(ctx, loadConfigOptions...)
			if err != nil {
				return nil
			}
			stsSvc := sts.NewFromConfig(awsConfig, func(options *sts.Options) {
				if region == "" {
					options.Region = "ap-northeast-1"
				} else {
					options.Region = region
				}
			})
			provider := stscreds.NewAssumeRoleProvider(
				stsSvc,
				roleARN,
				func(opts *stscreds.AssumeRoleOptions) {
					if externalID != "" {
						opts.ExternalID = &externalID
					}
				},
			)
			_, err = provider.Retrieve(ctx)
			if err == nil {
				return provider
			}
			logutil.Info("skipping bad role arn credentials provider",
				zap.Any("error", err),
			)
			return nil
		}(); provider != nil {
			return provider
		}
	}

	// static credential
	if apiKey != "" && apiSecret != "" {
		// static
		return credentials.NewStaticCredentialsProvider(apiKey, apiSecret, sessionToken)
	}

	return
}

func init() {
	_ = newAliyunCredentialsProvider
	_ = newTencentCloudCredentialsProvider
}

func newAliyunCredentialsProvider(
	roleARN string,
	externalID string,
) aws.CredentialsProvider {

	return aws.CredentialsProviderFunc(
		func(_ context.Context) (cs aws.Credentials, err error) {

			set := func(cred alicredentials.Credential) error {
				accessKeyID, err := cred.GetAccessKeyId()
				if err != nil {
					return err
				}
				cs.AccessKeyID = *accessKeyID

				secretAccessKey, err := cred.GetAccessKeySecret()
				if err != nil {
					return err
				}
				cs.SecretAccessKey = *secretAccessKey

				sessionToken, err := cred.GetSecurityToken()
				if err != nil {
					return err
				}
				cs.SessionToken = *sessionToken

				return nil
			}

			// rrsa
			if roleARN,
				OIDCProviderARN,
				OIDCTokenFile,
				sessionName :=
				os.Getenv(alicredentials.ENVRoleArn),
				os.Getenv(alicredentials.ENVOIDCProviderArn),
				os.Getenv(alicredentials.ENVOIDCTokenFile),
				os.Getenv(alicredentials.ENVRoleSessionName); roleARN != "" {
				if sessionName == "" {
					sessionName = "aliyun-rrsa"
				}
				config := new(alicredentials.Config).
					SetType("oidc_role_arn").
					SetRoleArn(roleARN).
					SetOIDCProviderArn(OIDCProviderARN).
					SetOIDCTokenFilePath(OIDCTokenFile).
					SetRoleSessionName(sessionName)
				var cred alicredentials.Credential
				cred, err = alicredentials.NewCredential(config)
				if err != nil {
					return
				}
				if err = set(cred); err != nil {
					return
				}
				// skip default
				return
			}

			// default credentials chain
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

			sessionToken, err := aliCredential.GetSecurityToken()
			if err != nil {
				return
			}
			cs.SessionToken = *sessionToken

			// with role arn
			if roleARN != "" {
				config := new(alicredentials.Config)
				config.SetType("ram_role_arn")
				config.SetAccessKeyId(*accessKeyID)
				config.SetAccessKeySecret(*secretAccessKey)
				config.SetSecurityToken(*sessionToken)
				config.SetRoleArn(roleARN)
				config.SetRoleSessionName(externalID)
				var cred alicredentials.Credential
				cred, err = alicredentials.NewCredential(config)
				if err != nil {
					return
				}
				if err = set(cred); err != nil {
					return
				}
			}

			return
		},
	)
}

func newTencentCloudCredentialsProvider(
	roleARN string,
	externalID string,
) aws.CredentialsProvider {

	return aws.CredentialsProviderFunc(
		func(_ context.Context) (cs aws.Credentials, err error) {

			// default credentials chain
			provider := tencentcommon.DefaultProviderChain()

			credentials, err := provider.GetCredential()
			if err != nil {
				return
			}

			cs.AccessKeyID = credentials.GetSecretId()
			cs.SecretAccessKey = credentials.GetSecretKey()
			cs.SessionToken = credentials.GetToken()

			// with role arn
			if roleARN != "" {
				roleARNProvider := tencentcommon.DefaultRoleArnProvider(
					credentials.GetSecretId(),
					credentials.GetSecretKey(),
					roleARN,
				)
				var cred tencentcommon.CredentialIface
				cred, err = roleARNProvider.GetCredential()
				if err != nil {
					return
				}
				cs.AccessKeyID = cred.GetSecretId()
				cs.SecretAccessKey = cred.GetSecretKey()
				cs.SessionToken = cred.GetToken()
			}

			return
		},
	)
}
