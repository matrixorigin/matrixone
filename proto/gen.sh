#!/bin/bash
#
# Generate all MatrixOne protobuf bindings. Run from repository root.
#
set -ex

program_exists() {
  if command -v "${1}" > /dev/null 2>&1; then
    echo "ok"
  else
    echo "fail"
  fi
}

VENDOR_DIR="$PWD/vendor"
PB_DIR="$PWD/pkg/pb"
PROTOC_DIR="$PWD/proto"
PROTO_SYNTAX_VERSION='3'
PROTOC_VERSION='21.1'
GOGOPROTOBUF_VERSION='1.'

if [ "${GOPATH}" == "" ];then
  GOPATH=`go env GOPATH`
fi
echo "GOPATH: ${GOPATH}"

res=$(program_exists goimports)
echo "res: ${res}"
if [ "${res}" == "ok" ];then
  echo "goimports exist"
else
  echo "install goimports"
  go install golang.org/x/tools/cmd/goimports@latest
fi

res=$(program_exists protoc)
echo "res: ${res}"
if [ "${res}" == "ok" ];then
  echo "protoc exist"
  version=$(protoc --version)
  if [[ "${version}" == *"${PROTO_SYNTAX_VERSION}.${PROTOC_VERSION}"* ]];then
    echo "protoc version matches ${PROTOC_VERSION}"
  else
    echo "protoc version does not match"
  fi
  if [ -f ${GOPATH}/bin/protoc ]; then
    ${GOPATH}/bin/protoc --version
  else
    ln -s $(which protoc) ${GOPATH}/bin/protoc
    ${GOPATH}/bin/protoc --version
  fi

else
  echo "install protoc"
  if [ "$(uname)" == "Darwin" ];then
    OS='osx'
  elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ];then
    OS='linux'
  else
    echo "unsupported os, failed"
    exit 1
  fi
  if [[ "$(uname -m)" == *"x86"* ]];then
    ARCH='x86'
  elif [[ "$(uname -m)" == *"arm"* ]];then
    ARCH='aarch'
  elif [[ "$(uname -m)" == *"aarch"* ]];then
    ARCH='aarch'
  else
    echo "unsupported arch, failed"
    exit 1
  fi
  if [[ "$(getconf LONG_BIT)" == "64" ]];then
    SYSTEM_BIT='64'
  elif [[ "$(getconf LONG_BIT)" == "32" ]];then
    SYSTEM_BIT='32'
  else
    echo "unsupported system bit, failed"
    exit 1
  fi
  PROTOC_ZIP="protoc-${PROTOC_VERSION}-${OS}-${ARCH}_${SYSTEM_BIT}.zip"
  curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/${PROTOC_ZIP}
  unzip -o $PROTOC_ZIP -d "${GOPATH}/" bin/protoc
  unzip -o $PROTOC_ZIP -d "${GOPATH}/" 'include/*'
  ${GOPATH}/bin/protoc --version
fi

res1=$(program_exists protoc-gen-gogofast)
res2=$(program_exists protoc-gen-gogofaster)
echo "res1: ${res1}, res2: ${res2}"
if [ "${res1}" == "ok" -a "${res2}" == "ok" ];then
  echo "protoc-gen-gogofast and protoc-gen-gogofaster exist"
else
  echo "install protoc-gen-gogofast"
  if [ -f protobuf/ ];then rm -rf protobuf/;fi
  git clone https://github.com/gogo/protobuf.git
  cd protobuf
  git checkout v1.3.2
  cd protoc-gen-gogofast
  go build -o $GOPATH/bin/protoc-gen-gogofast
  cd ..
  cd protoc-gen-gogofaster
  go build -o $GOPATH/bin/protoc-gen-gogofaster
  cd ../..
fi


for file in `ls $PROTOC_DIR/*.proto`
do
  outArgName="gogofast_out"
  # For query.proto and statsinfo.proto, extra fields are redundant.
  if echo $file | egrep "query.proto|statsinfo.proto" >/dev/null; then
    outArgName="gogofaster_out"
  fi
	dir=$(basename $file .proto)
	mkdir -p $PB_DIR/$dir
	${GOPATH}/bin/protoc -I=.:$PROTOC_DIR:$VENDOR_DIR --$outArgName=paths=source_relative:./pkg/pb/$dir  $file
    goimports -w $PB_DIR/$dir/*pb.go
done


# Generate pb file for each package's own
for fp in $(find pkg -name protogen.sh)
do
    cd $(dirname ${fp})
    bash $(basename ${fp})
    cd - > /dev/null
done

if [ -f protobuf/ ];then rm -rf protobuf/;fi
