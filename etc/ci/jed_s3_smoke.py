"""Tiny signed S3 probe for the newly authorized JED CI bucket, not business data."""
import csv
import datetime
import hashlib
import hmac
import os
import urllib.error
import urllib.parse
import urllib.request
import uuid
import xml.etree.ElementTree as ET


def main():
    config = dict(field.split('=', 1) for field in next(csv.reader([os.environ['TEST_S3FS_CS']])))
    endpoint = urllib.parse.urlsplit(config['endpoint'])
    if endpoint.scheme != 'http' or endpoint.hostname != 'minio.minio.svc' or endpoint.port != 80:
        raise RuntimeError('Unexpected JED internal endpoint')
    if config.get('bucket') != 'mo-ci-ut':
        raise RuntimeError('Refusing any bucket other than the dedicated CI bucket')
    region = config.get('region') or 'us-east-1'
    # This is cluster-internal traffic; do not route credentials via an HTTP proxy.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def request(method, path, query=None, data=b'', expected=200, headers=None):
        now = datetime.datetime.now(datetime.timezone.utc)
        stamp, day = now.strftime('%Y%m%dT%H%M%SZ'), now.strftime('%Y%m%d')
        path = urllib.parse.quote(path, safe='/')
        query = urllib.parse.urlencode(sorted((query or {}).items()), quote_via=urllib.parse.quote)
        digest = hashlib.sha256(data).hexdigest()
        fields = {'host': endpoint.netloc, 'x-amz-date': stamp, 'x-amz-content-sha256': digest}
        fields.update(headers or {})
        signed = ';'.join(sorted(fields))
        canonical_headers = ''.join(f'{name}:{fields[name]}\n' for name in sorted(fields))
        canonical = f'{method}\n{path}\n{query}\n{canonical_headers}\n{signed}\n{digest}'
        scope = f'{day}/{region}/s3/aws4_request'
        message = f'AWS4-HMAC-SHA256\n{stamp}\n{scope}\n{hashlib.sha256(canonical.encode()).hexdigest()}'
        def sign(key, value):
            return hmac.new(key, value.encode(), hashlib.sha256).digest()
        key = sign(sign(sign(sign(('AWS4' + config['key-secret']).encode(), day), region), 's3'), 'aws4_request')
        signature = hmac.new(key, message.encode(), hashlib.sha256).hexdigest()
        fields['Authorization'] = f"AWS4-HMAC-SHA256 Credential={config['key-id']}/{scope}, SignedHeaders={signed}, Signature={signature}"
        url = urllib.parse.urlunsplit((endpoint.scheme, endpoint.netloc, path, query, ''))
        req = urllib.request.Request(url, data=data if method == 'PUT' else None, headers=fields, method=method)
        try:
            with opener.open(req, timeout=20) as response:
                status, body = response.status, response.read()
        except urllib.error.HTTPError as error:
            status, body = error.code, error.read()
        if status != expected:
            raise RuntimeError(f'S3 {method} returned HTTP {status}, expected {expected}')
        return body

    denied = request('GET', '/matrixone-armos', {'list-type': '2', 'max-keys': '0'}, expected=403)
    if ET.fromstring(denied).findtext('Code') != 'AccessDenied':
        raise RuntimeError('Business bucket authorization denial was not proven')
    print('PASS: business bucket access denied (read-only authorization check)', flush=True)

    prefix = f"ci-ut/{os.environ['GITHUB_RUN_ID']}-{os.environ['GITHUB_RUN_ATTEMPT']}/probe-{uuid.uuid4().hex}/"
    object_path = '/mo-ci-ut/' + prefix + 'probe.txt'
    def keys():
        response = request('GET', '/mo-ci-ut', {'list-type': '2', 'prefix': prefix, 'max-keys': '2'})
        return [entry.text for entry in ET.fromstring(response).findall('{*}Contents/{*}Key')]

    if keys():
        raise RuntimeError('Refusing nonempty run prefix')
    created = False
    payload = b'MatrixOne CS S3 isolated probe\n'
    try:
        request('PUT', object_path, data=payload, headers={'if-none-match': '*'})
        created = True
        if request('GET', object_path) != payload:
            raise RuntimeError('S3 readback mismatch')
        if keys() != [prefix + 'probe.txt']:
            raise RuntimeError('S3 list mismatch')
        print('PASS: signed PUT, GET and LIST from JED runner; readback matches', flush=True)
    finally:
        if created:
            # Delete exactly our acknowledged write, never a recursive prefix or bucket.
            request('DELETE', object_path, expected=204)
            if keys():
                raise RuntimeError('Run prefix still contains objects after cleanup')
            print('PASS: deleted only this probe object; run prefix is empty', flush=True)
    with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as summary:
        summary.write('JED runner: internal S3 PUT/GET/LIST/DELETE passed; own run prefix empty; business bucket access denied. This is connectivity/permission smoke, not full MO UT Coverage.\n')


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        # HTTP exceptions can contain request details; only our own errors are displayed.
        print('FAIL:', str(error) if isinstance(error, RuntimeError) else type(error).__name__)
        raise SystemExit(1)
