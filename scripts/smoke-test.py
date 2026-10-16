#!/usr/bin/env python3
"""Exercise a running deployment through the public orchestrator, using only stdlib."""
import argparse
import json
import time
import urllib.error
import urllib.request
import uuid
from decimal import Decimal


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url', default='http://localhost:8080')
    parser.add_argument('--timeout', type=int, default=120)
    parser.add_argument('--expect-timeout', action='store_true',
                        help='Run only the missing-result case; stop inventory-cost-service first.')
    args = parser.parse_args()
    base = args.base_url.rstrip('/')

    def call(method, path, body=None):
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(base + path, data=data, method=method,
                                         headers={'Content-Type': 'application/json'})
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                return response.status, json.load(response)
        except urllib.error.HTTPError as error:
            return error.code, json.load(error)

    def request(product='P100'):
        return {'calculationId': str(uuid.uuid4()), 'rp': 'SMOKE-' + str(uuid.uuid4()),
                'mp': 'MP1', 'vbu': 'VBU1', 'financeGroup': 'FG1',
                'productId': product, 'quantity': 10}

    def submit(body):
        status, result = call('POST', '/api/calculations', body)
        assert status == 202, (status, result)

    def wait(body):
        deadline = time.monotonic() + args.timeout
        while time.monotonic() < deadline:
            status, result = call('GET', '/api/calculations/' + body['calculationId'])
            assert status in (200, 404), (status, result)
            if status == 200 and result['state'] in ('COMPLETED', 'FAILED'):
                return result
            time.sleep(0.5)
        raise AssertionError('No terminal projection before smoke-test timeout')

    if args.expect_timeout:
        body = request()
        submit(body)
        result = wait(body)
        assert result['state'] == 'FAILED', result
        assert result['result']['error']['code'] == 'DEADLINE_EXCEEDED', result
        assert 'totalCost' not in result['result'], result
        print('PASS: missing inventory result produces FAILED without a total')
        return

    body = request()
    submit(body)
    result = wait(body)
    assert result['state'] == 'COMPLETED', result
    assert Decimal(str(result['result']['totalCost'])) == Decimal('1200.00'), result
    assert len(result['result']['components']) == 4, result
    print('PASS: four components → COMPLETED, totalCost=1200.00')

    status, replay = call('POST', '/api/calculations', body)
    assert status == 200 and replay['replay'] is True, (status, replay)
    altered = dict(body, quantity=11)
    assert call('POST', '/api/calculations', altered)[0] == 409
    print('PASS: identical replay is idempotent; changed payload conflicts')

    # A final projection may be visible before command-side release; retry conservatively.
    replacement = dict(body, calculationId=str(uuid.uuid4()))
    deadline = time.monotonic() + 15
    while True:
        status, response = call('POST', '/api/calculations', replacement)
        if status == 202:
            break
        assert status == 409 and time.monotonic() < deadline, (status, response)
        time.sleep(0.5)
    assert wait(replacement)['state'] == 'COMPLETED'
    print('PASS: terminal calculation releases its business key for a new ID')

    failed = request('FAIL_FREIGHT_COST')
    submit(failed)
    result = wait(failed)
    assert result['state'] == 'FAILED', result
    assert result['result']['error']['code'] == 'DEMO_FAILURE', result
    assert 'totalCost' not in result['result'], result
    print('PASS: component failure produces FAILED without a total')

    invalid = dict(request(), quantity=0)
    assert call('POST', '/api/calculations', invalid)[0] == 400
    assert call('GET', '/api/calculations/' + str(uuid.uuid4()))[0] == 404
    print('PASS: invalid input rejected; unknown calculation returns 404')


if __name__ == '__main__':
    main()
