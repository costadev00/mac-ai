import os
import subprocess
import time
import requests
import json
import pytest

COMPOSE_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'docker-compose.yml')


def wait_for_app(url, timeout=60):
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = requests.get(url)
            if r.status_code < 500:
                return
        except Exception:
            pass
        time.sleep(1)
    raise RuntimeError('App did not become ready')


def compose_up():
    subprocess.run(['docker-compose', '-f', COMPOSE_FILE, 'up', '-d'], check=True)
    wait_for_app('http://localhost:8000/docs')


def compose_down():
    subprocess.run(['docker-compose', '-f', COMPOSE_FILE, 'down', '-v'], check=True)


@pytest.fixture(scope='module', autouse=True)
def run_compose():
    compose_up()
    yield
    compose_down()


def test_pergunta():
    question = 'Quantas coca colas vendi na semana passada?'
    resp = requests.post(
        'http://localhost:8000/perguntar',
        json={'pergunta': question},
    )
    resp.raise_for_status()
    data = resp.json()
    # represent entire response text for convenience
    text = json.dumps(data, ensure_ascii=False)
    assert 'Coca-Cola' in text
    assert 'semana passada' in text
    assert 'R$' in text
    assert data['sql'].strip().endswith('LIMIT 100')
