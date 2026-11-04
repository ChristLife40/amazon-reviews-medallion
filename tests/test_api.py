from fastapi.testclient import TestClient

from reviews_pipeline.worker_api import app

client = TestClient(app)


def test_health():
    response = client.get('/health')
    assert response.status_code == 200 and response.json()['status'] == 'ok'


def test_rejects_path_traversal_batch_id():
    body = {'batch_id': '../etc', 'shard': 0, 'shard_count': 4}
    assert client.post('/process', json=body).status_code == 422


def test_rejects_shard_out_of_range():
    body = {'batch_id': 'b1', 'shard': 4, 'shard_count': 4}
    assert client.post('/process', json=body).status_code == 422