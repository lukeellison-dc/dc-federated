"""
Tests for the DCFWorker and DCFServer class.
"""

import gevent
from gevent import Greenlet, sleep
from gevent import monkey; monkey.patch_all()

import io
import pickle
import logging
import zlib

import requests

from dc_federated.backend import DCFServer, DCFWorker, create_model_dict, is_valid_model_dict
from dc_federated.backend._constants import *
from dc_federated.utils import StoppableServer, get_host_ip


def test_server_functionality():
    """
    Unit tests for the DCFServer and DCFWorker classes.
    """
    worker_ids = []
    worker_updates = {}
    global_model_version = "1"
    worker_global_model_version = "0"
    stoppable_server = StoppableServer(host=get_host_ip(), port=8080)

    def begin_server():
        dcf_server.start_server(stoppable_server)

    def test_register_func_cb(id):
        worker_ids.append(id)

    def test_ret_global_model_cb():
        return create_model_dict(
            pickle.dumps("Pickle dump of a string"),
            global_model_version)

    def is_global_model_most_recent(version):
        return int(version) == global_model_version

    def test_rec_server_update_cb(worker_id, update):
        if worker_id in worker_ids:
            worker_updates[worker_id] = update
            return f"Update received for worker {worker_id}."
        else:
            return f"Unregistered worker {worker_id} tried to send an update."

    def test_glob_mod_chng_cb(model_dict):
        nonlocal worker_global_model_version
        worker_global_model_version = model_dict[GLOBAL_MODEL_VERSION]

    def test_get_last_glob_model_ver():
        nonlocal worker_global_model_version
        return worker_global_model_version

    dcf_server = DCFServer(
        register_worker_callback=test_register_func_cb,
        return_global_model_callback=test_ret_global_model_cb,
        is_global_model_most_recent=is_global_model_most_recent,
        receive_worker_update_callback=test_rec_server_update_cb,
        key_list_file=None
    )
    server_gl = Greenlet.spawn(begin_server)
    sleep(2)

    # register a set of workers
    data = {
        PUBLIC_KEY_STR: "dummy public key",
        SIGNED_PHRASE: "dummy signed phrase"
    }
    for i in range(3):
        requests.post(
            f"http://{dcf_server.server_host_ip}:{dcf_server.server_port}/{REGISTER_WORKER_ROUTE}", json=data)

    assert len(worker_ids) == 3
    assert worker_ids[0] != worker_ids[1] and worker_ids[1] != worker_ids[2] and worker_ids[0] != worker_ids[2]
    assert worker_ids[0].__class__ == worker_ids[1].__class__ == worker_ids[2].__class__

    # test getting the global model
    model_return_binary = requests.post(
        f"http://{dcf_server.server_host_ip}:{dcf_server.server_port}/{RETURN_GLOBAL_MODEL_ROUTE}",
        json={WORKER_ID_KEY: worker_ids[0],
              LAST_WORKER_MODEL_VERSION: "0"}
    ).content
    model_return = pickle.loads(zlib.decompress(model_return_binary))
    assert isinstance(model_return, dict)
    assert model_return[GLOBAL_MODEL_VERSION] == global_model_version
    assert pickle.loads(model_return[GLOBAL_MODEL]) == "Pickle dump of a string"

    # test sending the model update
    response = requests.post(
        f"http://{dcf_server.server_host_ip}:{dcf_server.server_port}/{RECEIVE_WORKER_UPDATE_ROUTE}/{worker_ids[1]}",
        files={ID_AND_MODEL_KEY: zlib.compress(pickle.dumps("Model update!!"))}
    ).content

    assert pickle.load(io.BytesIO(worker_updates[worker_ids[1]])) == "Model update!!"
    assert response.decode("UTF-8") == f"Update received for worker {worker_ids[1]}."

    response = requests.post(
        f"http://{dcf_server.server_host_ip}:{dcf_server.server_port}/{RECEIVE_WORKER_UPDATE_ROUTE}/3",
        files={ID_AND_MODEL_KEY: zlib.compress(pickle.dumps("Model update for unregistered worker!!"))}
    ).content

    assert 3 not in worker_updates
    assert response.decode('UTF-8') == UNREGISTERED_WORKER

    # *********** #
    # now test a DCFWorker on the same server.
    dcf_worker = DCFWorker(
        server_protocol='http',
        server_host_ip=dcf_server.server_host_ip,
        server_port=dcf_server.server_port,
        global_model_version_changed_callback=test_glob_mod_chng_cb,
        get_worker_version_of_global_model=test_get_last_glob_model_ver,
        private_key_file=None)

    # test worker registration
    dcf_worker.register_worker()
    assert dcf_worker.worker_id == worker_ids[3]

    # test getting the global model update
    global_model_dict = dcf_worker.get_global_model()
    assert is_valid_model_dict(global_model_dict)
    assert global_model_dict[GLOBAL_MODEL_VERSION] == global_model_version
    assert pickle.loads(global_model_dict[GLOBAL_MODEL]) == "Pickle dump of a string"

    # test sending the model update
    response = dcf_worker.send_model_update(
        pickle.dumps("DCFWorker model update"))
    assert pickle.load(io.BytesIO(
        worker_updates[worker_ids[3]])) == "DCFWorker model update"
    assert response.decode(
        "UTF-8") == f"Update received for worker {worker_ids[3]}."

    stoppable_server.shutdown()
