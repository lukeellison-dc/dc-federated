import os
import subprocess
from inspect import signature
from unittest.mock import patch, Mock

from dc_federated import backend
from dc_federated.backend import DCFServer, DCFServerHandler

def test_dcfServerHandler_interface():
    handler_params = list(signature(DCFServerHandler).parameters.values())
    for i,param in enumerate(signature(DCFServer).parameters.values()):
        assert param == handler_params[i]

@patch.object(subprocess, 'Popen')
def test_opens_subprocess(Popen_mock):
    server = DCFServerHandler(Mock(), Mock(), Mock(), Mock(), Mock(), False, None, socket_port='test_port')
    server.initialise_zmq = Mock()
    server.__del__ = Mock()
    server.wait_for_messages = Mock()
    server.start_server()

    backend_root = os.path.dirname(backend.__file__)
    print('call_args =', Popen_mock.call_args)
    print('args =', Popen_mock.call_args[0])
    assert Popen_mock.call_args[0][0] == f'python {backend_root}/subprocess_dcf_server.py test_port'